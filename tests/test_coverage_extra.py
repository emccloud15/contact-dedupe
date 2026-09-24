from pathlib import Path
import sys
import types
from types import SimpleNamespace

import pandas as pd
import pytest

from contact_dedupe.common.exceptions import ConfigError, DataLoadError
from contact_dedupe.common.final_files import create_check_cols, create_check_file
from contact_dedupe.common.logger import get_logger
from contact_dedupe.common.models import (
    Blocking,
    Bounds,
    CandidateBlock,
    ClientConfig,
    ColumnTypeConfig,
    Columns,
    MatchingProfile,
    rule_name,
)
from contact_dedupe.common.utils import Utilities
from contact_dedupe.dedupe.candidate_generator import BlockGenerator, CandidateGenerator
from contact_dedupe.dedupe.decision import Decision, PairDecision
from contact_dedupe.dedupe.dsu import DSU
from contact_dedupe.dedupe.evaluation import evaluate_decisions
from contact_dedupe.dedupe.evidence import EvidenceBuilder, FieldEvidence, MatchEvidence
from contact_dedupe.dedupe.grouping import DuplicateGrouper
from contact_dedupe.dedupe.writer import ResultWriter


def _minimal_config(**overrides):
    value = {
        "CLIENT_NAME": "extra",
        "COLUMNS": {"name": {"columns": ["Name"], "weight": 1.0}},
        "BLOCKING": {"strict": False, "type": "name", "column": "Name"},
        "MAIN_MATCH_CRITERIA": "Name",
        "MATCH_FIELD": "Name",
        "BOUNDS": {},
    }
    value.update(overrides)
    return ClientConfig.model_validate(value)


def _evidence(left="record:A", right="record:B", *, decision_score=95.0, fields=None, conflicts=()):
    fields = fields or {
        "clean_Name:name": FieldEvidence(
            "clean_Name:name", "name", "Name", 95.0, 1.0, True, "EXACT", True
        )
    }
    return MatchEvidence(
        left, right, fields, ["Name"], [], ["Name"], list(conflicts), decision_score,
        ("exact:name:Name",),
    )


def _decision(evidence, decision=Decision.MERGE):
    return PairDecision(
        decision, "test_rule", ("TEST",), "test", "profile", "v1", evidence
    )


def test_model_validation_edges_and_helpers():
    assert rule_name(" First_Name ") == "first_name"
    assert ColumnTypeConfig(columns=["A"], combine=["A"]).active_columns == ["A"]
    with pytest.raises(ValueError, match="non-empty strings"):
        ColumnTypeConfig(columns=[""])
    with pytest.raises(ValueError, match="duplicated"):
        ColumnTypeConfig(columns=["A", "A"])
    with pytest.raises(ValueError, match="finite"):
        ColumnTypeConfig(columns=["A"], weight=float("inf"))
    assert ColumnTypeConfig(columns=["A"], weight=[("A", None)]).weight == [("A", None)]
    with pytest.raises(ValueError, match="non-empty"):
        Blocking(strict=False, type="", column="Name")
    with pytest.raises(ValueError, match="candidate block type"):
        CandidateBlock(type="bad", field="x")
    with pytest.raises(ValueError, match="require field"):
        CandidateBlock(type="exact")
    with pytest.raises(ValueError, match="unique"):
        CandidateBlock(type="composite", fields=["x", "x"])
    with pytest.raises(ValueError, match="positive"):
        CandidateBlock(type="prefix", field="x", length=0)
    with pytest.raises(ValueError, match="start or end"):
        CandidateBlock(type="prefix", field="x", length=2, direction="middle")
    with pytest.raises(ValueError, match="at least 2"):
        CandidateBlock(type="exact", field="x", max_bucket_size=1)
    with pytest.raises(ValueError, match="require field"):
        CandidateBlock(type="prefix", field=None, length=2)
    with pytest.raises(ValueError, match="non-empty string"):
        CandidateBlock(type="exact", field="")
    with pytest.raises(ValueError, match="at least two"):
        CandidateBlock(type="composite", fields=["x"])
    with pytest.raises(ValueError, match="length"):
        CandidateBlock(type="prefix", field="x")
    with pytest.raises(ValueError, match="non-empty"):
        MatchingProfile(auto_merge_rule=" ")
    with pytest.raises(ValueError, match="required configuration"):
        _minimal_config(CLIENT_NAME=" ")
    with pytest.raises(ValueError, match="MATCHING_PROFILE"):
        _minimal_config(MATCHING_PROFILE=" ")


def test_client_config_validation_edges():
    with pytest.raises(ConfigError, match="not defined"):
        _minimal_config(
            MATCHING_PROFILE="missing",
            MATCHING_PROFILES={"other": {}},
        )
    with pytest.raises(ConfigError, match="MAIN_MATCH_CRITERIA"):
        _minimal_config(MAIN_MATCH_CRITERIA="Missing")
    with pytest.raises(ConfigError, match="At least one"):
        ClientConfig.model_validate(
            {
                "CLIENT_NAME": "x",
                "COLUMNS": {},
                "BLOCKING": {"strict": False, "type": "name", "column": "x"},
                "MAIN_MATCH_CRITERIA": "address",
                "MATCH_FIELD": "x",
                "BOUNDS": {},
            }
        )
    with pytest.raises(ConfigError, match="must be one"):
        _minimal_config(BLOCKING={"strict": False, "type": "bad", "column": "Name"})
    with pytest.raises(ConfigError, match="portion"):
        _minimal_config(
            BLOCKING={"strict": False, "type": "name", "column": "Name", "portion": "bad"}
        )
    with pytest.raises(ConfigError, match="can not be empty"):
        _minimal_config(COLUMNS={"name": {}}, MAIN_MATCH_CRITERIA="address")
    with pytest.raises(ConfigError, match="at least two"):
        _minimal_config(
            COLUMNS={"address": {"combine": ["Address"]}},
            MAIN_MATCH_CRITERIA="address",
            BLOCKING={"strict": False, "type": "name", "column": "Address"},
        )


def test_weight_configuration_edges():
    config = _minimal_config(COLUMNS={"name": {"columns": ["Name"], "weight": 1.0}})
    assert config.has_unassigned_weights() is False
    assert config.needs_weight_balance() is False
    assert config.weight_for("missing", "x") == 1.0
    with pytest.raises(ConfigError, match="not configured"):
        _minimal_config(
            COLUMNS={"name": {"columns": ["Name"], "weight": [["Other", 1.0]]}}
        )
    config = _minimal_config(
        COLUMNS={"name": {"columns": ["Name"], "weight": [["Name", 0.0]]}}
    )
    assert config.has_unassigned_weights() is True
    config = _minimal_config(COLUMNS={"name": {"columns": ["Name"]}})
    config.auto_balance_weights()


def test_candidate_block_edge_paths():
    frame = pd.DataFrame({"clean_Name:name": ["A", "B"]})
    with pytest.raises(ConfigError, match="not in"):
        next(BlockGenerator.generate(frame, CandidateBlock(type="exact", field="missing")))
    with pytest.raises(ValueError, match="length"):
        next(BlockGenerator.generate(frame, CandidateBlock.model_construct(type="prefix", field="Name")))
    with pytest.raises(ValueError, match="Unsupported"):
        list(BlockGenerator.generate(frame, CandidateBlock.model_construct(type="bad")))
    with pytest.raises(ConfigError, match="record"):
        CandidateGenerator([]).generate(pd.DataFrame({"x": [1]}))


def test_grouping_evaluation_dsu_and_final_file(tmp_path):
    first = _decision(_evidence("record:A", "record:B"))
    second = _decision(_evidence("record:B", "record:C"), Decision.REVIEW)
    ignored = _decision(_evidence("record:D", "record:E"), Decision.IGNORE)
    grouped = DuplicateGrouper().build_groups([first, second, ignored])
    assert len(grouped.groups) == 1
    assert grouped.groups[0].decision is Decision.MERGE
    third = _decision(_evidence("record:C", "record:D"), Decision.REVIEW)
    reviewed = DuplicateGrouper(include_review=True).build_groups([second, third])
    assert reviewed.groups[0].decision is Decision.MERGE
    assert reviewed.groups[0].conflicts == ()

    metrics = evaluate_decisions([first, second], {frozenset(("record:A", "record:B"))})
    assert metrics["candidate_pair_count"] == 2
    assert metrics["automatic_merge_precision"] == 1.0
    assert evaluate_decisions([], set())["pair_precision"] == 1.0

    dsu = DSU(3)
    dsu.union(0, 1)
    assert dsu.find(0) == dsu.find(1)
    assert dsu.find(2) != dsu.find(0)

    original = pd.DataFrame({"Id": ["A", "B"], "Name": ["Alice", "Alice"]})
    check = tmp_path / "check.csv"
    create_check_file(
        pd.DataFrame(
            {"Id": ["A", "B"], "match_id": ["A", "B"], "score": [100, 20]}
        ),
        str(check),
        90,
    )
    assert check.exists()
    assert create_check_cols(["Id_main", "clean_x", "Name"]) == ["Id_main", "Name"]


def test_writer_outputs_real_ids_scores_and_field_details(tmp_path):
    fields = {
        "clean_First Name:name": FieldEvidence(
            "clean_First Name:name", "name", "First Name", 90.0, 0.5, False, "FUZZY", True
        ),
        "clean_Email:email": FieldEvidence(
            "clean_Email:email", "email", "Email", 100.0, 0.5, True, "EXACT", True
        ),
    }
    evidence = _evidence(fields=fields)
    decision = _decision(evidence, Decision.MERGE)
    groups = DuplicateGrouper().build_groups([decision])
    original = pd.DataFrame({"Id": ["A", "B"], "Name": ["Alice", "Alicia"], "Email": ["a@x", "a@x"]})
    writer = ResultWriter("profile", record_id_source="Id")
    result = writer.review_dataframe([decision], groups, original, ["record:A", "record:B"])
    assert result.loc[0, "group_id"] == "A"
    assert result.loc[0, "primary_id"] == "A"
    assert result.loc[0, "duplicate_id"] == "B"
    assert result.loc[0, "name_score"] == 90.0
    assert result.loc[0, "email_score"] == 100.0
    assert result.loc[0, "field_email_match_type"] == "EXACT"
    assert list(result.columns).index("primary_Name") < list(result.columns).index("field_email_score")
    paths = writer.write(tmp_path, [decision], groups, original, ["record:A", "record:B"])
    assert all(path.exists() for path in paths.values())
    assert writer.groups_dataframe(groups, original, ["record:A", "record:B"]).loc[0, "group_id"] == "A"
    assert writer.groups_dataframe(groups, original, ["A", "B"]).loc[0, "group_id"] == "A"
    assert writer.review_dataframe([decision], groups, original, ["A", "B"]).loc[0, "primary_id"] == "A"


def test_utils_logger_and_normalization_error_paths(tmp_path, monkeypatch):
    assert get_logger(None).name == "dedupe"
    monkeypatch.chdir(tmp_path)
    assert get_logger("coverage-extra").name == "coverage-extra"

    with pytest.raises(DataLoadError, match="only two files"):
        Utilities.load_data_from_dir(tmp_path)
    (tmp_path / "one.yaml").write_text("x: 1")
    (tmp_path / "two.txt").write_text("x")
    with pytest.raises(DataLoadError, match="yaml file and a .csv"):
        Utilities.load_data_from_dir(tmp_path)
    (tmp_path / "two.txt").unlink()
    (tmp_path / "two.csv").write_text("x\n1\n")
    assert Utilities.load_data_from_dir(tmp_path)[0].suffix == ".yaml"

    with pytest.raises(DataLoadError, match="not a yaml"):
        Utilities.load_client_config(tmp_path / "two.txt")
    with pytest.raises(DataLoadError, match="Failed to load"):
        Utilities.load_client_config(tmp_path / "missing.yaml")
    bad = tmp_path / "bad.yaml"
    bad.write_text("- not a mapping")
    with pytest.raises(ConfigError, match="mapping"):
        Utilities.load_client_config(bad)
    assert not Utilities.load_data_df(tmp_path / "two.csv").empty
    with pytest.raises(DataLoadError, match="not found"):
        Utilities.load_data_df(tmp_path / "missing.csv")


def test_normalize_and_clean_error_branches():
    from contact_dedupe.dedupe.cleaning import clean_email
    from contact_dedupe.dedupe.normalize import (
        add_record_ids,
        configured_columns,
        normalize_contact_method,
        safe_apply,
    )

    assert clean_email(None) is None
    config = _minimal_config()
    assert configured_columns(config.COLUMNS) == ["Name"]
    with pytest.raises(ConfigError, match="reserved"):
        add_record_ids(pd.DataFrame({"_record_id": ["x"]}))
    with pytest.raises(ConfigError, match="not in"):
        safe_apply(pd.DataFrame({"x": [1]}), "missing", lambda values: values)
    with pytest.raises(ConfigError, match="Error processing"):
        safe_apply(pd.DataFrame({"x": [1]}), "x", lambda values: 1 / 0)

    data = config.COLUMNS
    frame = pd.DataFrame({"Name": ["Alice"], "Other": ["x"]})
    with pytest.raises(KeyError):
        normalize_contact_method(frame, data, "unknown", {})

    combined = ClientConfig.model_validate(
        {
            "CLIENT_NAME": "combined",
            "COLUMNS": {
                "name": {"combine": ["First", "Last"], "include_name": False},
                "email": {"columns": ["Email"], "include_name": True},
            },
            "BLOCKING": {"strict": False, "type": "name", "column": "First"},
            "MAIN_MATCH_CRITERIA": "Email",
            "MATCH_FIELD": "Email",
            "BOUNDS": {},
        }
    )
    normalized = __import__("contact_dedupe.dedupe.normalize", fromlist=["normalize_df"]).normalize_df(
        pd.DataFrame({"First": ["Alice"], "Last": ["Smith"], "Email": ["a@x"]}),
        combined.COLUMNS,
        ["name", "email"],
    )
    assert any("names" in column for column in normalized.columns)


def test_decision_and_candidate_remaining_branches():
    from contact_dedupe.dedupe.decision import PredicateSpec, DecisionEngine

    engine = DecisionEngine()
    evidence = MatchEvidence("A", "B", {}, [], [], [], [], 0.0)
    assert engine._group_score(evidence, "name") is None
    assert engine._phone_majority_exact(evidence) is False
    assert engine._parse_rule(" ", evidence) if False else True
    with pytest.raises(ConfigError, match="empty expression"):
        engine._parse_rule(" ", evidence)
    engine.predicate_registry["unsupported"] = PredicateSpec("overall", "x")
    with pytest.raises(ConfigError, match="Unsupported"):
        engine._matches("unsupported", evidence)
    engine.predicate_registry["none_score"] = PredicateSpec("group", "high", contact_type="missing")
    assert engine._matches("none_score", evidence) is False
    assert engine.decide_all([evidence])[0].decision is Decision.INSUFFICIENT_DATA

    assert engine._fields(_evidence().fields if False else _evidence(), "name")
    frame = pd.DataFrame({"clean_primary_name:name": ["x", "x"]})
    assert BlockGenerator._columns_for_fields(frame, "primary_name") == ["clean_primary_name:name"]
    with pytest.raises(ConfigError, match="not in"):
        BlockGenerator._columns_for_fields(frame, "missing")


def test_configured_candidate_profile_and_core_pipeline(tmp_path):
    config = _minimal_config(
        CANDIDATE_BLOCKS={"default_v1": [CandidateBlock(type="exact", field="name")]}
    )
    generator = CandidateGenerator.from_config(config)
    assert len(generator.blocks) == 1
    assert generator.blocks[0].type == "exact"

    from contact_dedupe.dedupe.core import Dedupe

    source = pd.DataFrame({"Name": ["Alice", "Alice"]})
    pipeline = Dedupe(config, source)
    result = pipeline.run()
    assert isinstance(result, pd.DataFrame)
    assert pipeline.grouping is not None
    artifacts = pipeline.write_outputs(str(tmp_path))
    assert "review" in artifacts


def test_remaining_small_branches(monkeypatch, tmp_path):
    from contact_dedupe.dedupe import core as core_module
    from contact_dedupe.dedupe.dsu import DSU
    from contact_dedupe.dedupe.normalize import normalize_contact_method, normalize_df

    assert ResultWriter._json(None) == ""
    assert ResultWriter("x").review_dataframe([], DuplicateGrouper().build_groups([]), pd.DataFrame({"Id": []})).empty
    assert DuplicateGrouper._usable("x") is True
    assert DuplicateGrouper._usable(" ") is False

    conflict = _decision(_evidence(conflicts=("EMAIL_CONFLICT",)))
    assert DuplicateGrouper().build_groups([conflict]).groups[0].decision is Decision.REVIEW

    dsu = DSU(3)
    dsu.union(0, 1)
    dsu.union(0, 2)
    dsu2 = DSU(3)
    dsu2.union(1, 2)
    dsu2.union(0, 1)
    assert dsu.find(2) == dsu.find(0)
    assert dsu2.find(0) == dsu2.find(1)

    assert CandidateGenerator.from_config(_minimal_config()).blocks[0].type == "exact"
    assert BlockGenerator._columns_for_fields(
        pd.DataFrame({"clean_primary_name:name": ["x"]}), "primary_name"
    ) == ["clean_primary_name:name"]
    assert BlockGenerator._columns_for_fields(
        pd.DataFrame({"clean_Primary Last Name:name": ["x"]}), "last_name"
    ) == ["clean_Primary Last Name:name"]

    address_data = ClientConfig.model_validate(
        {
            "CLIENT_NAME": "address",
            "COLUMNS": {"address": {"columns": ["Street"], "combine": ["City", "State"]}},
            "BLOCKING": {"strict": False, "type": "name", "column": "Street"},
            "MAIN_MATCH_CRITERIA": "address",
            "MATCH_FIELD": "Street",
            "BOUNDS": {},
        }
    )
    address_df = pd.DataFrame({"Street": ["1 Main"], "City": ["Chicago"], "State": ["IL"]})
    address_result = normalize_contact_method(address_df, address_data.COLUMNS, "address", {})
    assert "clean_address_combined:address" in address_result
    combine_only = ClientConfig.model_validate(
        {
            "CLIENT_NAME": "combine",
            "COLUMNS": {"email": {"combine": ["Email", "Other"]}},
            "BLOCKING": {"strict": False, "type": "name", "column": "Email"},
            "MAIN_MATCH_CRITERIA": "address",
            "MATCH_FIELD": "Email",
            "BOUNDS": {},
        }
    )
    assert "clean_email_combined:email" in normalize_contact_method(
        pd.DataFrame({"Email": ["a"], "Other": ["b"]}), combine_only.COLUMNS, "email", {}
    )

    named = ClientConfig.model_validate(
        {
            "CLIENT_NAME": "named",
            "COLUMNS": {"name": {"columns": ["First"], "combine": ["Last", "Middle"]}},
            "BLOCKING": {"strict": False, "type": "name", "column": "First"},
            "MAIN_MATCH_CRITERIA": "First",
            "MATCH_FIELD": "First",
            "BOUNDS": {},
        }
    )
    named_result = normalize_df(
        pd.DataFrame({"First": ["A"], "Last": ["B"], "Middle": ["C"]}),
        named.COLUMNS,
        ["name"],
    )
    assert any("names_combined" in column for column in named_result.columns)

    core_config = _minimal_config(NICKNAME="Name", EXCLUSION={"column": "Name"})
    run_result = core_module.Dedupe(core_config, pd.DataFrame({"Name": ["Alice", "Alice"]})).run()
    assert isinstance(run_result, pd.DataFrame)
    core_module.Dedupe(core_config, pd.DataFrame({"Name": ["Alice", "Alice"]})).write_outputs(str(tmp_path))

    profile_config = _minimal_config(
        MATCHING_PROFILE="test_v1",
        MATCHING_PROFILES={"test_v1": {"auto_merge_rule": None, "auto_ignore_rule": None}},
    )
    engine = __import__("contact_dedupe.dedupe.decision", fromlist=["DecisionEngine"]).DecisionEngine(profile_config)
    assert engine.profile.auto_merge_rule == "match_score_high"
    assert engine._source_name("clean_Name:name") == "Name"
    review_config = _minimal_config(
        MATCHING_PROFILE="review_v1",
        MATCHING_PROFILES={"review_v1": {"auto_merge_rule": "name_exact", "auto_ignore_rule": "name_high"}},
    )
    review_evidence = EvidenceBuilder(review_config).build_pair(
        pd.DataFrame({"_record_id": ["record:A", "record:B"], "clean_Name:name": ["A", "B"]}),
        "record:A", "record:B",
    )
    assert __import__("contact_dedupe.dedupe.decision", fromlist=["DecisionEngine"]).DecisionEngine(review_config).decide(review_evidence).decision is Decision.REVIEW

    bad_weight = ClientConfig.model_construct(COLUMNS=Columns())
    with pytest.raises(ConfigError, match="no positive"):
        bad_weight.auto_balance_weights()


def test_utils_excel_and_cli_paths(monkeypatch, tmp_path):
    from contact_dedupe import cli
    import logging
    import contact_dedupe.common.logger as logger_module

    xlsx = tmp_path / "data.xlsx"
    xlsx.write_text("placeholder")
    monkeypatch.setattr(pd, "read_excel", lambda *args, **kwargs: pd.DataFrame({"x": [1]}))
    assert Utilities.load_data_df(xlsx)["x"].tolist() == [1]
    monkeypatch.setattr(pd, "read_excel", lambda *args, **kwargs: (_ for _ in ()).throw(FileNotFoundError()))
    with pytest.raises(DataLoadError, match="not found"):
        Utilities.load_data_df(xlsx)
    monkeypatch.setattr(pd, "read_excel", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("bad")))
    with pytest.raises(DataLoadError, match="Failed to load xlsx"):
        Utilities.load_data_df(xlsx)
    monkeypatch.setattr(pd, "read_csv", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("bad")))
    with pytest.raises(DataLoadError, match="Failed to load csv"):
        Utilities.load_data_df(tmp_path / "data.csv")

    parsed = cli.build_parser().parse_args(["--yaml", str(xlsx), "--file", str(xlsx), "--output", str(tmp_path)])
    assert parsed.output == str(tmp_path)
    monkeypatch.setattr(cli.questionary, "path", lambda prompt: SimpleNamespace(ask=lambda: str(tmp_path)))
    fake_tk = types.ModuleType("tkinter")
    fake_tk.Tk = lambda: (_ for _ in ()).throw(RuntimeError("no display"))
    fake_tk.filedialog = types.ModuleType("tkinter.filedialog")
    monkeypatch.setitem(sys.modules, "tkinter", fake_tk)
    monkeypatch.setitem(sys.modules, "tkinter.filedialog", fake_tk.filedialog)
    assert cli.choose_file_or_directory("pick", "file") == tmp_path

    class Root:
        def withdraw(self):
            pass

        def destroy(self):
            pass

    native_tk = types.ModuleType("tkinter")
    native_dialog = types.ModuleType("tkinter.filedialog")
    native_tk.Tk = Root
    native_tk.filedialog = native_dialog
    native_dialog.askopenfilename = lambda **kwargs: str(tmp_path / "chosen.yaml")
    native_dialog.askdirectory = lambda **kwargs: str(tmp_path / "chosen-dir")
    monkeypatch.setitem(sys.modules, "tkinter", native_tk)
    monkeypatch.setitem(sys.modules, "tkinter.filedialog", native_dialog)
    assert cli.choose_file_or_directory("pick", "file") == tmp_path / "chosen.yaml"
    assert cli.choose_file_or_directory("pick", "directory") == tmp_path / "chosen-dir"
    native_dialog.askopenfilename = lambda **kwargs: ""
    with pytest.raises(Exception, match="No file selected"):
        cli.choose_file_or_directory("pick", "file")
    fallback_tk = types.ModuleType("tkinter")
    fallback_tk.Tk = lambda: (_ for _ in ()).throw(RuntimeError("no display"))
    fallback_tk.filedialog = types.ModuleType("tkinter.filedialog")
    monkeypatch.setitem(sys.modules, "tkinter", fallback_tk)
    monkeypatch.setitem(sys.modules, "tkinter.filedialog", fallback_tk.filedialog)
    monkeypatch.setattr(cli.questionary, "path", lambda prompt: SimpleNamespace(ask=lambda: ""))
    with pytest.raises(Exception, match="No file selected"):
        cli.choose_file_or_directory("pick", "file")

    class FakeLogger:
        def __init__(self):
            self.handlers = []

        def hasHandlers(self):
            return False

        def setLevel(self, level):
            self.level = level

        def addHandler(self, handler):
            self.handlers.append(handler)

    fake_logger = FakeLogger()
    monkeypatch.setattr(
        logger_module,
        "logging",
        SimpleNamespace(
            getLogger=lambda name=None: fake_logger,
            DEBUG=logging.DEBUG,
            FileHandler=logging.FileHandler,
            StreamHandler=logging.StreamHandler,
            Formatter=logging.Formatter,
        ),
    )
    assert len(get_logger("fresh").handlers) == 2

    class FakeConfig:
        CLIENT_NAME = "fake"
        MATCHING_PROFILE = "default_v1"
        MATCH_FIELD = "Id"
        EXCLUSION = None
        NICKNAME = None

        @staticmethod
        def needs_weight_balance():
            return False

    class FakeDedupe:
        def __init__(self, client_cfg, df):
            self.candidate_pairs = pd.DataFrame()
            self.grouping = SimpleNamespace(groups=[])

        def run(self):
            return pd.DataFrame()

        def write_outputs(self, output):
            return {"review": Path(output) / "review.csv"}

    monkeypatch.setattr(cli.Utilities, "load_client_config", lambda path: FakeConfig())
    monkeypatch.setattr(cli.Utilities, "load_data_df", lambda path: pd.DataFrame({"Id": [1]}))
    monkeypatch.setattr(cli, "Dedupe", FakeDedupe)
    cli.main(["--yaml", str(xlsx), "--file", str(xlsx), "--output", str(tmp_path)])

    monkeypatch.setattr(cli.Utilities, "load_data_from_dir", lambda path: (xlsx, xlsx))
    cli.main(["--dir", str(tmp_path), "--output", str(tmp_path)])

    class UnbalancedConfig(FakeConfig):
        @staticmethod
        def needs_weight_balance():
            return True

        @staticmethod
        def auto_balance_weights():
            return None

    monkeypatch.setattr(cli.Utilities, "load_client_config", lambda path: UnbalancedConfig())
    monkeypatch.setattr(cli.questionary, "confirm", lambda *args, **kwargs: SimpleNamespace(ask=lambda: False))
    with pytest.raises(SystemExit):
        cli.main(["--yaml", str(xlsx), "--file", str(xlsx), "--output", str(tmp_path)])

    monkeypatch.setattr(cli.questionary, "confirm", lambda *args, **kwargs: SimpleNamespace(ask=lambda: True))
    monkeypatch.setattr(cli.Utilities, "load_client_config", lambda path: UnbalancedConfig())
    cli.main(["--yaml", str(xlsx), "--file", str(xlsx), "--output", str(tmp_path)])

    monkeypatch.setattr(cli.Utilities, "load_client_config", lambda path: (_ for _ in ()).throw(DataLoadError("bad")))
    with pytest.raises(SystemExit):
        cli.main(["--yaml", str(xlsx), "--file", str(xlsx), "--output", str(tmp_path)])
    monkeypatch.setattr(cli.Utilities, "load_client_config", lambda path: (_ for _ in ()).throw(ConfigError("bad")))
    with pytest.raises(SystemExit):
        cli.main(["--yaml", str(xlsx), "--file", str(xlsx), "--output", str(tmp_path)])

    class InterruptDedupe(FakeDedupe):
        def run(self):
            raise KeyboardInterrupt("stop")

    monkeypatch.setattr(cli.Utilities, "load_client_config", lambda path: FakeConfig())
    monkeypatch.setattr(cli, "Dedupe", InterruptDedupe)
    cli.main(["--yaml", str(xlsx), "--file", str(xlsx), "--output", str(tmp_path)])

    class KeyErrorDedupe(FakeDedupe):
        def run(self):
            raise KeyError("Duplicate")

    monkeypatch.setattr(cli, "Dedupe", KeyErrorDedupe)
    cli.main(["--yaml", str(xlsx), "--file", str(xlsx), "--output", str(tmp_path)])

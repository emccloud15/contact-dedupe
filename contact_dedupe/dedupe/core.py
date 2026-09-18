"""Public deduplication facade and pipeline orchestration."""

from __future__ import annotations

import pandas as pd

from contact_dedupe.common.models import ClientConfig
from .candidate_generator import CandidateGenerator
from .decision import DecisionEngine, PairDecision
from .evidence import EvidenceBuilder, MatchEvidence
from .grouping import DuplicateGrouper, GroupingResult
from .normalize import normalize_df
from .writer import ResultWriter


class Dedupe:
    """Run normalization, candidate generation, comparison, decisions, and grouping."""

    def __init__(self, client_cfg: ClientConfig, df: pd.DataFrame) -> None:
        self.client_cfg = client_cfg
        self.original_df = df.copy()
        self.main_df = pd.DataFrame()
        self.candidate_pairs = pd.DataFrame()
        self.match_evidence: list[MatchEvidence] = []
        self.pair_decisions: list[PairDecision] = []
        self.grouping: GroupingResult | None = None
        self.result = pd.DataFrame()

    def run(self) -> pd.DataFrame:
        contact_types = [field for field, value in self.client_cfg.COLUMNS if value]
        required = [self.client_cfg.BLOCKING.column, self.client_cfg.MATCH_FIELD]
        if self.client_cfg.NICKNAME:
            required.append(self.client_cfg.NICKNAME)
        if self.client_cfg.EXCLUSION:
            required.append(self.client_cfg.EXCLUSION.column)
        self.main_df = normalize_df(
            self.original_df, self.client_cfg.COLUMNS, contact_types,
            required_columns=required, record_id_source=self.client_cfg.MATCH_FIELD,
        )
        self.candidate_pairs = CandidateGenerator.from_config(self.client_cfg).generate(self.main_df)
        self.match_evidence = EvidenceBuilder(self.client_cfg).build_all(
            self.main_df, self.candidate_pairs
        )
        self.pair_decisions = DecisionEngine(self.client_cfg).decide_all(self.match_evidence)
        self.grouping = DuplicateGrouper().build_groups(self.pair_decisions)
        self.result = ResultWriter(
            self.client_cfg.MATCHING_PROFILE, self.client_cfg.MATCHING_PROFILE,
            self.client_cfg.MATCH_FIELD,
        ).review_dataframe(self.pair_decisions, self.grouping, self.original_df,
                           self.main_df["_record_id"].tolist())
        return self.result

    def write_outputs(self, output_dir: str) -> dict[str, object]:
        if self.grouping is None:
            self.run()
        assert self.grouping is not None
        writer = ResultWriter(self.client_cfg.MATCHING_PROFILE, self.client_cfg.MATCHING_PROFILE,
                              self.client_cfg.MATCH_FIELD)
        artifacts = writer.write(output_dir, self.pair_decisions, self.grouping, self.original_df,
                                 self.main_df["_record_id"].tolist())
        return {key: value for key, value in artifacts.items()}

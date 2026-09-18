from .core import Dedupe
from .candidate_generator import CandidateGenerator
from .evidence import EvidenceBuilder, FieldEvidence, MatchEvidence
from .decision import Decision, DecisionEngine, PairDecision
from .grouping import DuplicateGroup, DuplicateGrouper, GroupingResult
from .writer import ResultWriter
from .evaluation import evaluate_decisions
from contact_dedupe.common.utils import Utilities
from contact_dedupe.common.models import ClientConfig

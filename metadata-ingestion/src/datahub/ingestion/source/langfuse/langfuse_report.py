from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class LangfuseSourceReport(StaleEntityRemovalSourceReport):
    traces_scanned: int = 0
    traces_filtered: int = 0
    generations_scanned: int = 0
    non_generation_observations_skipped: int = 0

    scores_attached: int = 0
    scores_dropped_unattachable_subject: int = 0
    scores_dropped_unattachable_subjects_sample: LossyList[str] = field(
        default_factory=LossyList
    )

    prompts_scanned: int = 0
    prompt_versions_scanned: int = 0
    prompts_filtered: int = 0

    def report_score_dropped(self, score_id: str, subject_kind: str) -> None:
        self.scores_dropped_unattachable_subject += 1
        self.scores_dropped_unattachable_subjects_sample.append(
            f"{score_id} (subject_kind={subject_kind})"
        )

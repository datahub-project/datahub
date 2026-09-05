"""Processing context for tracking state and building lineage during Pentaho file processing."""

from typing import Dict, List, Set

from datahub.ingestion.source.pentaho.config import PentahoSourceConfig


class ProcessingContext:
    """Context for tracking processing state and building lineage."""

    def __init__(self, file_path: str, config: PentahoSourceConfig):
        self.file_path = file_path
        self.config = config
        self.input_datasets: Set[str] = set()
        self.output_datasets: Set[str] = set()
        self.custom_properties: Dict[str, str] = {}
        self.step_sequence: List[Dict[str, str]] = []  # Track step processing order

    def add_input_dataset(self, dataset_urn: str) -> None:
        """Add an input dataset URN to the context."""
        if dataset_urn:
            self.input_datasets.add(dataset_urn)

    def add_output_dataset(self, dataset_urn: str) -> None:
        """Add an output dataset URN to the context."""
        if dataset_urn:
            self.output_datasets.add(dataset_urn)

    def add_step_info(self, step_name: str, step_type: str) -> None:
        """Track step processing for debugging."""
        self.step_sequence.append({"name": step_name, "type": step_type})

    def sorted_input_datasets(self) -> List[str]:
        """Input dataset URNs in a stable order.

        Set iteration order depends on per-process string hashing, so emitting
        the sets directly makes DataJobInputOutput differ between runs over the
        same file and churns the aspect on every ingestion.
        """
        return sorted(self.input_datasets)

    def sorted_output_datasets(self) -> List[str]:
        """Output dataset URNs in a stable order. See sorted_input_datasets."""
        return sorted(self.output_datasets)

    def get_custom_properties(self) -> Dict[str, str]:
        return {
            "source": "Pentaho",
            "file_path": self.file_path,
            "input_count": str(len(self.input_datasets)),
            "output_count": str(len(self.output_datasets)),
            "steps_processed": str(len(self.step_sequence)),
            **self.custom_properties,
        }

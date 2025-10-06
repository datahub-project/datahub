from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class Dataset:
    name: str
    display_name: str
    fields: List[Dict] = field(default_factory=list)
    platform: str = "snaplogic"
    platform_instance: Optional[str] = None
    type: Optional[str] = None  # INPUT or OUTPUT
    env: str = "PROD"


@dataclass
class Pipeline:
    name: str
    id: str
    namespace: str


@dataclass
class Task:
    name: str
    id: str
    namespace: str


@dataclass
class ColumnMapping:
    input_dataset: Dataset
    output_dataset: Dataset
    input_field: str
    output_field: str


class SnapLogicParser:
    def __init__(self, case_insensitive_namespaces: list[str], namespace_mapping: dict):
        self.case_insensitive_namespaces = case_insensitive_namespaces
        self.namespace_mapping = namespace_mapping
        self.platform_mapping = {
            "sqlserver": "mssql",
        }

    def _parse_platform(self, namespace: str) -> str:
        type_part = namespace.split("://")[0] if "://" in namespace else namespace

        return self.platform_mapping.get(type_part.lower(), type_part.lower())

    def extract_task_from_lineage(self, lineage: dict) -> Task:
        job = lineage.get("job")
        if not job:
            raise ValueError("Job information is missing in the lineage data.")
        name = job.get("name")
        namespace = job.get("namespace")

        return Task(
            id=name,
            name=name.rsplit(":", 1)[0],
            namespace=self._parse_platform(namespace),
        )

    def extract_pipeline_from_lineage(self, lineage: dict) -> Pipeline:
        parent_run = lineage.get("run", {}).get("facets", {}).get("parent", {})
        job = parent_run.get("job", {})
        name = job.get("name")
        namespace = job.get("namespace")
        pipeline_snode_id = parent_run.get("_producer").split("#pipe_snode=")[1]
        return Pipeline(
            id=pipeline_snode_id, name=name, namespace=self._parse_platform(namespace)
        )

    def _get_case_sensitive_value(self, value: str, namespace: str) -> str:
        """Transform value to lowercase if namespace is case-insensitive."""
        return value.lower() if namespace in self.case_insensitive_namespaces else value

    def _create_dataset_info(
        self,
        namespace: str,
        name: str,
        display_name: str,
        type: str,
        fields: Optional[List[Dict]] = None,
    ) -> Dataset:
        """Create a Dataset instance with proper case sensitivity."""
        return Dataset(
            platform=self._parse_platform(namespace),
            name=self._get_case_sensitive_value(name, namespace),
            display_name=display_name or name,
            fields=fields or [],
            env="PROD",
            platform_instance=self.namespace_mapping.get(namespace, None),
            type=type,
        )

    def extract_columns_mapping_from_lineage(
        self, lineage: dict
    ) -> List[ColumnMapping]:
        outputs = lineage.get("outputs", [])
        lineages = []

        for output in outputs:
            output_namespace = output.get("namespace")
            output_name = output.get("name", "")
            column_lineage = (
                output.get("facets", {}).get("columnLineage", {}).get("fields", {})
            )

            for field_name, field_dict in column_lineage.items():
                output_field = self._get_case_sensitive_value(
                    field_name, output_namespace
                )

                for input_field in field_dict.get("inputFields", []):
                    input_namespace = input_field.get("namespace")
                    input_name = input_field.get("name", "")
                    input_field_name = input_field.get("field", "")

                    lineages.append(
                        ColumnMapping(
                            input_dataset=self._create_dataset_info(
                                input_namespace, input_name, input_name, "INPUT"
                            ),
                            output_dataset=self._create_dataset_info(
                                output_namespace, output_name, output_name, "OUTPUT"
                            ),
                            input_field=self._get_case_sensitive_value(
                                input_field_name, input_namespace
                            ),
                            output_field=output_field,
                        )
                    )

        return lineages

    def extract_datasets_from_lineage(self, lineage: dict) -> List[Dataset]:
        inputs = lineage.get("inputs", {})
        outputs = lineage.get("outputs", {})

        datasets = []
        for dataset, dataset_type in [
            *[(input_dataset, "INPUT") for input_dataset in inputs],
            *[(output_dataset, "OUTPUT") for output_dataset in outputs],
        ]:
            namespace = dataset.get("namespace")
            name = dataset.get("name", "")
            fields = dataset.get("facets", {}).get("schema", {}).get("fields", [])
            display_name = name

            # Transform names to lowercase if namespace is in case_insensitive_namespaces
            if namespace in self.case_insensitive_namespaces:
                name = name.lower()
                fields = [
                    {**field, "name": field.get("name", "").lower()} for field in fields
                ]

            datasets.append(
                self._create_dataset_info(
                    namespace=namespace,
                    name=name,
                    fields=fields,
                    display_name=display_name,
                    type=dataset_type,
                )
            )

        return datasets

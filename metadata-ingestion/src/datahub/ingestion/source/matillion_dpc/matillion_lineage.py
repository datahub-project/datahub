import logging
from typing import Dict, List, Mapping, Optional, Set, Union
from urllib.parse import urlparse

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.ingestion.source.matillion_dpc.config import NamespacePlatformMapping
from datahub.ingestion.source.matillion_dpc.constants import (
    LOWERCASE_FIELD_PLATFORMS,
    PLATFORM_MAPPING,
)
from datahub.ingestion.source.matillion_dpc.models import (
    MatillionColumnLineageInfo,
    MatillionDatasetInfo,
    MatillionPlatformInstanceInfo,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

logger = logging.getLogger(__name__)


class OpenLineageParser:
    def __init__(
        self,
        namespace_to_platform_instance: Optional[
            Mapping[str, Union[Dict[str, str], NamespacePlatformMapping]]
        ] = None,
        platform_mapping: Optional[Dict[str, str]] = None,
        env: str = "PROD",
    ):
        self.namespace_to_platform_instance: Mapping[
            str, Union[Dict[str, str], NamespacePlatformMapping]
        ] = namespace_to_platform_instance or {}
        self.platform_mapping = platform_mapping or PLATFORM_MAPPING
        self.default_env = env

    def _normalize_field_name(self, field_name: str, platform: str) -> str:
        if platform in LOWERCASE_FIELD_PLATFORMS:
            return field_name.lower()
        return field_name

    def _parse_namespace(self, namespace: str) -> tuple[str, str]:
        if "://" in namespace:
            parsed = urlparse(namespace)
            platform = parsed.scheme
            host_identifier = parsed.netloc or parsed.path.lstrip("/")
        else:
            platform = namespace
            host_identifier = namespace

        platform = self.platform_mapping.get(platform.lower(), platform.lower())

        return platform, host_identifier

    def _get_platform_instance_info(
        self, namespace: str
    ) -> MatillionPlatformInstanceInfo:
        matching_prefix = None
        for prefix in self.namespace_to_platform_instance:
            if namespace.startswith(prefix):
                if matching_prefix is None or len(prefix) > len(matching_prefix):
                    matching_prefix = prefix

        if matching_prefix:
            mapping = self.namespace_to_platform_instance[matching_prefix]
            if isinstance(mapping, NamespacePlatformMapping):
                return MatillionPlatformInstanceInfo(
                    platform_instance=mapping.platform_instance,
                    env=mapping.env or self.default_env,
                    database=mapping.database,
                    default_schema=mapping.default_schema,
                    convert_urns_to_lowercase=mapping.convert_urns_to_lowercase,
                )
            else:
                return MatillionPlatformInstanceInfo(
                    platform_instance=mapping.get("platform_instance"),
                    env=mapping.get("env", self.default_env),
                    database=mapping.get("database"),
                    default_schema=mapping.get("schema")
                    or mapping.get("default_schema"),
                    convert_urns_to_lowercase=bool(
                        mapping.get("convert_urns_to_lowercase", False)
                    ),
                )

        return MatillionPlatformInstanceInfo(env=self.default_env)

    def _extract_dataset_info(
        self, dataset_dict: Dict, event_type: str = "unknown"
    ) -> Optional[MatillionDatasetInfo]:
        namespace = dataset_dict.get("namespace")
        name = dataset_dict.get("name")

        if not namespace or not name:
            logger.warning(
                f"Missing namespace or name in {event_type} dataset: {dataset_dict}"
            )
            return None

        try:
            platform, _ = self._parse_namespace(namespace)
            info = self._get_platform_instance_info(namespace)

            # When platform_instance is used, DataHub prepends it to the dataset name,
            # so we omit the database from the normalized name
            normalized_name = MatillionDatasetInfo.normalize_name(
                name,
                platform,
                None if info.platform_instance else info.database,
                info.default_schema,
            )

            if info.convert_urns_to_lowercase:
                normalized_name = normalized_name.lower()

            return MatillionDatasetInfo(
                platform=platform,
                name=normalized_name,
                namespace=namespace,
                platform_instance=info.platform_instance,
                env=info.env,
            )
        except Exception as e:
            logger.info(
                f"Skipping {event_type} dataset {namespace}/{name} due to parsing error: {e}"
            )
            return None

    def _make_dataset_urn(self, dataset: MatillionDatasetInfo) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=dataset.platform,
            name=dataset.name,
            env=dataset.env,
            platform_instance=dataset.platform_instance,
        )

    def extract_column_lineage(
        self,
        event: Dict,
        output_dataset: MatillionDatasetInfo,
        input_datasets: List[MatillionDatasetInfo],
    ) -> List[MatillionColumnLineageInfo]:
        column_lineages: List[MatillionColumnLineageInfo] = []

        outputs = event.get("outputs", [])
        if not outputs:
            return column_lineages

        for output in outputs:
            if output.get("namespace") != output_dataset.namespace:
                continue
            if output.get("name") != output_dataset.name:
                continue

            facets = output.get("facets", {})
            column_lineage_facet = facets.get("columnLineage", {})
            fields = column_lineage_facet.get("fields", {})

            input_dataset_map = {(ds.namespace, ds.name): ds for ds in input_datasets}

            for downstream_field, lineage_info in fields.items():
                input_fields_list = lineage_info.get("inputFields", [])

                for input_field in input_fields_list:
                    input_namespace = input_field.get("namespace")
                    input_name = input_field.get("name")
                    input_field_name = input_field.get("field")

                    if not all([input_namespace, input_name, input_field_name]):
                        continue

                    input_dataset_key = (input_namespace, input_name)
                    if input_dataset_key in input_dataset_map:
                        input_dataset = input_dataset_map[input_dataset_key]

                        column_lineages.append(
                            MatillionColumnLineageInfo(
                                downstream_field=downstream_field,
                                upstream_datasets=[input_dataset],
                                upstream_fields=[input_field_name],
                            )
                        )

        return column_lineages

    def extract_sql_from_event(self, event: Dict) -> Optional[str]:
        try:
            job_facets = event.get("job", {}).get("facets", {})
            sql_facet = job_facets.get("sql")
            if sql_facet and isinstance(sql_facet, dict):
                query = sql_facet.get("query")
                if query and isinstance(query, str):
                    return query
        except Exception as e:
            logger.debug(f"Error extracting SQL from OpenLineage event: {e}")

        return None

    def parse_lineage_event(
        self, event: Dict
    ) -> tuple[
        List[MatillionDatasetInfo],
        List[MatillionDatasetInfo],
        List[MatillionColumnLineageInfo],
    ]:
        input_datasets = []
        output_datasets = []
        column_lineages = []

        for input_dict in event.get("inputs", []):
            dataset_info = self._extract_dataset_info(input_dict, "input")
            if dataset_info:
                input_datasets.append(dataset_info)

        for output_dict in event.get("outputs", []):
            dataset_info = self._extract_dataset_info(output_dict, "output")
            if dataset_info:
                output_datasets.append(dataset_info)

        for output_dataset in output_datasets:
            col_lineages = self.extract_column_lineage(
                event, output_dataset, input_datasets
            )
            column_lineages.extend(col_lineages)

        return input_datasets, output_datasets, column_lineages

    def create_upstream_lineage(
        self,
        input_datasets: List[MatillionDatasetInfo],
        output_dataset: MatillionDatasetInfo,
        column_lineages: List[MatillionColumnLineageInfo],
    ) -> UpstreamLineageClass:
        upstreams = []

        for input_dataset in input_datasets:
            upstream_urn = self._make_dataset_urn(input_dataset)
            upstreams.append(
                UpstreamClass(
                    dataset=upstream_urn, type=DatasetLineageTypeClass.TRANSFORMED
                )
            )

        fine_grained_lineages = []
        output_urn = self._make_dataset_urn(output_dataset)

        downstream_to_upstreams: Dict[str, Set[tuple[str, str]]] = {}
        for col_lineage in column_lineages:
            if col_lineage.downstream_field not in downstream_to_upstreams:
                downstream_to_upstreams[col_lineage.downstream_field] = set()

            for i, upstream_dataset in enumerate(col_lineage.upstream_datasets):
                upstream_urn = self._make_dataset_urn(upstream_dataset)
                if i < len(col_lineage.upstream_fields):
                    upstream_field = col_lineage.upstream_fields[i]
                    downstream_to_upstreams[col_lineage.downstream_field].add(
                        (upstream_urn, upstream_field)
                    )

        for downstream_field, upstream_set in downstream_to_upstreams.items():
            upstreams_list = []

            upstream_by_urn: Dict[str, List[str]] = {}
            for upstream_urn, upstream_field in upstream_set:
                if upstream_urn not in upstream_by_urn:
                    upstream_by_urn[upstream_urn] = []
                upstream_by_urn[upstream_urn].append(upstream_field)

            for upstream_urn, upstream_fields in upstream_by_urn.items():
                upstream_platform = output_dataset.platform
                for input_dataset in input_datasets:
                    if self._make_dataset_urn(input_dataset) == upstream_urn:
                        upstream_platform = input_dataset.platform
                        break

                for upstream_field in upstream_fields:
                    normalized_field = self._normalize_field_name(
                        upstream_field, upstream_platform
                    )
                    upstream_field_urn = make_schema_field_urn(
                        upstream_urn, normalized_field
                    )
                    upstreams_list.append(upstream_field_urn)

            if upstreams_list:
                normalized_downstream_field = self._normalize_field_name(
                    downstream_field, output_dataset.platform
                )
                downstream_field_urn = make_schema_field_urn(
                    output_urn, normalized_downstream_field
                )

                fine_grained_lineages.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=upstreams_list,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[downstream_field_urn],
                    )
                )

        return UpstreamLineageClass(
            upstreams=upstreams,
            fineGrainedLineages=fine_grained_lineages or None,
        )

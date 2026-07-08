from typing import Optional

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.workday.config import (
    DataSourcePlatformConfig,
    WorkdayConfig,
)


def resolve_external_upstream_urn(
    data_source_name: str,
    config: WorkdayConfig,
) -> Optional[str]:
    """Resolve a Prism data source to an external warehouse dataset URN.

    Returns None when the data source has no `data_source_platform_mapping`
    entry (it is then treated as a native Workday dataset, not a cross-platform
    upstream). Matching is exact on the data source name.
    """
    mapping: Optional[DataSourcePlatformConfig] = (
        config.data_source_platform_mapping.get(data_source_name)
    )
    if mapping is None or not mapping.platform:
        return None

    dataset_name = mapping.dataset_name or data_source_name
    if mapping.convert_urns_to_lowercase:
        dataset_name = dataset_name.lower()

    env = mapping.env or config.env
    return make_dataset_urn_with_platform_instance(
        platform=mapping.platform,
        name=dataset_name,
        platform_instance=mapping.platform_instance,
        env=env,
    )

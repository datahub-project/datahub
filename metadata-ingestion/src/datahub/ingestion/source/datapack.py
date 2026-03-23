"""DataPack ingestion source for loading curated data packs into DataHub.

This source extends the demo-data concept to support named data packs from
the DataHub registry, arbitrary URLs, time-shifting, and trust verification.

Usage in a recipe:
    source:
      type: datapack
      config:
        pack_name: "bootstrap"         # Load a named pack from the registry
        # OR
        pack_url: "https://..."        # Load from an arbitrary URL
        no_time_shift: false           # Set true to keep original timestamps
        as_of: "2026-01-01T00:00:00Z"  # Time-shift anchor (default: now)
        trust_community: false         # Allow community packs
        trust_custom: false            # Allow custom URL packs
        no_cache: false                # Force re-download
"""

from datetime import datetime, timezone
from typing import Iterable, Optional

from pydantic import Field

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.file import FileSourceConfig, GenericFileSource


class DataPackSourceConfig(ConfigModel):
    """Configuration for the DataPack ingestion source."""

    pack_name: Optional[str] = Field(
        default=None,
        description="Name of a data pack from the registry (e.g. 'bootstrap', 'covid-bigquery').",
    )
    pack_url: Optional[str] = Field(
        default=None,
        description="HTTP(S) URL to an MCP/MCE JSON file. Use instead of pack_name for custom packs.",
    )
    no_time_shift: bool = Field(
        default=False,
        description="If true, load with original timestamps (no time-shifting).",
    )
    as_of: Optional[str] = Field(
        default=None,
        description="ISO 8601 datetime to use as the time-shift target (default: current time).",
    )
    trust_community: bool = Field(
        default=False,
        description="Allow loading community-contributed packs without warning.",
    )
    trust_custom: bool = Field(
        default=False,
        description="Allow loading from unverified URLs without warning.",
    )
    no_cache: bool = Field(
        default=False,
        description="Force re-download even if the pack is cached.",
    )

    def __init__(self, **data: object) -> None:
        super().__init__(**data)
        if not self.pack_name and not self.pack_url:
            raise ValueError("Either 'pack_name' or 'pack_url' must be specified.")


@platform_name("DataPack")
@config_class(DataPackSourceConfig)
@support_status(SupportStatus.TESTING)
class DataPackSource(Source):
    """Load curated data packs into DataHub.

    Data packs are pre-built collections of metadata (MCPs/MCEs) that can be
    loaded from the DataHub registry or arbitrary URLs, with support for
    time-shifting, trust verification, and SHA256 integrity checking.
    """

    def __init__(self, ctx: PipelineContext, config: DataPackSourceConfig):
        super().__init__(ctx)
        self.config = config

        from datahub.cli.datapack.loader import check_trust, download_pack
        from datahub.cli.datapack.models import DataPackInfo, TrustTier
        from datahub.cli.datapack.time_shift import time_shift_file

        # Resolve the pack
        if config.pack_name:
            from datahub.cli.datapack.registry import get_pack

            pack = get_pack(config.pack_name)
        else:
            assert config.pack_url is not None
            pack = DataPackInfo(
                name="custom",
                description=f"Custom pack from {config.pack_url}",
                url=config.pack_url,
                trust=TrustTier.CUSTOM,
            )

        # Trust check
        check_trust(
            pack,
            trust_community=config.trust_community,
            trust_custom=config.trust_custom,
        )

        # Download
        pack_path = download_pack(pack, no_cache=config.no_cache)

        # Time-shift
        effective_path = pack_path
        if not config.no_time_shift and pack.reference_timestamp:
            target_ts = None
            if config.as_of:
                as_of_dt = datetime.fromisoformat(config.as_of)
                if as_of_dt.tzinfo is None:
                    as_of_dt = as_of_dt.replace(tzinfo=timezone.utc)
                target_ts = int(as_of_dt.timestamp() * 1000)

            effective_path = time_shift_file(
                input_path=pack_path,
                reference_timestamp=pack.reference_timestamp,
                target_timestamp=target_ts,
            )

        # Delegate to GenericFileSource
        file_config = FileSourceConfig(path=str(effective_path))
        self.file_source = GenericFileSource(ctx, file_config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self.file_source.get_workunits()

    def get_report(self) -> SourceReport:
        return self.file_source.get_report()

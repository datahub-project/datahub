"""Demo Data ingestion source for loading curated data packs into DataHub.

By default, loads the "bootstrap" pack with no time-shifting (backward compatible).
Can also load named packs from the registry or arbitrary URLs, with support for
time-shifting, trust verification, and SHA256 integrity checking.

Usage in a recipe:
    # Zero-config (loads bootstrap data):
    source:
      type: demo-data

    # Load a specific pack with time-shifting:
    source:
      type: demo-data
      config:
        pack_name: "showcase-ecommerce"
        no_time_shift: false
"""

import json
import tempfile
import uuid
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


class DemoDataConfig(ConfigModel):
    """Configuration for the Demo Data source.

    With no configuration, loads the "bootstrap" pack with original timestamps.
    """

    pack_name: Optional[str] = Field(
        default="bootstrap",
        description="Name of a data pack from the registry (e.g. 'bootstrap', 'showcase-ecommerce').",
    )
    pack_url: Optional[str] = Field(
        default=None,
        description="HTTP(S) URL to an MCP/MCE JSON file. Use instead of pack_name for custom packs.",
    )
    no_time_shift: bool = Field(
        default=True,
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


@platform_name("Demo Data")
@config_class(DemoDataConfig)
@support_status(SupportStatus.UNKNOWN)
class DemoDataSource(Source):
    """Load curated data packs into DataHub.

    By default, loads the "bootstrap" sample data pack. Can also load named packs
    from the DataHub registry or arbitrary URLs, with support for time-shifting,
    trust verification, and SHA256 integrity checking.
    """

    def __init__(self, ctx: PipelineContext, config: DemoDataConfig):
        super().__init__(ctx)
        self.config = config

        from datahub.cli.datapack.loader import (
            check_trust,
            download_pack,
            ingest_datapack_file_entries,
        )
        from datahub.cli.datapack.models import DataPackInfo, TrustTier

        # Resolve the pack
        if config.pack_url:
            pack = DataPackInfo(
                name="custom",
                description=f"Custom pack from {config.pack_url}",
                url=config.pack_url,
                trust=TrustTier.CUSTOM,
            )
        elif config.pack_name:
            from datahub.cli.datapack.registry import get_pack

            pack = get_pack(config.pack_name)
        else:
            raise ValueError("Either 'pack_name' or 'pack_url' must be specified.")

        check_trust(
            pack,
            trust_community=config.trust_community,
            trust_custom=config.trust_custom,
        )

        file_entries = download_pack(pack, no_cache=config.no_cache)

        as_of_dt: Optional[datetime] = None
        if config.as_of:
            as_of_dt = datetime.fromisoformat(config.as_of)
            if as_of_dt.tzinfo is None:
                as_of_dt = as_of_dt.replace(tzinfo=timezone.utc)

        run_id = ctx.run_id or f"demo-data-{uuid.uuid4()}"
        ingest_datapack_file_entries(
            pack,
            file_entries,
            run_id,
            no_time_shift=config.no_time_shift,
            as_of=as_of_dt,
            log_progress=False,
        )

        # Outer pipeline sink is unused; datapack ingest already completed above.
        empty_pack = tempfile.NamedTemporaryFile(
            suffix=".json", delete=False, prefix="demo-data-empty-", mode="w"
        )
        json.dump([], empty_pack)
        empty_pack.close()
        file_config = FileSourceConfig(path=empty_pack.name)
        self.file_source = GenericFileSource(ctx, file_config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self.file_source.get_workunits()

    def get_report(self) -> SourceReport:
        return self.file_source.get_report()

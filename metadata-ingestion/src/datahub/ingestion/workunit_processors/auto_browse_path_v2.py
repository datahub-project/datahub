from functools import partial
from typing import Iterable, List, Optional

from datahub.configuration.source_common import PlatformInstanceConfigMixin
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
)


class AutoBrowsePathV2Processor(WorkunitProcessor):
    """Generate BrowsePathsV2 from Container and BrowsePaths aspects."""

    NAME = "auto_browse_path_v2"

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        super().__init__(ctx)
        flags = ctx.pipeline_context.flags
        self.dry_run: bool = flags.generate_browse_path_v2_dry_run

        config = ctx.source_config
        platform = ctx.source_platform or ctx.infer_platform()
        env = getattr(config, "env", None)
        drop_dirs_raw: List[Optional[str]] = [
            platform,
            platform.lower() if platform else None,
            env,
            env.lower() if env else None,
        ]
        self.drop_dirs: List[str] = [s for s in drop_dirs_raw if s is not None]
        self.platform = platform

        platform_instance: Optional[str] = None
        if isinstance(config, PlatformInstanceConfigMixin) and config.platform_instance:
            platform_instance = config.platform_instance
        self.platform_instance = platform_instance

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        return ctx.pipeline_context.flags.generate_browse_path_v2

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        from datahub.ingestion.api.source_helpers import auto_browse_path_v2

        processor = partial(
            auto_browse_path_v2,
            platform=self.platform,
            platform_instance=self.platform_instance,
            drop_dirs=self.drop_dirs,
            dry_run=self.dry_run,
        )
        return processor(stream)

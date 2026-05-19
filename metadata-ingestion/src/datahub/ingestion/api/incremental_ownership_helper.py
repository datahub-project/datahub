"""Incremental ownership workunit processor.

Two behaviors diverge intentionally from :func:`auto_incremental_properties`:

1. Dataset-only entity filter. ``OwnershipClass`` is attached to ~28 entity
   types (datasets, containers, charts, dashboards, glossary terms, ML models,
   …). The patch path uses ``create_dataset_owners_patch_builder`` →
   ``DatasetPatchBuilder``, which is dataset-specific. We therefore filter
   explicitly by dataset URN (MCE) / ``entityType == "dataset"`` (MCPW);
   non-dataset ownership passes through as UPSERT. Compare
   ``auto_incremental_lineage``, which dispatches across
   dataset/chart/dashboard patch builders for the same reason.

2. Empty owners are dropped silently. ``create_dataset_owners_patch_builder``
   emits one patch op per owner, so an empty ``owners`` list yields zero ops
   and ``build()`` returns ``[]``. The properties helper does not hit this
   case because ``create_dataset_props_patch_builder`` unconditionally sets
   seven fields, producing at least one op regardless of input. The drop here
   is a no-op for incremental semantics (no ops = no change).
"""

import logging
from typing import Iterable, Optional

from pydantic.fields import Field

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import set_aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.source_helpers import create_dataset_owners_patch_builder
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass,
    OwnershipClass,
    SystemMetadataClass,
)

logger = logging.getLogger(__name__)


def convert_ownership_to_patch(
    urn: str,
    aspect: OwnershipClass,
    system_metadata: Optional[SystemMetadataClass] = None,
) -> MetadataWorkUnit:
    patch_builder = create_dataset_owners_patch_builder(urn, aspect, system_metadata)
    mcp = next(iter(patch_builder.build()))
    return MetadataWorkUnit(id=MetadataWorkUnit.generate_workunit_id(mcp), mcp_raw=mcp)


def auto_incremental_ownership(
    incremental_ownership: bool,
    stream: Iterable[MetadataWorkUnit],
) -> Iterable[MetadataWorkUnit]:
    if not incremental_ownership:
        yield from stream
        return  # early exit

    for wu in stream:
        urn = wu.get_urn()

        if isinstance(wu.metadata, MetadataChangeEventClass):
            if urn.startswith("urn:li:dataset:"):
                ownership_aspect = wu.get_aspect_of_type(OwnershipClass)
                set_aspect(wu.metadata, None, OwnershipClass)
                if len(wu.metadata.proposedSnapshot.aspects) > 0:
                    yield wu

                if ownership_aspect and ownership_aspect.owners:
                    yield convert_ownership_to_patch(
                        urn, ownership_aspect, wu.metadata.systemMetadata
                    )
            else:
                yield wu
        elif (
            isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, OwnershipClass)
            and wu.metadata.entityType == "dataset"
        ):
            ownership_aspect = wu.metadata.aspect
            if ownership_aspect.owners:
                yield convert_ownership_to_patch(
                    urn, ownership_aspect, wu.metadata.systemMetadata
                )
        else:
            yield wu


class IncrementalOwnershipConfigMixin(ConfigModel):
    incremental_ownership: bool = Field(
        default=False,
        description="When enabled, emits ownership as incremental to existing ownership "
        "in DataHub. When disabled, re-states ownership on each run.",
    )

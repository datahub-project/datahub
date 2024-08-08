import logging
from dataclasses import dataclass
from typing import Iterable, Optional, Type, TypeVar, Union, overload

from datahub.emitter.aspect import TIMESERIES_ASPECT_MAP
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import WorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.schema_classes import _Aspect

logger = logging.getLogger(__name__)

T_Aspect = TypeVar("T_Aspect", bound=_Aspect)


@dataclass
class MetadataWorkUnit(WorkUnit):
    metadata: Union[
        MetadataChangeEvent, MetadataChangeProposal, MetadataChangeProposalWrapper
    ]

    # A workunit creator can determine if this workunit is allowed to fail.
    # TODO: This flag was initially added during the rollout of the subType aspect
    # to improve backwards compatibility, but is not really needed anymore and so
    # should be removed.
    treat_errors_as_warnings: bool = False

    # When this is set to false, this MWU will be ignored by automatic helpers
    # like auto_status_aspect and auto_stale_entity_removal.
    is_primary_source: bool = True

    @overload
    def __init__(
        self, id: str, mce: MetadataChangeEvent, *, is_primary_source: bool = True
    ):
        # TODO: Force `mce` to be a keyword-only argument.
        ...

    @overload
    def __init__(
        self,
        id: str,
        *,
        mcp_raw: MetadataChangeProposal,
        is_primary_source: bool = True,
    ):
        # We force `mcp_raw` to be a keyword-only argument.
        ...

    @overload
    def __init__(
        self,
        id: str,
        *,
        mcp: MetadataChangeProposalWrapper,
        treat_errors_as_warnings: bool = False,
        is_primary_source: bool = True,
    ):
        # We force `mcp` to be a keyword-only argument.
        ...

    def __init__(
        self,
        id: str,
        mce: Optional[MetadataChangeEvent] = None,
        mcp: Optional[MetadataChangeProposalWrapper] = None,
        mcp_raw: Optional[MetadataChangeProposal] = None,
        treat_errors_as_warnings: bool = False,
        is_primary_source: bool = True,
    ):
        super().__init__(id)
        self.treat_errors_as_warnings = treat_errors_as_warnings
        self.is_primary_source = is_primary_source

        if sum(1 if v else 0 for v in [mce, mcp, mcp_raw]) != 1:
            raise ValueError("exactly one of mce, mcp, or mcp_raw must be provided")

        if mcp_raw:
            assert not (mce or mcp)
            self.metadata = mcp_raw
        elif mce and not mcp:
            self.metadata = mce
        elif mcp and not mce:
            self.metadata = mcp

    def get_metadata(self) -> dict:
        return {"metadata": self.metadata}

    def get_urn(self) -> str:
        if isinstance(self.metadata, MetadataChangeEvent):
            return self.metadata.proposedSnapshot.urn
        else:
            assert self.metadata.entityUrn
            return self.metadata.entityUrn

    @classmethod
    def generate_workunit_id(
        cls,
        item: Union[
            MetadataChangeEvent, MetadataChangeProposal, MetadataChangeProposalWrapper
        ],
    ) -> str:
        if isinstance(item, MetadataChangeEvent):
            return f"{item.proposedSnapshot.urn}/mce"
        elif isinstance(item, (MetadataChangeProposalWrapper, MetadataChangeProposal)):
            if item.aspect and item.aspectName in TIMESERIES_ASPECT_MAP:
                # TODO: Make this a cleaner interface.
                ts = getattr(item.aspect, "timestampMillis", None)
                assert ts is not None

                # If the aspect is a timeseries aspect, include the timestampMillis in the ID.
                return f"{item.entityUrn}-{item.aspectName}-{ts}"

            return f"{item.entityUrn}-{item.aspectName}"
        else:
            raise ValueError(f"Unexpected type {type(item)}")

    def get_aspect_of_type(self, aspect_cls: Type[T_Aspect]) -> Optional[T_Aspect]:
        aspects: list
        if isinstance(self.metadata, MetadataChangeEvent):
            aspects = self.metadata.proposedSnapshot.aspects
        elif isinstance(self.metadata, MetadataChangeProposalWrapper):
            aspects = [self.metadata.aspect]
        elif isinstance(self.metadata, MetadataChangeProposal):
            aspects = []
            # Best effort attempt to deserialize MetadataChangeProposalClass
            if self.metadata.aspectName == aspect_cls.ASPECT_NAME:
                try:
                    mcp = MetadataChangeProposalWrapper.try_from_mcpc(self.metadata)
                    if mcp:
                        aspects = [mcp.aspect]
                except Exception:
                    pass
        else:
            raise ValueError(f"Unexpected type {type(self.metadata)}")

        aspects = [a for a in aspects if isinstance(a, aspect_cls)]
        if len(aspects) > 1:
            logger.warning(f"Found multiple aspects of type {aspect_cls} in MCE {self}")
        return aspects[-1] if aspects else None

    def decompose_mce_into_mcps(self) -> Iterable["MetadataWorkUnit"]:
        from datahub.emitter.mcp_builder import mcps_from_mce

        assert isinstance(self.metadata, MetadataChangeEvent)

        yield from [
            MetadataWorkUnit(
                id=self.id,
                mcp=mcpw,
                treat_errors_as_warnings=self.treat_errors_as_warnings,
            )
            for mcpw in mcps_from_mce(self.metadata)
        ]

    @classmethod
    def from_metadata(
        cls,
        metadata: Union[
            MetadataChangeEvent, MetadataChangeProposal, MetadataChangeProposalWrapper
        ],
        id: Optional[str] = None,
    ) -> "MetadataWorkUnit":
        workunit_id = id or cls.generate_workunit_id(metadata)

        if isinstance(metadata, MetadataChangeEvent):
            return MetadataWorkUnit(id=workunit_id, mce=metadata)
        elif isinstance(metadata, (MetadataChangeProposal)):
            return MetadataWorkUnit(id=workunit_id, mcp_raw=metadata)
        elif isinstance(metadata, MetadataChangeProposalWrapper):
            return MetadataWorkUnit(id=workunit_id, mcp=metadata)
        else:
            raise ValueError(f"Unexpected metadata type {type(metadata)}")

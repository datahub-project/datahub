from __future__ import annotations

import dataclasses
from datetime import datetime
from typing import Dict, Optional

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import make_ts_millis, parse_ts_millis
from datahub.metadata.urns import CorpGroupUrn, CorpUserUrn, Urn
from datahub.sdk._shared import ActorUrn
from datahub.utilities.str_enum import StrEnum


class KnownAttribution(StrEnum):
    INGESTION = "INGESTION"
    INGESTION_ALTERNATE = "INGESTION_ALTERNATE"

    UI = "UI"
    SDK = "SDK"

    PROPAGATION = "PROPAGATION"

    def is_ingestion(self) -> bool:
        return self in (
            KnownAttribution.INGESTION,
            KnownAttribution.INGESTION_ALTERNATE,
        )


_default_attribution = KnownAttribution.SDK


def set_default_attribution(attribution: KnownAttribution) -> None:
    global _default_attribution
    _default_attribution = attribution


def get_default_attribution() -> KnownAttribution:
    return _default_attribution


def is_ingestion_attribution() -> bool:
    return get_default_attribution().is_ingestion()


# TODO: Don't feel great about this interface yet.
@dataclasses.dataclass
class AttributionDetails:
    time: datetime
    actor: ActorUrn
    sourceRaw: Optional[str] = None
    sourceDetailRaw: Optional[Dict[str, str]] = None

    @staticmethod
    def parse_obj(obj: models.MetadataAttributionClass) -> AttributionDetails:
        actor = Urn.from_string(obj.actor)
        assert isinstance(actor, (CorpUserUrn, CorpGroupUrn))

        return AttributionDetails(
            sourceRaw=obj.source,
            time=parse_ts_millis(obj.time),
            actor=actor,
            sourceDetailRaw=obj.sourceDetail,
        )

    def to_obj(self) -> models.MetadataAttributionClass:
        return models.MetadataAttributionClass(
            source=self.sourceRaw,
            time=make_ts_millis(self.time),
            actor=str(self.actor),
            sourceDetail=self.sourceDetailRaw,
        )

    def get_attribution_type(self) -> Optional[KnownAttribution]:
        # TODO fix this
        if self.sourceRaw in KnownAttribution:
            return KnownAttribution(self.sourceRaw)
        return None

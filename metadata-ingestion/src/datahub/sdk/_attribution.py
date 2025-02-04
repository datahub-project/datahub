from __future__ import annotations

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

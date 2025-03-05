from __future__ import annotations

import contextlib
from typing import Iterator

from datahub.utilities.str_enum import StrEnum

# TODO: This attribution setup is not the final form. I expect that once we have better
# backend support for attribution and attribution-oriented patch, this will become a bit
# more sophisticated.


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


def get_default_attribution() -> KnownAttribution:
    return _default_attribution


def set_default_attribution(attribution: KnownAttribution) -> None:
    global _default_attribution
    _default_attribution = attribution


@contextlib.contextmanager
def change_default_attribution(attribution: KnownAttribution) -> Iterator[None]:
    old_attribution = get_default_attribution()
    try:
        set_default_attribution(attribution)
        yield
    finally:
        set_default_attribution(old_attribution)


def is_ingestion_attribution() -> bool:
    return get_default_attribution().is_ingestion()

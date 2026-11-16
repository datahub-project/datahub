from typing import Optional


def description_from_source_label(
    label: Optional[str], identifier: Optional[str]
) -> Optional[str]:
    # SAP exposes a single human-readable label per entity/column (CSN
    # @EndUserText.label, EDMX Common.Label). The metadata model treats a lone source
    # string as a description, and SchemaField.label is deprecated and not rendered by
    # the schema UI, so the label belongs in the visible description. Drop it only when
    # it just repeats the technical identifier so we never duplicate the name.
    if not isinstance(label, str):
        return None
    cleaned = label.strip()
    if not cleaned:
        return None
    if identifier is not None and cleaned.casefold() == identifier.strip().casefold():
        return None
    return cleaned

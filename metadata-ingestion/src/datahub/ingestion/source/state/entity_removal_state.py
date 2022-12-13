from typing import Dict, Iterable, List, Type

import pydantic

from datahub.emitter.mce_builder import make_assertion_urn, make_container_urn
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityCheckpointStateBase,
)
from datahub.utilities.checkpoint_state_util import CheckpointStateUtil
from datahub.utilities.urns.urn import guess_entity_type


class GenericCheckpointState(StaleEntityCheckpointStateBase["GenericCheckpointState"]):
    urns: List[str] = pydantic.Field(default_factory=list)

    @classmethod
    def get_supported_types(cls) -> List[str]:
        return ["*"]

    def add_checkpoint_urn(self, type: str, urn: str) -> None:
        # TODO: dedup
        self.urns.append(urn)

    def get_urns_not_in(
        self, type: str, other_checkpoint_state: "GenericCheckpointState"
    ) -> Iterable[str]:
        diff = set(self.urns) - set(other_checkpoint_state.urns)

        # To maintain backwards compatibility, we provide this filtering mechanism.
        if type == "*":
            yield from diff
        else:
            yield from (urn for urn in diff if guess_entity_type(urn) == type)

    def get_percent_entities_changed(
        self, old_checkpoint_state: "GenericCheckpointState"
    ) -> float:
        return StaleEntityCheckpointStateBase.compute_percent_entities_changed(
            [(self.urns, old_checkpoint_state.urns)]
        )


def pydantic_state_migrator(mapping: Dict[str, str]) -> classmethod:
    # mapping would be something like:
    # {
    #    'encoded_view_urns': 'dataset',
    #    'encoded_container_urns': 'container',
    # }

    SUPPORTED_TYPES = [
        "dataset",
        "container",
        "assertion",
    ]
    assert set(mapping.values()) <= set(SUPPORTED_TYPES)

    def _validate_field_rename(cls: Type, values: dict) -> dict:
        values.setdefault("urns", [])

        for old_field, mapped_type in mapping.items():
            if old_field not in values:
                continue

            value = values.pop(old_field)
            if mapped_type == "dataset":
                values["urns"] += CheckpointStateUtil.get_dataset_urns_not_in(value, [])
            elif mapped_type == "container":
                values["urns"] += [make_container_urn(guid) for guid in value]
            elif mapped_type == "assertion":
                values["urns"] += [make_assertion_urn(encoded) for encoded in value]
            else:
                raise ValueError(f"Unsupported type {mapped_type}")

        return values

    return pydantic.root_validator(pre=True, allow_reuse=True)(_validate_field_rename)

from typing import Any, Dict, Iterable, List, Type

import pydantic

from datahub.emitter.mce_builder import make_assertion_urn, make_container_urn
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityCheckpointStateBase,
)
from datahub.utilities.checkpoint_state_util import CheckpointStateUtil
from datahub.utilities.dedup_list import deduplicate_list
from datahub.utilities.urns.urn import guess_entity_type


class GenericCheckpointState(StaleEntityCheckpointStateBase["GenericCheckpointState"]):
    urns: List[str] = pydantic.Field(default_factory=list)

    # We store a bit of extra internal-only state so that we can keep the urns list deduplicated.
    # However, we still want `urns` to be a list so that it maintains its order.
    # We can't used OrderedSet here because pydantic doesn't recognize it and
    # it isn't JSON serializable.
    _urns_set: set = pydantic.PrivateAttr(default_factory=set)

    def __init__(self, **data: Any):  # type: ignore
        super().__init__(**data)
        self.urns = deduplicate_list(self.urns)
        self._urns_set = set(self.urns)

    @classmethod
    def get_supported_types(cls) -> List[str]:
        return ["*"]

    def add_checkpoint_urn(self, type: str, urn: str) -> None:
        if urn not in self._urns_set:
            self.urns.append(urn)
            self._urns_set.add(urn)

    def get_urns_not_in(
        self, type: str, other_checkpoint_state: "GenericCheckpointState"
    ) -> Iterable[str]:
        diff = set(self.urns) - set(other_checkpoint_state.urns)

        # To maintain backwards compatibility, we provide this filtering mechanism.
        if type == "*":
            yield from diff
        elif type == "topic":
            yield from (urn for urn in diff if guess_entity_type(urn) == "dataset")
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
        "topic",
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
            elif mapped_type == "topic":
                values["urns"] += [
                    CheckpointStateUtil.get_urn_from_encoded_topic(encoded_urn)
                    for encoded_urn in value
                ]
            elif mapped_type == "container":
                values["urns"] += [make_container_urn(guid) for guid in value]
            elif mapped_type == "assertion":
                values["urns"] += [make_assertion_urn(encoded) for encoded in value]
            else:
                raise ValueError(f"Unsupported type {mapped_type}")

        return values

    return pydantic.root_validator(pre=True, allow_reuse=True)(_validate_field_rename)

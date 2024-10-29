from typing import Any, Dict, Iterable, List, Tuple, Type

import pydantic

from datahub.emitter.mce_builder import make_assertion_urn, make_container_urn
from datahub.ingestion.source.state.checkpoint import CheckpointStateBase
from datahub.utilities.checkpoint_state_util import CheckpointStateUtil
from datahub.utilities.dedup_list import deduplicate_list
from datahub.utilities.urns.urn import guess_entity_type

STATEFUL_INGESTION_IGNORED_ENTITY_TYPES = {
    "dataProcessInstance",
    "query",
}


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
                values["urns"] += [
                    CheckpointStateUtil.get_urn_from_encoded_dataset(encoded_urn)
                    for encoded_urn in value
                ]
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


class GenericCheckpointState(CheckpointStateBase):
    urns: List[str] = pydantic.Field(default_factory=list)

    # We store a bit of extra internal-only state so that we can keep the urns list deduplicated.
    # However, we still want `urns` to be a list so that it maintains its order.
    # We can't used OrderedSet here because pydantic doesn't recognize it and
    # it isn't JSON serializable.
    _urns_set: set = pydantic.PrivateAttr(default_factory=set)

    _migration = pydantic_state_migrator(
        {
            # From SQL:
            "encoded_table_urns": "dataset",
            "encoded_view_urns": "dataset",
            "encoded_container_urns": "container",
            "encoded_assertion_urns": "assertion",
            # From kafka:
            "encoded_topic_urns": "topic",
            # From dbt:
            "encoded_node_urns": "dataset",
            # "encoded_assertion_urns": "assertion",  # already handled from SQL
        }
    )

    def __init__(self, **data: Any):  # type: ignore
        super().__init__(**data)
        self.urns = deduplicate_list(self.urns)
        self._urns_set = set(self.urns)

    def add_checkpoint_urn(self, type: str, urn: str) -> None:
        """
        Adds an urn into the list used for tracking the type.

        :param type: Deprecated parameter, has no effect.
        :param urn: The urn string
        """

        # TODO: Deprecate the `type` parameter and remove it.
        if urn not in self._urns_set:
            self.urns.append(urn)
            self._urns_set.add(urn)

    def get_urns_not_in(
        self, type: str, other_checkpoint_state: "GenericCheckpointState"
    ) -> Iterable[str]:
        """
        Gets the urns present in this checkpoint but not the other_checkpoint for the given type.

        :param type: Deprecated. Set to "*".
        :param other_checkpoint_state: the checkpoint state to compute the urn set difference against.
        :return: an iterable to the set of urns present in this checkpoint state but not in the other_checkpoint.
        """

        diff = set(self.urns) - set(other_checkpoint_state.urns)

        # To maintain backwards compatibility, we provide this filtering mechanism.
        # TODO: Deprecate the `type` parameter and remove it.
        if type == "*":
            yield from diff
        elif type == "topic":
            yield from (urn for urn in diff if guess_entity_type(urn) == "dataset")
        else:
            yield from (urn for urn in diff if guess_entity_type(urn) == type)

    def get_percent_entities_changed(
        self, old_checkpoint_state: "GenericCheckpointState"
    ) -> float:
        """
        Returns the percentage of entities that have changed relative to `old_checkpoint_state`.

        :param old_checkpoint_state: the old checkpoint state to compute the relative change percent against.
        :return: (1-|intersection(self, old_checkpoint_state)| / |old_checkpoint_state|) * 100.0
        """

        old_urns_filtered = filter_ignored_entity_types(old_checkpoint_state.urns)

        return compute_percent_entities_changed(
            new_entities=self.urns, old_entities=old_urns_filtered
        )

    def urn_count(self) -> int:
        return len(self.urns)


def compute_percent_entities_changed(
    new_entities: List[str], old_entities: List[str]
) -> float:
    (overlap_count, old_count, _,) = _get_entity_overlap_and_cardinalities(
        new_entities=new_entities, old_entities=old_entities
    )

    if old_count:
        return (1 - overlap_count / old_count) * 100.0
    return 0.0


def _get_entity_overlap_and_cardinalities(
    new_entities: List[str], old_entities: List[str]
) -> Tuple[int, int, int]:
    new_set = set(new_entities)
    old_set = set(old_entities)
    return len(new_set.intersection(old_set)), len(old_set), len(new_set)


def filter_ignored_entity_types(urns: List[str]) -> List[str]:
    # We previously stored ignored entity urns (e.g.dataProcessInstance) in state.
    # For smoother transition from old checkpoint state, without requiring explicit
    # setting of `fail_safe_threshold` due to removal of irrelevant urns from new state,
    # here, we would ignore irrelevant urns from percentage entities changed computation
    # This special handling can be removed after few months.
    return [
        urn
        for urn in urns
        if not any(
            urn.startswith(f"urn:li:{entityType}")
            for entityType in STATEFUL_INGESTION_IGNORED_ENTITY_TYPES
        )
    ]

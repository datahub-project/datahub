from typing import Callable, List, Tuple, Union

from avro.schema import Field, RecordSchema

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DictWrapper,
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
)
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub.utilities.urns.urn import Urn, guess_entity_type

_Path = List[Union[str, int]]


def _add_prefix_to_paths(
    prefix: _Path, items: List[Tuple[str, _Path]]
) -> List[Tuple[str, _Path]]:
    return [(urn, [*prefix, *path]) for urn, path in items]


def list_urns_with_path(
    model: Union[DictWrapper, MetadataChangeProposalWrapper]
) -> List[Tuple[str, _Path]]:
    """List urns in the given model with their paths.

    Args:
        model: The model to list urns from.

    Returns:
        A list of tuples of the form (urn, path), where path is a list of keys.
    """

    urns: List[Tuple[str, _Path]] = []

    if isinstance(model, MetadataChangeProposalWrapper):
        if model.entityUrn:
            urns.append((model.entityUrn, ["entityUrn"]))
        if model.entityKeyAspect:
            urns.extend(
                _add_prefix_to_paths(
                    ["entityKeyAspect"], list_urns_with_path(model.entityKeyAspect)
                )
            )
        if model.aspect:
            urns.extend(
                _add_prefix_to_paths(["aspect"], list_urns_with_path(model.aspect))
            )

        return urns

    schema: RecordSchema = model.RECORD_SCHEMA

    for key, value in model.items():
        if not value:
            continue

        field_schema: Field = schema.fields_dict[key]
        is_urn = field_schema.get_prop("Urn") is not None

        if isinstance(value, DictWrapper):
            urns.extend(_add_prefix_to_paths([key], list_urns_with_path(value)))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, DictWrapper):
                    urns.extend(
                        _add_prefix_to_paths([key, i], list_urns_with_path(item))
                    )
                elif is_urn:
                    urns.append((item, [key, i]))
        elif is_urn:
            urns.append((value, [key]))

    return urns


def list_urns(model: Union[DictWrapper, MetadataChangeProposalWrapper]) -> List[str]:
    """List urns in the given model.

    Args:
        model: The model to list urns from.

    Returns: A list of URNs contained in the given model.
    """

    return [urn for urn, _ in list_urns_with_path(model)]


def transform_urns(
    model: Union[
        DictWrapper,
        MetadataChangeEventClass,
        MetadataChangeProposalClass,
        MetadataChangeProposalWrapper,
    ],
    func: Callable[[str], str],
) -> None:
    """
    Rewrites all URNs in the given object according to the given function.
    """

    for old_urn, path in list_urns_with_path(model):
        new_urn = func(old_urn)
        if old_urn != new_urn:
            _modify_at_path(model, path, new_urn)


def _modify_at_path(
    model: Union[DictWrapper, MetadataChangeProposalWrapper, list],
    path: _Path,
    new_value: str,
) -> None:
    assert len(path) > 0

    if len(path) == 1:
        if isinstance(path[0], int):
            assert isinstance(model, list)
            model[path[0]] = new_value
        elif isinstance(model, DictWrapper):
            model._inner_dict[path[0]] = new_value
        else:  # MCPW
            setattr(model, path[0], new_value)
    elif isinstance(path[0], int):
        assert isinstance(model, list)
        _modify_at_path(model[path[0]], path[1:], new_value)
    elif isinstance(model, DictWrapper):
        _modify_at_path(model._inner_dict[path[0]], path[1:], new_value)
    else:  # MCPW
        _modify_at_path(getattr(model, path[0]), path[1:], new_value)


def _lowercase_dataset_urn(dataset_urn: str) -> str:
    cur_urn = DatasetUrn.from_string(dataset_urn)
    new_urn = DatasetUrn(
        platform=cur_urn.platform, name=cur_urn.name.lower(), env=cur_urn.env
    )
    return str(new_urn)


def lowercase_dataset_urns(
    model: Union[
        DictWrapper,
        MetadataChangeEventClass,
        MetadataChangeProposalClass,
        MetadataChangeProposalWrapper,
    ]
) -> None:
    def modify_urn(urn: str) -> str:
        if guess_entity_type(urn) == "dataset":
            return _lowercase_dataset_urn(urn)
        elif guess_entity_type(urn) == "schemaField":
            cur_urn = Urn.from_string(urn)
            cur_urn._entity_ids[0] = _lowercase_dataset_urn(cur_urn._entity_ids[0])
            return str(cur_urn)
        return urn

    transform_urns(model, modify_urn)

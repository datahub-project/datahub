import json
import logging
import os
import pathlib
import re
import tempfile
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.sink.file import write_metadata_file
from datahub.metadata.schema_classes import MetadataChangeEventClass
from datahub.metadata.urns import Urn
from datahub.testing.compare_metadata_json import (
    assert_metadata_files_equal,
    load_json_file,
)

logger = logging.getLogger(__name__)

IGNORE_PATH_TIMESTAMPS = [
    # Ignore timestamps from the ETL pipeline. A couple examples:
    r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['created'\]\['time'\]",
    r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['lastModified'\]\['time'\]",
    r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['createStamp'\]\['time'\]",
    r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['auditStamp'\]\['time'\]",
]


class MCEConstants:
    PROPOSED_SNAPSHOT = "proposedSnapshot"
    DATASET_SNAPSHOT_CLASS = (
        "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot"
    )


class MCPConstants:
    CHANGE_TYPE = "changeType"
    ENTITY_URN = "entityUrn"
    ENTITY_TYPE = "entityType"
    ASPECT_NAME = "aspectName"
    ASPECT_VALUE = "aspect"


class EntityType:
    DATASET = "dataset"
    PIPELINE = "dataFlow"
    FLOW = "dataFlow"
    TASK = "dataJob"
    JOB = "dataJob"
    USER = "corpuser"
    GROUP = "corpGroup"


def clean_nones(value):
    """
    Recursively remove all None values from dictionaries and lists, and returns
    the result as a new dictionary or list.
    """
    if isinstance(value, list):
        return [clean_nones(x) for x in value if x is not None]
    elif isinstance(value, dict):
        return {key: clean_nones(val) for key, val in value.items() if val is not None}
    else:
        return value


def check_golden_file(
    pytestconfig: pytest.Config,
    output_path: Union[str, os.PathLike],
    golden_path: Union[str, os.PathLike],
    ignore_paths: Sequence[str] = (),
    ignore_paths_v2: Sequence[str] = (),
    ignore_order: bool = True,
) -> None:
    update_golden = pytestconfig.getoption("--update-golden-files")
    copy_output = pytestconfig.getoption("--copy-output-files")
    assert_metadata_files_equal(
        output_path=output_path,
        golden_path=golden_path,
        update_golden=update_golden,
        copy_output=copy_output,
        ignore_paths=ignore_paths,
        ignore_paths_v2=ignore_paths_v2,
        ignore_order=ignore_order,
    )


def check_goldens_stream(
    pytestconfig: pytest.Config,
    outputs: List,
    golden_path: Union[str, os.PathLike],
    ignore_paths: Sequence[str] = (),
    ignore_order: bool = True,
) -> None:
    with tempfile.NamedTemporaryFile() as f:
        write_metadata_file(pathlib.Path(f.name), outputs)

        check_golden_file(
            pytestconfig=pytestconfig,
            output_path=f.name,
            golden_path=golden_path,
            ignore_paths=ignore_paths,
            ignore_order=ignore_order,
        )


def _get_field_for_entity_type_in_mce(entity_type: str) -> str:
    """Returns the field to look for depending on the type of entity in the MCE"""
    if entity_type == EntityType.DATASET:
        return MCEConstants.DATASET_SNAPSHOT_CLASS
    raise Exception(f"Not implemented for entity_type {entity_type}")


def _get_filter(
    mce: bool = False, mcp: bool = False, entity_type: Optional[str] = None
) -> Callable[[Dict], bool]:
    if mce:
        # cheap way to determine if we are working with an MCE for the appropriate entity_type
        if entity_type:
            return (
                lambda x: MCEConstants.PROPOSED_SNAPSHOT in x
                and _get_field_for_entity_type_in_mce(str(entity_type))
                in x[MCEConstants.PROPOSED_SNAPSHOT]
            )
        else:
            return lambda x: MCEConstants.PROPOSED_SNAPSHOT in x
    if mcp:
        # cheap way to determine if we are working with an MCP
        return lambda x: MCPConstants.CHANGE_TYPE in x and (
            x[MCPConstants.ENTITY_TYPE] == entity_type if entity_type else True
        )
    return lambda _: False


def _get_element(event: Dict[str, Any], path_spec: List[str]) -> Any:
    try:
        for p in path_spec:
            if p not in event:
                return None
            else:
                event = event.get(p, {})
        return event
    except Exception as e:
        print(event)
        raise e


def _element_matches_pattern(
    event: Dict[str, Any], path_spec: List[str], pattern: str
) -> Tuple[bool, bool]:
    import re

    element = _get_element(event, path_spec)
    if element is None:
        return (False, False)
    else:
        return (True, re.search(pattern, str(element)) is not None)


def get_entity_urns(events_file: str) -> Set[str]:
    events = load_json_file(events_file)
    assert isinstance(events, list)
    return _get_entity_urns(events)


def _get_entity_urns(events_list: List[Dict]) -> Set[str]:
    entity_type = "dataset"
    # mce urns
    mce_urns = {
        _get_element(x, _get_mce_urn_path_spec(entity_type))
        for x in events_list
        if _get_filter(mce=True, entity_type=entity_type)(x)
    }
    mcp_urns = {
        _get_element(x, _get_mcp_urn_path_spec())
        for x in events_list
        if _get_filter(mcp=True, entity_type=entity_type)(x)
    }
    all_urns = mce_urns.union(mcp_urns)
    return all_urns


def assert_mcp_entity_urn(
    filter: str, entity_type: str, regex_pattern: str, file: str
) -> int:
    def get_path_spec_for_urn() -> List[str]:
        return [MCPConstants.ENTITY_URN]

    test_output = load_json_file(file)
    if isinstance(test_output, list):
        path_spec = get_path_spec_for_urn()
        filter_operator = _get_filter(mcp=True, entity_type=entity_type)
        filtered_events = [
            (x, _element_matches_pattern(x, path_spec, regex_pattern))
            for x in test_output
            if filter_operator(x)
        ]
        failed_events = [y for y in filtered_events if not y[1][0] or not y[1][1]]
        if failed_events:
            raise Exception("Failed to match events", failed_events)
        return len(filtered_events)
    else:
        raise Exception(
            f"Did not expect the file {file} to not contain a list of items"
        )


def _get_mce_urn_path_spec(entity_type: str) -> List[str]:
    if entity_type == EntityType.DATASET:
        return [
            MCEConstants.PROPOSED_SNAPSHOT,
            MCEConstants.DATASET_SNAPSHOT_CLASS,
            "urn",
        ]
    raise Exception(f"Not implemented for entity_type: {entity_type}")


def _get_mcp_urn_path_spec() -> List[str]:
    return [MCPConstants.ENTITY_URN]


def assert_mce_entity_urn(
    filter: str, entity_type: str, regex_pattern: str, file: str
) -> int:
    """Assert that all mce entity urns must match the regex pattern passed in. Return the number of events matched"""

    test_output = load_json_file(file)
    if isinstance(test_output, list):
        path_spec = _get_mce_urn_path_spec(entity_type)
        filter_operator = _get_filter(mce=True)
        filtered_events = [
            (x, _element_matches_pattern(x, path_spec, regex_pattern))
            for x in test_output
            if filter_operator(x)
        ]
        failed_events = [y for y in filtered_events if not y[1][0] or not y[1][1]]
        if failed_events:
            raise Exception(
                "Failed to match events: {json.dumps(failed_events, indent=2)}"
            )
        return len(filtered_events)
    else:
        raise Exception(
            f"Did not expect the file {file} to not contain a list of items"
        )


def assert_for_each_entity(
    entity_type: str,
    aspect_name: str,
    aspect_field_matcher: Dict[str, Any],
    file: str,
    exception_urns: List[str] = [],
) -> int:
    """Assert that an aspect name with the desired fields exists for each entity urn"""
    test_output = load_json_file(file)
    assert isinstance(test_output, list)
    # mce urns
    mce_urns = {
        _get_element(x, _get_mce_urn_path_spec(entity_type))
        for x in test_output
        if _get_filter(mce=True, entity_type=entity_type)(x)
    }
    mcp_urns = {
        _get_element(x, _get_mcp_urn_path_spec())
        for x in test_output
        if _get_filter(mcp=True, entity_type=entity_type)(x)
    }
    all_urns = mce_urns.union(mcp_urns)
    # there should not be any None urns
    assert None not in all_urns
    aspect_map = {urn: None for urn in all_urns}
    # iterate over all mcps
    for o in [
        mcp
        for mcp in test_output
        if _get_filter(mcp=True, entity_type=entity_type)(mcp)
    ]:
        if o.get(MCPConstants.ASPECT_NAME) == aspect_name:
            # load the inner aspect payload and assign to this urn
            aspect_map[o[MCPConstants.ENTITY_URN]] = o.get(
                MCPConstants.ASPECT_VALUE, {}
            ).get("json")

    success: List[str] = []
    failures: List[str] = []
    for urn, aspect_val in aspect_map.items():
        if aspect_val is not None:
            for f in aspect_field_matcher:
                assert aspect_field_matcher[f] == _get_element(aspect_val, [f]), (
                    f"urn: {urn} -> Field {f} must match value {aspect_field_matcher[f]}, found {_get_element(aspect_val, [f])}"
                )
            success.append(urn)
        elif urn not in exception_urns:
            print(f"Adding {urn} to failures")
            failures.append(urn)

    if success:
        print(f"Succeeded on assertion for urns {success}")
    if failures:
        raise AssertionError(
            f"Failed to find aspect_name {aspect_name} for urns {json.dumps(failures, indent=2)}"
        )

    return len(success)


def assert_entity_mce_aspect(
    entity_urn: str, aspect: Any, aspect_type: Type, file: str
) -> int:
    # TODO: Replace with read_metadata_file()
    test_output = load_json_file(file)
    entity_type = Urn.from_string(entity_urn).get_type()
    assert isinstance(test_output, list)
    # mce urns
    mces: List[MetadataChangeEventClass] = [
        MetadataChangeEventClass.from_obj(x)
        for x in test_output
        if _get_filter(mce=True, entity_type=entity_type)(x)
        and _get_element(x, _get_mce_urn_path_spec(entity_type)) == entity_urn
    ]
    matches = 0
    for mce in mces:
        for a in mce.proposedSnapshot.aspects:
            if isinstance(a, aspect_type):
                assert a == aspect
                matches = matches + 1
    return matches


def assert_entity_mcp_aspect(
    entity_urn: str, aspect_field_matcher: Dict[str, Any], aspect_name: str, file: str
) -> int:
    # TODO: Replace with read_metadata_file()
    test_output = load_json_file(file)
    entity_type = Urn.from_string(entity_urn).get_type()
    assert isinstance(test_output, list)
    # mcps that match entity_urn
    mcps: List[MetadataChangeProposalWrapper] = [
        MetadataChangeProposalWrapper.from_obj_require_wrapper(x)
        for x in test_output
        if _get_filter(mcp=True, entity_type=entity_type)(x)
        and _get_element(x, _get_mcp_urn_path_spec()) == entity_urn
    ]
    matches = 0
    for mcp in mcps:
        if mcp.aspectName == aspect_name:
            assert mcp.aspect
            aspect_val = mcp.aspect.to_obj()
            for f in aspect_field_matcher:
                assert aspect_field_matcher[f] == _get_element(aspect_val, [f]), (
                    f"urn: {mcp.entityUrn} -> Field {f} must match value {aspect_field_matcher[f]}, found {_get_element(aspect_val, [f])}"
                )
                matches = matches + 1
    return matches


def assert_entity_urn_not_like(entity_type: str, regex_pattern: str, file: str) -> int:
    """Assert that there are no entity urns that match the regex pattern passed in. Returns the total number of events in the file"""

    # TODO: Refactor common code with assert_entity_urn_like.
    test_output = load_json_file(file)
    assert isinstance(test_output, list)
    # mce urns
    mce_urns = {
        _get_element(x, _get_mce_urn_path_spec(entity_type))
        for x in test_output
        if _get_filter(mce=True, entity_type=entity_type)(x)
    }
    mcp_urns = {
        _get_element(x, _get_mcp_urn_path_spec())
        for x in test_output
        if _get_filter(mcp=True, entity_type=entity_type)(x)
    }
    all_urns = mce_urns.union(mcp_urns)
    print(all_urns)
    matched_urns = [u for u in all_urns if re.match(regex_pattern, u)]
    if matched_urns:
        raise AssertionError(f"urns found that match the deny list {matched_urns}")
    return len(test_output)


def assert_entity_urn_like(entity_type: str, regex_pattern: str, file: str) -> int:
    """Assert that there exist entity urns that match the regex pattern passed in. Returns the total number of events in the file"""

    test_output = load_json_file(file)
    assert isinstance(test_output, list)
    # mce urns
    mce_urns = {
        _get_element(x, _get_mce_urn_path_spec(entity_type))
        for x in test_output
        if _get_filter(mce=True, entity_type=entity_type)(x)
    }
    mcp_urns = {
        _get_element(x, _get_mcp_urn_path_spec())
        for x in test_output
        if _get_filter(mcp=True, entity_type=entity_type)(x)
    }
    all_urns = mce_urns.union(mcp_urns)
    print(all_urns)
    matched_urns = [u for u in all_urns if re.match(regex_pattern, u)]
    if matched_urns:
        return len(matched_urns)
    else:
        raise AssertionError(
            f"No urns found that match the pattern {regex_pattern}. Full list is {all_urns}"
        )

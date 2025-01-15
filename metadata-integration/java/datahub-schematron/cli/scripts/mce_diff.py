import json
from typing import Dict, Any, Optional, Tuple
import click

import json
from typing import Dict, Any


def diff_lists(list1, list2):
    """
    Compare two lists element by element and return their differences.

    Args:
        list1 (list): First list to compare
        list2 (list): Second list to compare

    Returns:
        dict: A dictionary containing the differences
    """
    result = {"added": {}, "removed": {}, "modified": set(), "modified_details": {}}

    if len(list1) != len(list2):
        # Let's first line up the elements that are common to both lists using
        # the fieldPath as the key if it exists
        if "fieldPath" in list1[0]:
            list1_dict = {field["fieldPath"]: field for field in list1}
            list2_dict = {field["fieldPath"]: field for field in list2}
            common_keys = set(list1_dict.keys()) & set(list2_dict.keys())
            list1 = [list1_dict[key] for key in common_keys]
            list2 = [list2_dict[key] for key in common_keys]
            list1.extend(
                [list1_dict[key] for key in set(list1_dict.keys()) - common_keys]
            )
            list2.extend(
                [list2_dict[key] for key in set(list2_dict.keys()) - common_keys]
            )

    # Handle added elements (if list2 is longer)
    if len(list2) > len(list1):
        for i in range(len(list1), len(list2)):
            if "fieldPath" in list2[i]:
                result["added"][list2[i]["fieldPath"]] = list2[i]
            else:
                result["added"][str(i)] = list2[i]

    # Handle removed elements (if list1 is longer)
    if len(list1) > len(list2):
        for i in range(len(list2), len(list1)):
            if "fieldPath" in list1[i]:
                result["removed"][list1[i]["fieldPath"]] = list1[i]
            else:
                result["removed"][str(i)] = list1[i]

    # Compare common indices
    for i in range(min(len(list1), len(list2))):
        value1 = list1[i]
        value2 = list2[i]

        if type(value1) != type(value2):
            result["modified"].add(str(i))
            result["modified_details"][str(i)] = {"before": value1, "after": value2}
        elif isinstance(value1, dict) and isinstance(value2, dict):
            nested_diff = diff_dicts(
                value1, value2, identifier=value1.get("fieldPath", i)
            )
            if any(nested_diff.values()):
                result["modified"].add(value1.get("fieldPath", i))
                result["modified_details"][value1.get("fieldPath", i)] = nested_diff
        elif isinstance(value1, list) and isinstance(value2, list):
            nested_diff = diff_lists(value1, value2)
            if any(nested_diff.values()):
                result["modified"].add(str(i))
                result["modified_details"][str(i)] = nested_diff
        elif value1 != value2:
            result["modified"].add(str(i))
            result["modified_details"][str(i)] = {
                "before": value1,
                "after": value2,
                "identifier": i,
            }

    return result


def diff_schema_field(field1_dict, field2_dict):

    from datahub.metadata.schema_classes import SchemaFieldClass

    field1 = SchemaFieldClass.from_obj(field1_dict)
    field2 = SchemaFieldClass.from_obj(field2_dict)

    # Initialize result structure
    result = {"added": {}, "removed": {}, "modified": set(), "modified_details": {}}

    result = {}
    if field1.fieldPath != field2.fieldPath:
        result["fieldPath"] = {"before": field1.fieldPath, "after": field2.fieldPath}

    if field1.type != field2.type:
        result["type"] = {
            "before": field1.type,
            "after": field2.type,
            "identifier": field1.fieldPath,
        }

    if field1.description != field2.description:
        result["description"] = {
            "before": field1.description,
            "after": field2.description,
            "identifier": field1.fieldPath,
        }

    if field1.nullable != field2.nullable:
        result["nullable"] = {
            "before": field1.nullable,
            "after": field2.nullable,
            "identifier": field1.fieldPath,
        }

    return result


def diff_schema_metadata(schema1_dict, schema2_dict):

    ignored_for_diff = [
        "created",
        "modified",
        "hash",
        "platformSchema",
        "lastModified",
    ]  # TODO: Reduce this list

    for key in ignored_for_diff:
        schema1_dict.pop(key, None)
        schema2_dict.pop(key, None)

    return diff_dicts(schema1_dict, schema2_dict)


def is_empty_diff(diff_dict) -> bool:
    if diff_dict.keys() == EMPTY_DIFF().keys():
        for key in diff_dict:
            if diff_dict[key]:
                return False
        return True
    return False


def format_diff(diff_dict) -> Any:
    if isinstance(diff_dict, set):
        diff_dict = sorted(list([x for x in diff_dict]))
    elif isinstance(diff_dict, dict):
        for key in diff_dict:
            diff_dict[key] = format_diff(diff_dict[key])
    return diff_dict


def EMPTY_DIFF():
    return {
        "added": {},
        "removed": {},
        "modified": set(),
        "modified_details": {},
    }


def diff_dicts(dict1, dict2, identifier=None):
    """
    Compare two dictionaries recursively and return their differences.

    Args:
        dict1 (dict): First dictionary to compare
        dict2 (dict): Second dictionary to compare

    Returns:
        dict: A dictionary containing the differences with the following structure:
            {
                'added': Keys present in dict2 but not in dict1,
                'removed': Keys present in dict1 but not in dict2,
                'modified': Keys present in both but with different values,
                'modified_details': Detailed before/after values for modified keys
            }
    """
    if "nullable" in dict1:
        # Assume this is a SchemaFieldClass
        return diff_schema_field(dict1, dict2)

    if "hash" in dict1:
        # Assume this is a schema metadata class
        return diff_schema_metadata(dict1, dict2)

    dict1_keys = set(dict1.keys())
    dict2_keys = set(dict2.keys())

    # Find keys that were added, removed, or modified
    added_keys = dict2_keys - dict1_keys
    removed_keys = dict1_keys - dict2_keys
    common_keys = dict1_keys & dict2_keys

    # Initialize result structure
    result = EMPTY_DIFF()
    # Handle added keys
    for key in added_keys:
        result["added"][key] = dict2[key]

    # Handle removed keys
    for key in removed_keys:
        result["removed"][key] = dict1[key]

    # Check common keys for modifications
    for key in common_keys:
        value1 = dict1[key]
        value2 = dict2[key]

        # If both values are dictionaries, recurse
        if isinstance(value1, dict) and isinstance(value2, dict):
            nested_diff = diff_dicts(
                value1, value2, identifier=value1.get("fieldPath", key)
            )
            if any(nested_diff.values()):  # If there are any differences
                result["modified"].add(key)
                result["modified_details"][key] = nested_diff
        # If both values are lists, compare them element by element
        elif isinstance(value1, list) and isinstance(value2, list):
            nested_diff = diff_lists(value1, value2)
            if any(nested_diff.values()):
                result["modified"].add(key)
                result["modified_details"][key] = nested_diff
        # Otherwise compare directly
        elif value1 != value2:
            result["modified"].add(key)
            result["modified_details"][key] = {
                "before": value1,
                "after": value2,
                "identifier": identifier,
            }

    return result


def process_single_element(element) -> Tuple[str, str, Dict[str, Any]]:
    if "entityUrn" in element:
        entity = element["entityUrn"]
    else:
        raise Exception("Element does not have an entityUrn key")
    if "aspectName" in element:
        aspect = element["aspectName"]
    else:
        raise Exception("Element does not have an aspectName key")
    if "aspect" in element:
        if "json" in element["aspect"]:
            return entity, aspect, element["aspect"]["json"]
        elif "value" in element["aspect"]:
            json_value = json.loads(element["aspect"]["value"])
            return entity, aspect, json_value
        else:
            raise Exception("Element does not have a json or value key")
    else:
        raise Exception("Element does not have an aspect key")


def process_element_with_dict(element, global_dict):
    entity, aspect, data = process_single_element(element)
    if entity not in global_dict:
        global_dict[entity] = {}
    if aspect not in global_dict[entity]:
        global_dict[entity][aspect] = data
    else:
        # breakpoint()
        raise Exception("Duplicate aspect found")


@click.command("compute_diff")
@click.argument("input_file_1", type=click.Path(exists=True))
@click.argument("input_file_2", type=click.Path(exists=True))
@click.option("--golden-diff-file", type=click.Path(), default=None)
@click.option("--update-golden-diff", is_flag=True)
def compute_diff(
    input_file_1: str,
    input_file_2: str,
    golden_diff_file: Optional[str] = None,
    update_golden_diff: bool = False,
):

    # Read the files into json objects and compare them
    # If they are the same, exit 0
    # If they are different, exit 1
    file_1_mcps = {}
    with open(input_file_1) as file1:
        data1 = json.load(file1)
        assert isinstance(data1, list)
        for element in data1:
            process_element_with_dict(element, file_1_mcps)
    print(f"Processed {len(file_1_mcps)} elements from file {input_file_1}")

    file_2_mcps = {}
    with open(input_file_2) as file2:
        data2 = json.load(file2)
        assert isinstance(data2, list)
        for element in data2:
            process_element_with_dict(element, file_2_mcps)

    print(f"Processed {len(file_2_mcps)} elements from file {input_file_2}")

    if golden_diff_file and not update_golden_diff:
        with open(golden_diff_file) as golden_diff:
            golden_diff_data = json.load(golden_diff)
    else:
        golden_diff_data = None

    computed_diff_data = {}

    assert len(file_1_mcps) == len(file_2_mcps)
    for entity in file_1_mcps:
        assert entity in file_2_mcps
        assert len(file_1_mcps[entity]) == len(file_2_mcps[entity])
        for aspect in file_1_mcps[entity]:
            assert aspect in file_2_mcps[entity]
            aspect_diff = diff_dicts(
                file_1_mcps[entity][aspect], file_2_mcps[entity][aspect]
            )
            if golden_diff_data:
                assert aspect in golden_diff_data[entity]
                assert format_diff(aspect_diff) == golden_diff_data[entity][aspect], (
                    f"Computed difference is {json.dumps(format_diff(aspect_diff), indent=2)}\n"
                    f"Expected difference is {json.dumps(golden_diff_data[entity][aspect], indent=2)}"
                )

            else:
                if update_golden_diff:
                    if entity not in computed_diff_data:
                        computed_diff_data[entity] = {}
                    computed_diff_data[entity][aspect] = format_diff(aspect_diff)
                else:
                    assert is_empty_diff(
                        aspect_diff
                    ), f"Difference is {json.dumps(format_diff(aspect_diff), indent=2)}"

    if update_golden_diff:
        with open(golden_diff_file, "w") as golden_diff:
            json.dump(computed_diff_data, golden_diff, indent=2, sort_keys=True)


if __name__ == "__main__":
    compute_diff()

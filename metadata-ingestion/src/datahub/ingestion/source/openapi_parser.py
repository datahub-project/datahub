import json
import logging
import re
from typing import Any, Dict, Generator, List, Optional, Tuple

import requests
import yaml
from requests.auth import HTTPBasicAuth

from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    OtherSchemaClass,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    RecordTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
)

logger = logging.getLogger(__name__)


def flatten(d: dict, prefix: str = "") -> Generator:
    for k, v in d.items():
        if isinstance(v, dict):
            # First yield the parent field
            yield f"{prefix}.{k}".strip(".")
            # Then yield all nested fields
            yield from flatten(v, f"{prefix}.{k}")
        elif isinstance(v, list) and len(v) > 0:
            # Handle arrays by taking the first element as a sample
            # First yield the parent field (array itself)
            yield f"{prefix}.{k}".strip(".")
            # Then yield fields from the first element if it's a dict
            if isinstance(v[0], dict):
                yield from flatten(v[0], f"{prefix}.{k}")
        else:
            yield f"{prefix}.{k}".strip(".")  # Use dot instead of hyphen


def flatten2list(d: dict) -> list:
    """
    This function explodes dictionary keys such as:
        d = {"first":
            {"second_a": 3, "second_b": 4},
         "another": 2,
         "anotherone": {"third_a": {"last": 3}}
         }

    yields:

        ["first.second_a",
         "first.second_b",
         "another",
         "anotherone.third_a.last"
         ]
    """
    fl_l = list(flatten(d))
    return fl_l


def request_call(
    url: str,
    token: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    proxies: Optional[dict] = None,
    verify_ssl: bool = True,
) -> requests.Response:
    headers = {"accept": "application/json"}
    if username is not None and password is not None:
        return requests.get(
            url,
            headers=headers,
            auth=HTTPBasicAuth(username, password),
            verify=verify_ssl,
        )
    elif token is not None:
        headers["Authorization"] = f"{token}"
        return requests.get(url, proxies=proxies, headers=headers, verify=verify_ssl)
    else:
        return requests.get(url, headers=headers, verify=verify_ssl)


def get_swag_json(
    url: str,
    token: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    swagger_file: str = "",
    proxies: Optional[dict] = None,
    verify_ssl: bool = True,
) -> Dict:
    tot_url = url + swagger_file
    response = request_call(
        url=tot_url,
        token=token,
        username=username,
        password=password,
        proxies=proxies,
        verify_ssl=verify_ssl,
    )

    if response.status_code != 200:
        raise Exception(f"Unable to retrieve {tot_url}, error {response.status_code}")
    try:
        dict_data = json.loads(response.content)
    except json.JSONDecodeError:  # it's not a JSON!
        dict_data = yaml.safe_load(response.content)
    return dict_data


def get_url_basepath(sw_dict: dict) -> str:
    if "basePath" in sw_dict:
        return sw_dict["basePath"]
    if "servers" in sw_dict:
        # When the API path doesn't match the OAS path
        return sw_dict["servers"][0]["url"]

    return ""


def check_sw_version(sw_dict: dict) -> None:
    if "swagger" in sw_dict:
        v_split = sw_dict["swagger"].split(".")
    else:
        v_split = sw_dict["openapi"].split(".")

    version = [int(v) for v in v_split]

    if version[0] == 3 and version[1] > 0:
        logger.warning(
            "This plugin has not been fully tested with Swagger version >3.0"
        )


def get_endpoints(sw_dict: dict) -> dict:
    """
    Get all the URLs, together with their description and the tags
    """
    url_details = {}

    check_sw_version(sw_dict)

    for p_k, p_o in sw_dict["paths"].items():
        for method, method_spec in p_o.items():
            # skip non-method keys like "parameters"
            if method.lower() not in [
                "get",
                "post",
                "put",
                "delete",
                "patch",
                "options",
                "head",
            ]:
                continue

            responses = method_spec.get("responses", {})
            base_res = responses.get("200") or responses.get(200)
            if not base_res:
                # if there is no 200 response, we skip this method
                continue

            # if the description is not present, we will use the summary
            # if both are not present, we will use an empty string
            desc = method_spec.get("description") or method_spec.get("summary", "")

            # if the tags are not present, we will use an empty list
            tags = method_spec.get("tags", [])

            url_details[p_k] = {
                "description": desc,
                "tags": tags,
                "method": method.upper(),
            }

            example_data = check_for_api_example_data(base_res, p_k)
            if example_data:
                url_details[p_k]["data"] = example_data

            # checking whether there are defined parameters to execute the call...
            if "parameters" in p_o[method]:
                url_details[p_k]["parameters"] = p_o[method]["parameters"]

    return dict(sorted(url_details.items()))


def check_for_api_example_data(base_res: dict, key: str) -> dict:
    """
    Try to determine if example data is defined for the endpoint, and return it
    """
    data = {}
    if "content" in base_res:
        res_cont = base_res["content"]
        if "application/json" in res_cont:
            json_content = res_cont["application/json"]

            # Check for single example (OpenAPI v3)
            if "example" in json_content:
                data = json_content["example"]
            # Check for multiple examples (OpenAPI v3)
            elif "examples" in json_content:
                examples = json_content["examples"]
                # Take the first example if it's a dict of examples
                if isinstance(examples, dict) and examples:
                    first_example_name = next(iter(examples))
                    example_value = examples[first_example_name].get("value", {})
                    # Preserve the example name as a wrapper to maintain structure
                    data = {first_example_name: example_value}
                # Handle list format (OpenAPI v2 style)
                elif isinstance(examples, list) and examples:
                    data = examples[0]
            else:
                # Only warn if we're in debug mode or if this is a critical endpoint
                # Most OpenAPI v3 specs don't include examples, so this is normal
                logger.debug(
                    f"No example data found for endpoint --- {key} (this is normal for OpenAPI v3)"
                )
        elif "text/csv" in res_cont:
            data = res_cont["text/csv"]["schema"]
    # Handle OpenAPI v2 format
    elif "examples" in base_res:
        data = base_res["examples"]["application/json"]

    return data


def guessing_url_name(url: str, examples: dict) -> str:
    """
    given a url and dict of extracted data, we try to guess a working URL. Example:
    url2complete = "/advancedcomputersearches/name/{name}/id/{id}"
    extr_data = {"advancedcomputersearches": {'id': 202, 'name': '_unmanaged'}}
    -->> guessed_url = /advancedcomputersearches/name/_unmanaged/id/202'
    """
    url2op = url[1:] if url[0] == "/" else url
    divisions = url2op.split("/")

    # the very first part of the url should stay the same.
    root = url2op.split("{")[0]

    needed_n = [
        a for a in divisions if not a.find("{")
    ]  # search for stuff like "{example}"
    cleaned_needed_n = [
        name[1:-1] for name in needed_n
    ]  # no parenthesis, {example} -> example

    # in the cases when the parameter name is specified, we have to correct the root.
    # in the example, advancedcomputersearches/name/ -> advancedcomputersearches/
    for field in cleaned_needed_n:
        if field in root:
            div_pos = root.find(field)
            if div_pos > 0:
                root = root[: div_pos - 1]  # like "base/field" should become "base"

    if root in examples:
        # if our root is contained in our samples examples...
        ex2use = root
    elif root[:-1] in examples:
        ex2use = root[:-1]
    elif root.replace("/", ".") in examples:
        ex2use = root.replace("/", ".")
    elif root[:-1].replace("/", ".") in examples:
        ex2use = root[:-1].replace("/", ".")
    else:
        return url

    # we got our example! Let's search for the needed parameters...
    guessed_url = url  # just a copy of the original url

    # substituting the parameter's name w the value
    for name, clean_name in zip(needed_n, cleaned_needed_n):
        if clean_name in examples[ex2use]:
            guessed_url = re.sub(name, str(examples[ex2use][clean_name]), guessed_url)

    return guessed_url


def compose_url_attr(raw_url: str, attr_list: list) -> str:
    """
    This function will compose URLs based on attr_list.
    Examples:
    asd = compose_url_attr(raw_url="http://asd.com/{id}/boh/{name}",
                           attr_list=["2", "my"])
    asd == "http://asd.com/2/boh/my"

    asd2 = compose_url_attr(raw_url="http://asd.com/{id}",
                           attr_list=["2",])
    asd2 == "http://asd.com/2"
    """
    splitted = re.split(r"\{[^}]+}", raw_url)
    if splitted[-1] == "":  # it can happen that the last element is empty
        splitted = splitted[:-1]
    composed_url = ""

    for i_s, split in enumerate(splitted):
        try:
            composed_url += split + attr_list[i_s]
        except IndexError:  # we already ended to fill the url
            composed_url += split
    return composed_url


def maybe_theres_simple_id(url: str) -> str:
    dets = re.findall(r"(\{[^}]+})", url)  # searching the fields between parenthesis
    if len(dets) == 0:
        return url
    dets_w_id = [det for det in dets if "id" in det]  # the fields containing "id"
    if len(dets) == len(dets_w_id):
        # if we only have fields containing IDs, we guess to use "1"s
        return compose_url_attr(url, ["1" for _ in dets_w_id])
    else:
        return url


def try_guessing(url: str, examples: dict) -> str:
    """
    We will guess the content of the url string...
    Any non-guessed name will stay as it was (with parenthesis{})
    """
    url_guess = guessing_url_name(url, examples)  # try to fill with known informations
    return maybe_theres_simple_id(url_guess)


def clean_url(url: str) -> str:
    protocols = ["http://", "https://"]
    for prot in protocols:
        if prot in url:
            parts = url.split(prot)
            return prot + parts[1].replace("//", "/")
    raise Exception(f"Unable to understand URL {url}")


def extract_fields(
    response: requests.Response, dataset_name: str
) -> Tuple[List[Any], Dict[Any, Any]]:
    """
    Given a URL, this function will extract the fields contained in the
    response of the call to that URL, supposing that the response is a JSON.

    The list in the output tuple will contain the fields name.
    The dict in the output tuple will contain a sample of data.
    """
    dict_data = json.loads(response.content)
    if isinstance(dict_data, str):
        # no sense
        logger.warning(f"Empty data --- {dataset_name}")
        return [], {}
    elif isinstance(dict_data, list):
        # it's maybe just a list
        if len(dict_data) == 0:
            logger.warning(f"Empty data --- {dataset_name}")
            return [], {}
        # so we take the fields of the first element,
        # if it's a dict
        if isinstance(dict_data[0], dict):
            return flatten2list(dict_data[0]), dict_data[0]
        elif isinstance(dict_data[0], str):
            # this is actually data
            return ["contains_a_string"], {"contains_a_string": dict_data[0]}
        else:
            raise ValueError("unknown format")
    elif not dict_data:  # Handle empty dict case
        return [], {}
    if len(dict_data) > 1:
        # the elements are directly inside the dict
        return flatten2list(dict_data), dict_data
    dst_key = list(dict_data)[0]  # the first and unique key is the dataset's name

    try:
        return flatten2list(dict_data[dst_key]), dict_data[dst_key]
    except AttributeError:
        # if the content is a list, we should treat each element as a dataset.
        # ..but will take the keys of the first element (to be improved)
        if isinstance(dict_data[dst_key], list):
            if len(dict_data[dst_key]) > 0:
                return flatten2list(dict_data[dst_key][0]), dict_data[dst_key][0]
            else:
                return [], {}  # it's empty!
        else:
            logger.warning(f"Unable to get the attributes --- {dataset_name}")
            return [], {}


def get_tok(
    url: str,
    username: str = "",
    password: str = "",
    tok_url: str = "",
    method: str = "post",
    proxies: Optional[dict] = None,
    verify_ssl: bool = True,
) -> str:
    """
    Trying to post username/password to get auth.
    """
    token = ""
    url4req = url + tok_url
    if method == "post":
        # this will make a POST call with username and password
        data = {"username": username, "password": password, "maxDuration": True}
        # url2post = url + "api/authenticate/"
        response = requests.post(url4req, proxies=proxies, json=data, verify=verify_ssl)
        if response.status_code == 200:
            cont = json.loads(response.content)
            if "token" in cont:  # other authentication scheme
                token = cont["token"]
            else:  # works only for bearer authentication scheme
                token = f"Bearer {cont['tokens']['access']}"
    elif method == "get":
        # this will make a GET call with username and password
        response = requests.get(url4req, verify=verify_ssl)
        if response.status_code == 200:
            cont = json.loads(response.content)
            token = cont["token"]
    else:
        raise ValueError(f"Method unrecognised: {method}")
    if token != "":
        return token
    else:
        raise Exception(f"Unable to get a valid token: {response.text}")


def set_metadata(
    dataset_name: str,
    fields: List,
    platform: str = "api",
    original_data: Optional[Dict] = None,
) -> SchemaMetadata:
    canonical_schema: List[SchemaField] = []
    seen_paths = set()

    # First pass: identify which paths are structs (have children) vs leaf fields vs arrays
    struct_paths = set()
    leaf_paths = set()
    array_paths = set()

    for field_path in fields:
        parts = field_path.split(".")

        # Check if this path has children (other paths that start with this path + ".")
        has_children = any(
            other_path.startswith(field_path + ".") for other_path in fields
        )

        # Check if this field is an array in the original data
        is_array = False
        if original_data:
            # Navigate to the field in the original data to check if it's an array
            current_data = original_data
            for part in parts:
                if isinstance(current_data, dict) and part in current_data:
                    current_data = current_data[part]
                else:
                    break
            is_array = isinstance(current_data, list)

        if has_children:
            if is_array:
                array_paths.add(field_path)
            else:
                struct_paths.add(field_path)
        else:
            leaf_paths.add(field_path)

    # Second pass: create schema fields
    for field_path in fields:
        parts = field_path.split(".")

        # Add struct/object fields for each ancestor path
        current_path: List[str] = []
        for part in parts[:-1]:
            ancestor_path = ".".join(current_path + [part])
            if ancestor_path not in seen_paths:
                struct_field = SchemaField(
                    fieldPath=ancestor_path,
                    nativeDataType="object",  # OpenAPI term for struct/record
                    type=SchemaFieldDataTypeClass(type=RecordTypeClass()),
                    description="",
                    recursive=False,
                )
                canonical_schema.append(struct_field)
                seen_paths.add(ancestor_path)
            current_path.append(part)

        # Add the field if not already seen
        if field_path not in seen_paths:
            if field_path in array_paths:
                # This is an array field
                from datahub.metadata.schema_classes import ArrayTypeClass

                array_field = SchemaField(
                    fieldPath=field_path,
                    nativeDataType="array",  # Array type
                    type=SchemaFieldDataTypeClass(type=ArrayTypeClass()),
                    description="",
                    recursive=False,
                )
                canonical_schema.append(array_field)
            elif field_path in struct_paths:
                # This is a struct field (has children)
                struct_field = SchemaField(
                    fieldPath=field_path,
                    nativeDataType="object",  # OpenAPI term for struct/record
                    type=SchemaFieldDataTypeClass(type=RecordTypeClass()),
                    description="",
                    recursive=False,
                )
                canonical_schema.append(struct_field)
            else:
                # This is a leaf field (no children)
                leaf_field = SchemaField(
                    fieldPath=field_path,
                    nativeDataType="str",  # Keeping `str` for backwards compatability, ideally this is the correct type
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    description="",
                    recursive=False,
                )
                canonical_schema.append(leaf_field)
            seen_paths.add(field_path)

    schema_metadata = SchemaMetadata(
        schemaName=dataset_name,
        platform=f"urn:li:dataPlatform:{platform}",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=canonical_schema,
    )
    return schema_metadata


def enhance_schema_with_titles(
    schema: Dict, sw_dict: Dict, schema_name: str = ""
) -> Dict:
    """
    Enhance schemas with title fields so that json_schema_util.py uses schema names instead of 'object'.
    This is done without modifying json_schema_util.py itself.
    """
    if not isinstance(schema, dict):
        return schema

    enhanced_schema = schema.copy()

    # Add title if it doesn't exist and we have a schema name
    if "title" not in enhanced_schema and schema_name:
        enhanced_schema["title"] = schema_name

    # Recursively enhance nested schemas
    if "properties" in enhanced_schema:
        for prop_name, prop_schema in enhanced_schema["properties"].items():
            # Use property name as schema name for nested objects
            enhanced_schema["properties"][prop_name] = enhance_schema_with_titles(
                prop_schema, sw_dict, prop_name
            )

    # Enhance array items
    if "items" in enhanced_schema:
        enhanced_schema["items"] = enhance_schema_with_titles(
            enhanced_schema["items"], sw_dict, "item"
        )

    # Enhance additionalProperties
    if "additionalProperties" in enhanced_schema and isinstance(
        enhanced_schema["additionalProperties"], dict
    ):
        enhanced_schema["additionalProperties"] = enhance_schema_with_titles(
            enhanced_schema["additionalProperties"], sw_dict, "value"
        )

    # Handle union types
    for union_key in ["oneOf", "anyOf", "allOf"]:
        if union_key in enhanced_schema:
            enhanced_schema[union_key] = [
                enhance_schema_with_titles(union_schema, sw_dict, f"{union_key}_{i}")
                for i, union_schema in enumerate(enhanced_schema[union_key])
            ]

    return enhanced_schema


def resolve_schema_references(schema: Dict, sw_dict: Dict) -> Dict:
    """
    Recursively resolve all schema references in a Swagger v2 or OpenAPI v3 spec.
    This ensures that all $ref references are resolved before passing to json_schema_util.py.
    """
    if not isinstance(schema, dict):
        return schema

    resolved_schema = schema.copy()

    # Handle direct references
    if "$ref" in resolved_schema:
        ref_path = resolved_schema["$ref"]

        # Handle v2 references (e.g., "#/definitions/Pet")
        if ref_path.startswith("#/definitions/"):
            schema_name = ref_path.split("/")[-1]
            referenced_schema = sw_dict.get("definitions", {}).get(schema_name, {})
            if referenced_schema:
                # Recursively resolve references in the referenced schema
                resolved_referenced = resolve_schema_references(
                    referenced_schema, sw_dict
                )
                # Enhance with title using the schema name
                return enhance_schema_with_titles(
                    resolved_referenced, sw_dict, schema_name
                )

        # Handle v3 references (e.g., "#/components/schemas/Pet")
        elif ref_path.startswith("#/components/schemas/"):
            schema_name = ref_path.split("/")[-1]
            referenced_schema = (
                sw_dict.get("components", {}).get("schemas", {}).get(schema_name, {})
            )
            if referenced_schema:
                # Recursively resolve references in the referenced schema
                resolved_referenced = resolve_schema_references(
                    referenced_schema, sw_dict
                )
                # Enhance with title using the schema name
                return enhance_schema_with_titles(
                    resolved_referenced, sw_dict, schema_name
                )

    # Recursively resolve references in properties
    if "properties" in resolved_schema:
        for prop_name, prop_schema in resolved_schema["properties"].items():
            resolved_schema["properties"][prop_name] = resolve_schema_references(
                prop_schema, sw_dict
            )

    # Recursively resolve references in array items
    if "items" in resolved_schema:
        resolved_schema["items"] = resolve_schema_references(
            resolved_schema["items"], sw_dict
        )

    # Recursively resolve references in additionalProperties
    if "additionalProperties" in resolved_schema and isinstance(
        resolved_schema["additionalProperties"], dict
    ):
        resolved_schema["additionalProperties"] = resolve_schema_references(
            resolved_schema["additionalProperties"], sw_dict
        )

    # Handle union types (oneOf, anyOf, allOf)
    for union_key in ["oneOf", "anyOf", "allOf"]:
        if union_key in resolved_schema:
            resolved_schema[union_key] = [
                resolve_schema_references(union_schema, sw_dict)
                for union_schema in resolved_schema[union_key]
            ]

    return resolved_schema


def extract_schema_from_response_schema(
    response_schema: Dict, sw_dict: Dict, schema_name: str = ""
) -> Dict:
    """
    Extract schema definition from response schema, handling both v2 and v3 references.
    """
    if "$ref" in response_schema:
        ref_path = response_schema["$ref"]

        # Handle v2 references (e.g., "#/definitions/Pet")
        if ref_path.startswith("#/definitions/"):
            schema_name = ref_path.split("/")[-1]
            return sw_dict.get("definitions", {}).get(schema_name, {})

        # Handle v3 references (e.g., "#/components/schemas/Pet")
        elif ref_path.startswith("#/components/schemas/"):
            schema_name = ref_path.split("/")[-1]
            return sw_dict.get("components", {}).get("schemas", {}).get(schema_name, {})

    return response_schema


def get_schema_from_response(response_schema: Dict, sw_dict: Dict) -> Optional[Dict]:
    """
    Extract the actual schema definition from a response schema.
    Handles both direct schemas and references.
    """
    if not response_schema:
        return None

    # Handle array responses
    if response_schema.get("type") == "array":
        items_schema = response_schema.get("items", {})
        resolved_items_schema = extract_schema_from_response_schema(
            items_schema, sw_dict
        )
        # Resolve all references in the schema
        return resolve_schema_references(resolved_items_schema, sw_dict)

    # Handle direct object schemas
    elif response_schema.get("type") == "object":
        return resolve_schema_references(response_schema, sw_dict)

    # Handle references
    elif "$ref" in response_schema:
        resolved_schema = extract_schema_from_response_schema(response_schema, sw_dict)
        # Resolve all references in the schema
        return resolve_schema_references(resolved_schema, sw_dict)

    return None

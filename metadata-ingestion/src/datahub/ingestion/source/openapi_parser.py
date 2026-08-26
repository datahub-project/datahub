import json
import logging
import re
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    TypeGuard,
    Union,
)

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

# HTTP methods that can provide useful schemas for extraction
# Priority order matches schema extraction precedence in APISource.extract_schema_from_all_methods
SCHEMA_EXTRACTABLE_METHODS = ["get", "post", "put", "patch"]

# HTTP methods that typically don't provide useful schemas
OTHER_HTTP_METHODS = ["delete", "options", "head"]

# Default timeout for outbound HTTP calls (seconds).
_REQUEST_TIMEOUT_SECONDS = 30


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
    timeout = _REQUEST_TIMEOUT_SECONDS
    if username is not None and password is not None:
        return requests.get(
            url,
            headers=headers,
            auth=HTTPBasicAuth(username, password),
            verify=verify_ssl,
            timeout=timeout,
        )
    elif token is not None:
        headers["Authorization"] = f"{token}"
        return requests.get(
            url,
            proxies=proxies,
            headers=headers,
            verify=verify_ssl,
            timeout=timeout,
        )
    else:
        return requests.get(url, headers=headers, verify=verify_ssl, timeout=timeout)


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
        return json.loads(response.content)
    except json.JSONDecodeError:
        try:
            return yaml.safe_load(response.content)
        except yaml.YAMLError as e:
            raise ValueError(
                f"Unable to parse OpenAPI spec from {tot_url} as JSON or YAML"
            ) from e


def get_url_basepath(sw_dict: dict) -> str:
    if "basePath" in sw_dict:
        return sw_dict["basePath"]
    if sw_dict.get("servers"):
        # When the API path doesn't match the OAS path.
        # Some specs declare "servers": [] or entries without a "url" key.
        return sw_dict["servers"][0].get("url", "")

    return ""


def check_sw_version(sw_dict: dict) -> None:
    version_str = sw_dict.get("swagger") or sw_dict.get("openapi")
    if not isinstance(version_str, str):
        logger.warning(
            "OpenAPI/Swagger spec has no swagger or openapi version key; "
            "skipping version check"
        )
        return
    # Strip prerelease / build suffixes (e.g. 3.0.1-rc1, 3.1.0+SNAPSHOT) before
    # comparing major.minor — this is only an informational "not fully tested" notice.
    core = re.split(r"[-+]", version_str, maxsplit=1)[0]
    parts = core.split(".")
    try:
        major = int(parts[0])
        minor = int(parts[1]) if len(parts) > 1 else 0
    except (ValueError, IndexError):
        logger.warning(
            "Unable to parse OpenAPI/Swagger version %r; skipping version check",
            version_str,
        )
        return

    if major == 3 and minor > 0:
        logger.warning(
            "This plugin has not been fully tested with Swagger version >3.0"
        )


def get_endpoints(sw_dict: dict) -> dict:
    """
    Get all the URLs, together with their description and the tags
    """
    url_details: Dict[str, Dict[str, Any]] = {}

    check_sw_version(sw_dict)

    # Process methods in priority order to align with schema extraction
    # Higher priority methods are processed first and their metadata is preserved
    # Priority order matches schema extraction precedence in APISource.extract_schema_from_all_methods
    all_methods = SCHEMA_EXTRACTABLE_METHODS + OTHER_HTTP_METHODS

    for p_k, p_o in sw_dict["paths"].items():
        # Only set metadata if it doesn't already exist (don't overwrite higher-priority metadata)

        for method in all_methods:
            method_spec = p_o.get(method)
            if not method_spec:
                continue

            responses = method_spec.get("responses", {})
            base_res = responses.get("200") or responses.get(200)
            if not base_res:
                # if there is no 200 response, we skip this method
                continue

            # Initialize endpoint details if it doesn't exist
            if p_k not in url_details:
                url_details[p_k] = {}

            # Only set metadata if it doesn't already exist (preserve higher-priority metadata)
            if "method" not in url_details[p_k]:
                url_details[p_k]["method"] = method.lower()

            if (
                "description" not in url_details[p_k]
                or not url_details[p_k]["description"]
            ):
                # if the description is not present, we will use the summary
                # if both are not present, we will use an empty string
                desc = method_spec.get("description") or method_spec.get("summary", "")
                url_details[p_k]["description"] = desc

            if "tags" not in url_details[p_k] or not url_details[p_k]["tags"]:
                # if the tags are not present, we will use an empty list
                tags = method_spec.get("tags", [])
                url_details[p_k]["tags"] = tags

            # Example data can be added from any method (accumulate if needed)
            example_data = check_for_api_example_data(base_res, p_k)
            if example_data and "data" not in url_details[p_k]:
                url_details[p_k]["data"] = example_data

            # checking whether there are defined parameters to execute the call...
            if "parameters" in p_o[method] and "parameters" not in url_details[p_k]:
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
    for name, clean_name in zip(needed_n, cleaned_needed_n, strict=False):
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
    try:
        dict_data = json.loads(response.content)
    except json.JSONDecodeError:
        logger.warning(f"Non-JSON response --- {dataset_name}")
        return [], {}
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
    timeout = _REQUEST_TIMEOUT_SECONDS
    if method == "post":
        # this will make a POST call with username and password
        data = {"username": username, "password": password, "maxDuration": True}
        response = requests.post(
            url4req,
            proxies=proxies,
            json=data,
            verify=verify_ssl,
            timeout=timeout,
        )
        if response.status_code == 200:
            try:
                cont = json.loads(response.content)
                if "token" in cont:  # other authentication scheme
                    token = cont["token"]
                else:  # works only for bearer authentication scheme
                    token = f"Bearer {cont['tokens']['access']}"
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                raise ValueError(
                    f"Unexpected token response shape from {url4req}"
                ) from e
    elif method == "get":
        # this will make a GET call with username and password
        response = requests.get(url4req, verify=verify_ssl, timeout=timeout)
        if response.status_code == 200:
            try:
                cont = json.loads(response.content)
                token = cont["token"]
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                raise ValueError(
                    f"Unexpected token response shape from {url4req}"
                ) from e
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


def merge_allof_schemas(
    schema: Dict, sw_dict: Dict, resolving_refs: bool = False, max_depth: int = 10
) -> Dict:
    """
    Merge allOf schemas into a single schema object.

    According to JSON Schema, allOf means all schemas must be valid, which means
    we should merge their properties, required fields, and other attributes.

    Args:
        schema: Schema dictionary that may contain allOf
        sw_dict: Complete OpenAPI specification for resolving references
        resolving_refs: Flag to prevent infinite recursion when called from resolve_schema_references
        max_depth: Maximum recursion depth for resolving schema references (default: 10)

    Returns:
        Merged schema with all allOf entries combined
    """
    if not isinstance(schema, dict):
        return schema

    # If no allOf, return as-is
    if "allOf" not in schema:
        return schema

    allof_schemas = schema.get("allOf", [])
    if not allof_schemas:
        return schema

    # Start with a base schema (copy everything except allOf)
    merged_schema = {k: v for k, v in schema.items() if k != "allOf"}

    # Merge each schema in allOf
    for allof_schema in allof_schemas:
        # Resolve any references in the allOf entry first
        # Use resolving_refs flag to prevent infinite recursion
        if resolving_refs:
            # If we're already resolving refs, just resolve $ref directly
            resolved_allof = _resolve_ref_directly(allof_schema, sw_dict)
        else:
            resolved_allof = resolve_schema_references(
                allof_schema, sw_dict, max_depth=max_depth
            )

        # Merge properties
        if "properties" in resolved_allof:
            if "properties" not in merged_schema:
                merged_schema["properties"] = {}
            merged_schema["properties"].update(resolved_allof["properties"])

        # Merge required fields (union of all required lists)
        if "required" in resolved_allof:
            if "required" not in merged_schema:
                merged_schema["required"] = []
            # Combine required lists and remove duplicates while preserving order
            existing_required = merged_schema.get("required", [])
            new_required = resolved_allof.get("required", [])
            merged_schema["required"] = list(
                dict.fromkeys(existing_required + new_required)
            )

        # Merge other schema attributes (type, format, description, etc.)
        # Only merge if not already present in merged_schema.
        for key in [
            "type",
            "format",
            "description",
            "title",
            "enum",
            "default",
            "example",
        ]:
            if key in resolved_allof and key not in merged_schema:
                merged_schema[key] = resolved_allof[key]

        # Merge JSON-Schema validation keywords with allOf semantics (most
        # restrictive wins) so repeated contributors don't drop constraints.
        _merge_allof_validation_keywords(merged_schema, resolved_allof)

        # Merge items (for arrays)
        if "items" in resolved_allof:
            if "items" not in merged_schema:
                merged_schema["items"] = resolved_allof["items"]
            else:
                # Recursively merge nested items
                merged_schema["items"] = merge_allof_schemas(
                    {"allOf": [merged_schema["items"], resolved_allof["items"]]},
                    sw_dict,
                    resolving_refs=True,
                    max_depth=max_depth,
                )

        _merge_allof_map_keywords(merged_schema, resolved_allof, sw_dict, max_depth)

    # Recursively handle any nested allOf in the merged result
    # But don't call resolve_schema_references again to avoid recursion
    if "allOf" in merged_schema:
        merged_schema = merge_allof_schemas(
            merged_schema, sw_dict, resolving_refs=True, max_depth=max_depth
        )

    return merged_schema


def _merge_allof_map_keywords(
    merged_schema: Dict[str, Any],
    resolved_allof: Dict[str, Any],
    sw_dict: Dict[str, Any],
    max_depth: int,
) -> None:
    if "additionalProperties" in resolved_allof:
        if "additionalProperties" not in merged_schema:
            merged_schema["additionalProperties"] = resolved_allof[
                "additionalProperties"
            ]
        elif isinstance(merged_schema["additionalProperties"], dict) and isinstance(
            resolved_allof["additionalProperties"], dict
        ):
            merged_schema["additionalProperties"] = merge_allof_schemas(
                {
                    "allOf": [
                        merged_schema["additionalProperties"],
                        resolved_allof["additionalProperties"],
                    ]
                },
                sw_dict,
                resolving_refs=True,
                max_depth=max_depth,
            )

    pattern_props = resolved_allof.get("patternProperties")
    if isinstance(pattern_props, dict):
        # Copy before mutate — merged_schema may still alias the caller's nested dict.
        existing_pp = merged_schema.get("patternProperties")
        merged_schema["patternProperties"] = (
            dict(existing_pp) if isinstance(existing_pp, dict) else {}
        )
        for pattern, prop_schema in pattern_props.items():
            existing = merged_schema["patternProperties"].get(pattern)
            if existing is not None:
                prop_schema = _combine_under_allof(existing, prop_schema)
            merged_schema["patternProperties"][pattern] = prop_schema

    # Keep propertyNames so unresolved $ref there cannot break later jsonref loads.
    # json_schema_util does not emit fields from propertyNames; we only preserve/resolve.
    if "propertyNames" in resolved_allof:
        incoming_names = resolved_allof["propertyNames"]
        existing_names = merged_schema.get("propertyNames")
        if existing_names is None:
            merged_schema["propertyNames"] = incoming_names
        elif isinstance(existing_names, dict) and isinstance(incoming_names, dict):
            merged_schema["propertyNames"] = _combine_under_allof(
                existing_names, incoming_names
            )


# Numeric allOf bounds without exclusive* modifiers. minimum/maximum are merged
# jointly with exclusiveMinimum/Maximum so OpenAPI 3.0 boolean exclusivity stays
# tied to the winning bound (independent OR of exclusivity over-restricts).
_Number = Union[int, float]
_ALLOF_NUMERIC_BOUNDS: Dict[str, Callable[[_Number, _Number], _Number]] = {
    "minLength": max,
    "minItems": max,
    "minProperties": max,
    "maxLength": min,
    "maxItems": min,
    "maxProperties": min,
}


def _is_number(value: object) -> TypeGuard[_Number]:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _merge_numeric_keyword(
    merged_schema: Dict[str, Any],
    key: str,
    incoming: object,
    more_restrictive: Callable[[_Number, _Number], _Number],
) -> None:
    if not _is_number(incoming):
        return
    current = merged_schema.get(key)
    merged_schema[key] = (
        more_restrictive(current, incoming) if _is_number(current) else incoming
    )


def _merge_allof_validation_keywords(
    merged_schema: Dict[str, Any], resolved_allof: Dict[str, Any]
) -> None:
    for key, more_restrictive in _ALLOF_NUMERIC_BOUNDS.items():
        if key in resolved_allof:
            _merge_numeric_keyword(
                merged_schema, key, resolved_allof[key], more_restrictive
            )
    # minimum/maximum + exclusive* (OpenAPI 3.0 bool modifiers / draft-6 numerics).
    _merge_bound_with_exclusivity(
        merged_schema, resolved_allof, "minimum", "exclusiveMinimum", prefer_higher=True
    )
    _merge_bound_with_exclusivity(
        merged_schema,
        resolved_allof,
        "maximum",
        "exclusiveMaximum",
        prefer_higher=False,
    )
    if "uniqueItems" in resolved_allof:
        merged_schema["uniqueItems"] = (
            merged_schema.get("uniqueItems", False) or resolved_allof["uniqueItems"]
        )
    # pattern and multipleOf can't collapse to a single value under allOf (a name must
    # match every pattern / be a multiple of every value); keep the first as best-effort.
    for key in ("pattern", "multipleOf"):
        if key in resolved_allof and key not in merged_schema:
            merged_schema[key] = resolved_allof[key]


def _apply_bool_exclusivity(
    merged_schema: Dict[str, Any], exclusive_key: str, incoming_excl: object
) -> None:
    """Attach exclusivity from the member that owns the winning bound.

    Absent exclusivity clears a stale OpenAPI 3.0 boolean so a stricter inclusive
    bound is not left exclusive from a weaker member.
    """
    if isinstance(incoming_excl, bool):
        merged_schema[exclusive_key] = incoming_excl
    elif exclusive_key in merged_schema and isinstance(
        merged_schema.get(exclusive_key), bool
    ):
        del merged_schema[exclusive_key]


def _merge_bound_with_exclusivity(
    merged_schema: Dict[str, Any],
    resolved_allof: Dict[str, Any],
    bound_key: str,
    exclusive_key: str,
    prefer_higher: bool,
) -> None:
    """Merge a numeric bound with its exclusive* keyword as one constraint.

    OpenAPI 3.0 exclusiveMinimum/Maximum are boolean modifiers of minimum/maximum.
    Taking max(minimum) then OR(exclusiveMinimum) rejects valid boundary values when
    the exclusivity belonged to a weaker bound. Draft-6+ numeric exclusive* values
    remain independent bounds and use the numeric merge path.
    """
    incoming_bound = resolved_allof.get(bound_key)
    incoming_excl = resolved_allof.get(exclusive_key)
    has_bound = bound_key in resolved_allof and _is_number(incoming_bound)
    has_excl = exclusive_key in resolved_allof
    reducer: Callable[[_Number, _Number], _Number] = max if prefer_higher else min

    if not has_bound and not has_excl:
        return

    # Draft-6+ numeric exclusive bound: merge independently of minimum/maximum.
    if has_excl and _is_number(incoming_excl):
        if has_bound:
            _merge_numeric_keyword(merged_schema, bound_key, incoming_bound, reducer)
        _merge_exclusive_bound(merged_schema, resolved_allof, exclusive_key, reducer)
        return

    if has_bound:
        assert _is_number(incoming_bound)
        current_bound = merged_schema.get(bound_key)
        current_excl = merged_schema.get(exclusive_key)
        # Mixed draft: keep an already-merged numeric exclusive* untouched.
        if _is_number(current_excl):
            _merge_numeric_keyword(merged_schema, bound_key, incoming_bound, reducer)
            return

        if not _is_number(current_bound):
            merged_schema[bound_key] = incoming_bound
            _apply_bool_exclusivity(merged_schema, exclusive_key, incoming_excl)
            return

        incoming_wins = (
            incoming_bound > current_bound
            if prefer_higher
            else incoming_bound < current_bound
        )
        if incoming_wins:
            merged_schema[bound_key] = incoming_bound
            _apply_bool_exclusivity(merged_schema, exclusive_key, incoming_excl)
        elif incoming_bound == current_bound:
            current_strict = isinstance(current_excl, bool) and current_excl
            incoming_strict = isinstance(incoming_excl, bool) and incoming_excl
            if current_strict or incoming_strict:
                merged_schema[exclusive_key] = True
        return

    # Boolean exclusivity with no bound on this member is ignored. Attaching it to
    # another member's bound is order-dependent (OpenAPI 3.0 exclusive* modifies
    # that member's minimum/maximum, not a later sibling's).
    return


def _merge_exclusive_bound(
    merged_schema: Dict[str, Any],
    resolved_allof: Dict[str, Any],
    key: str,
    numeric_more_restrictive: Callable[[_Number, _Number], _Number],
) -> None:
    if key not in resolved_allof:
        return
    incoming = resolved_allof[key]
    if _is_number(incoming):
        _merge_numeric_keyword(merged_schema, key, incoming, numeric_more_restrictive)
        return
    if isinstance(incoming, bool):
        # A boolean flag must not clobber an already-merged numeric bound, so a
        # mixed-version spec stays order-independent.
        current = merged_schema.get(key)
        merged_schema[key] = (
            current if _is_number(current) else bool(current) or incoming
        )


def _combine_under_allof(
    existing: Dict[str, Any], addition: Dict[str, Any]
) -> Dict[str, Any]:
    # Flatten a pure allOf wrapper from an earlier merge before appending, so a 3rd+
    # contributor is a sibling not a nested member — merge_allof_schemas(resolving_refs)
    # does not expand nested member allOf and would drop the earlier schemas.
    members: List[Dict] = []
    for part in (existing, addition):
        if (
            isinstance(part, dict)
            and set(part.keys()) == {"allOf"}
            and isinstance(part["allOf"], list)
        ):
            members.extend(part["allOf"])
        else:
            members.append(part)
    return {"allOf": members}


def _lookup_local_ref_target(
    ref_path: str,
    sw_dict: Dict,
    *,
    missing: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Resolve `#/definitions/X` or `#/components/schemas/X`.

    Returns None for unsupported ref formats (external files, other JSON Pointers).
    When the name is absent under a known prefix, returns `missing` (default None).
    """
    if ref_path.startswith("#/definitions/"):
        schemas_map = sw_dict.get("definitions", {})
    elif ref_path.startswith("#/components/schemas/"):
        schemas_map = sw_dict.get("components", {}).get("schemas", {})
    else:
        return None
    if not isinstance(schemas_map, dict):
        return missing
    target = schemas_map.get(ref_path.split("/")[-1])
    return target if isinstance(target, dict) else missing


def _resolve_ref_directly(schema: Dict, sw_dict: Dict) -> Dict:
    """
    Resolve a direct $ref reference without full schema resolution.
    Used internally to avoid infinite recursion.
    """
    if not isinstance(schema, dict):
        return schema

    if "$ref" in schema:
        referenced_schema = _lookup_local_ref_target(schema["$ref"], sw_dict)
        if referenced_schema:
            return referenced_schema.copy()

    return schema


def _resolve_pattern_properties(
    resolved_schema: Dict[str, Any], sw_dict: Dict[str, Any], max_depth: int
) -> None:
    pattern_properties = resolved_schema.get("patternProperties")
    if isinstance(pattern_properties, dict):
        # resolved_schema may still alias sw_dict's nested map — copy before rewrite.
        pattern_properties = dict(pattern_properties)
        resolved_schema["patternProperties"] = pattern_properties
        for pattern, prop_schema in list(pattern_properties.items()):
            pattern_properties[pattern] = resolve_schema_references(
                prop_schema, sw_dict, max_depth=max_depth - 1
            )


def _shallow_schema_copy(schema: object) -> object:
    return dict(schema) if isinstance(schema, dict) else schema


def _promote_pattern_properties_to_additional(
    resolved_schema: Dict[str, Any],
) -> None:
    # json_schema_util treats dict additionalProperties as a map and skips
    # named properties — only promote on map-only schemas, after allOf merge.
    pattern_properties = resolved_schema.get("patternProperties")
    if not isinstance(pattern_properties, dict):
        return
    # An existing dict value schema already lets json_schema_util extract the map.
    # Absent, false, and true (JSON Schema default ≡ absent) all still need promotion
    # so closed / unrestricted maps yield extractable columns.
    existing_additional = resolved_schema.get("additionalProperties")
    if isinstance(existing_additional, dict):
        return
    # Empty properties: {} has no named fields to protect.
    if resolved_schema.get("properties"):
        return
    pattern_schemas = list(pattern_properties.values())
    if len(pattern_schemas) == 1:
        resolved_schema["additionalProperties"] = _shallow_schema_copy(
            pattern_schemas[0]
        )
    elif pattern_schemas:
        # Disjoint pattern namespaces (e.g. ^str_ → string, ^num_ → int) collapse to
        # one map value type — a lossy approximation of the original patterns.
        logger.warning(
            "Collapsing %s patternProperties entries into additionalProperties "
            "anyOf; disjoint pattern namespaces are approximated as a single map "
            "value type.",
            len(pattern_schemas),
        )
        resolved_schema["additionalProperties"] = {
            "anyOf": [_shallow_schema_copy(s) for s in pattern_schemas]
        }


def resolve_schema_references(schema: Dict, sw_dict: Dict, max_depth: int = 10) -> Dict:
    """
    Recursively resolve all schema references in a Swagger v2 or OpenAPI v3 spec.
    This ensures that all $ref references are resolved before passing to json_schema_util.py.

    Args:
        schema: Schema dictionary to resolve
        sw_dict: Complete OpenAPI specification for resolving references
        max_depth: Maximum recursion depth (default: 10) to prevent infinite recursion

    Returns:
        Resolved schema dictionary with all $ref references expanded

    Note:
        If max_depth is exceeded, returns partially resolved schema and logs a warning.
        This prevents infinite recursion from deeply nested or circular references.
    """
    if not isinstance(schema, dict):
        return schema

    # Check recursion depth
    if max_depth <= 0:
        logger.warning(
            "Maximum recursion depth exceeded while resolving schema references. "
            "Schema may be deeply nested or contain circular references. "
            "Returning partially resolved schema."
        )
        return schema

    resolved_schema = schema.copy()

    # Handle direct references. OAS 3.1 / JSON Schema draft-2019-09 allow sibling
    # keywords alongside $ref — returning the target alone would drop them.
    if "$ref" in resolved_schema:
        ref_path = resolved_schema["$ref"]
        referenced_schema = _lookup_local_ref_target(ref_path, sw_dict)

        if referenced_schema:
            resolved_referenced = resolve_schema_references(
                referenced_schema, sw_dict, max_depth=max_depth - 1
            )
            siblings = {k: v for k, v in resolved_schema.items() if k != "$ref"}
            if not siblings:
                return resolved_referenced
            return resolve_schema_references(
                _combine_under_allof(resolved_referenced, siblings),
                sw_dict,
                max_depth=max_depth - 1,
            )
        logger.warning(
            "Unable to resolve schema $ref %r; leaving reference unresolved",
            ref_path,
        )

    # Recursively resolve references in properties. Shallow schema.copy() still
    # aliases the nested properties map from sw_dict — copy before rewrite so
    # shared components are not permanently mutated across endpoints.
    properties = resolved_schema.get("properties")
    if isinstance(properties, dict):
        properties = dict(properties)
        resolved_schema["properties"] = properties
        for prop_name, prop_schema in properties.items():
            properties[prop_name] = resolve_schema_references(
                prop_schema, sw_dict, max_depth=max_depth - 1
            )

    # Recursively resolve references in array items
    if "items" in resolved_schema:
        resolved_schema["items"] = resolve_schema_references(
            resolved_schema["items"], sw_dict, max_depth=max_depth - 1
        )

    # Recursively resolve references in additionalProperties
    if "additionalProperties" in resolved_schema and isinstance(
        resolved_schema["additionalProperties"], dict
    ):
        resolved_schema["additionalProperties"] = resolve_schema_references(
            resolved_schema["additionalProperties"], sw_dict, max_depth=max_depth - 1
        )

    # Handle allOf by merging schemas (before treating as union)
    if "allOf" in resolved_schema:
        resolved_schema = merge_allof_schemas(
            resolved_schema, sw_dict, resolving_refs=True, max_depth=max_depth
        )

    # After allOf so keywords contributed by members are resolved too.
    _resolve_pattern_properties(resolved_schema, sw_dict, max_depth)

    # Resolve propertyNames so leftover $refs cannot break jsonref in json_schema_util.
    property_names = resolved_schema.get("propertyNames")
    if isinstance(property_names, dict):
        resolved_schema["propertyNames"] = resolve_schema_references(
            property_names, sw_dict, max_depth=max_depth - 1
        )

    # Handle union types (oneOf, anyOf) - allOf is already handled above
    for union_key in ["oneOf", "anyOf"]:
        if union_key in resolved_schema:
            resolved_schema[union_key] = [
                resolve_schema_references(
                    union_schema, sw_dict, max_depth=max_depth - 1
                )
                for union_schema in resolved_schema[union_key]
            ]

    _promote_pattern_properties_to_additional(resolved_schema)

    return resolved_schema


def extract_schema_from_response_schema(
    response_schema: Dict, sw_dict: Dict, schema_name: str = ""
) -> Dict:
    """
    Extract schema definition from response schema, handling both v2 and v3 references.
    """
    if "$ref" in response_schema:
        referenced = _lookup_local_ref_target(
            response_schema["$ref"], sw_dict, missing={}
        )
        if referenced is not None:
            return referenced

    return response_schema


def get_schema_from_response(
    response_schema: Dict, sw_dict: Dict, max_depth: int = 10
) -> Optional[Dict]:
    """
    Extract the actual schema definition from a response schema.
    Handles both direct schemas and references.

    Args:
        response_schema: Schema dictionary from response
        sw_dict: Complete OpenAPI specification for resolving references
        max_depth: Maximum recursion depth for resolving schema references (default: 10)
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
        return resolve_schema_references(resolved_items_schema, sw_dict, max_depth)

    # Handle direct object schemas
    elif response_schema.get("type") == "object":
        return resolve_schema_references(response_schema, sw_dict, max_depth)

    # Handle references
    elif "$ref" in response_schema:
        resolved_schema = extract_schema_from_response_schema(response_schema, sw_dict)
        # Resolve all references in the schema
        return resolve_schema_references(resolved_schema, sw_dict, max_depth)

    return None

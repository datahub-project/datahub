import json
import logging
import re
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Literal,
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


def flatten2list(d: dict) -> List[str]:
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
    return list(flatten(d))


def _join_url(base: str, path: str) -> str:
    # Docs/recipes often omit a trailing slash on url and a leading slash on
    # swagger_file; naive concatenation would produce a broken host+path join.
    if not path:
        return base
    if base.endswith("/") and path.startswith("/"):
        return f"{base}{path[1:]}"
    if not base.endswith("/") and not path.startswith("/"):
        return f"{base}/{path}"
    return f"{base}{path}"


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
            proxies=proxies,
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
        return requests.get(
            url, headers=headers, proxies=proxies, verify=verify_ssl, timeout=timeout
        )


def get_swag_json(
    url: str,
    token: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    swagger_file: str = "",
    proxies: Optional[dict] = None,
    verify_ssl: bool = True,
) -> Dict:
    tot_url = _join_url(url, swagger_file)
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
    servers = sw_dict.get("servers")
    if isinstance(servers, list) and servers:
        # When the API path doesn't match the OAS path.
        # Some specs declare "servers": [] or entries without a "url" key.
        first_server = servers[0]
        if isinstance(first_server, dict):
            return first_server.get("url", "")
        logger.warning(
            "Ignoring malformed servers[0] entry %r (expected object, got %s)",
            first_server,
            type(first_server).__name__,
        )

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
        if not isinstance(p_o, dict):
            # A malformed path item must not abort every other endpoint's extraction.
            logger.warning(
                "Skipping malformed path item %r (expected object, got %s)",
                p_k,
                type(p_o).__name__,
            )
            continue

        for method in all_methods:
            method_spec = p_o.get(method)
            if not method_spec:
                continue
            if not isinstance(method_spec, dict):
                logger.warning(
                    "Skipping malformed method spec %r on %r (expected object, got %s)",
                    method,
                    p_k,
                    type(method_spec).__name__,
                )
                continue

            responses = method_spec.get("responses", {})
            if not isinstance(responses, dict):
                logger.warning(
                    "Skipping malformed responses %r on method %r of %r "
                    "(expected object, got %s)",
                    responses,
                    method,
                    p_k,
                    type(responses).__name__,
                )
                continue
            base_res = responses.get("200") or responses.get(200)
            if not base_res:
                # if there is no 200 response, we skip this method
                continue
            if not isinstance(base_res, dict):
                logger.warning(
                    "Skipping malformed 200 response %r on method %r of %r "
                    "(expected object, got %s)",
                    base_res,
                    method,
                    p_k,
                    type(base_res).__name__,
                )
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
    data: Dict[str, Any] = {}
    if "content" in base_res:
        res_cont = base_res["content"]
        # get_endpoints calls this once per spec, outside the per-endpoint try/except
        # in APISource.get_workunits_internal -- an unguarded crash here would abort
        # extraction for every endpoint, not just the one with the malformed example.
        if not isinstance(res_cont, dict):
            logger.warning(
                "Skipping malformed 'content' %r for endpoint %r (expected object, got %s)",
                res_cont,
                key,
                type(res_cont).__name__,
            )
            return data
        if "application/json" in res_cont:
            json_content = res_cont["application/json"]
            if not isinstance(json_content, dict):
                logger.warning(
                    "Skipping malformed 'application/json' content %r for endpoint %r "
                    "(expected object, got %s)",
                    json_content,
                    key,
                    type(json_content).__name__,
                )
                return data

            # Check for single example (OpenAPI v3)
            if "example" in json_content:
                data = json_content["example"]
            # Check for multiple examples (OpenAPI v3)
            elif "examples" in json_content:
                examples = json_content["examples"]
                # Take the first example if it's a dict of examples
                if isinstance(examples, dict) and examples:
                    first_example_name = next(iter(examples))
                    first_example = examples[first_example_name]
                    # A well-formed Example Object wraps the value under "value";
                    # tolerate a malformed spec placing the raw value directly.
                    example_value = (
                        first_example.get("value", {})
                        if isinstance(first_example, dict)
                        else first_example
                    )
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
            csv_content = res_cont["text/csv"]
            if isinstance(csv_content, dict) and isinstance(
                csv_content.get("schema"), dict
            ):
                data = csv_content["schema"]
            else:
                logger.warning(
                    "Skipping malformed 'text/csv' content %r for endpoint %r",
                    csv_content,
                    key,
                )
    # Handle OpenAPI v2 format
    elif "examples" in base_res:
        v2_examples = base_res["examples"]
        # The value itself may legitimately be a raw string (literal example text)
        # rather than a parsed object -- only the container needs to be a dict.
        if isinstance(v2_examples, dict):
            data = v2_examples.get("application/json", {})
        else:
            logger.warning(
                "Skipping malformed v2 'examples' %r for endpoint %r",
                v2_examples,
                key,
            )

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
) -> Tuple[List[str], Dict[Any, Any]]:
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
    method: Literal["get", "post"] = "post",
    proxies: Optional[dict] = None,
    verify_ssl: bool = True,
) -> str:
    """
    Trying to post username/password to get auth.
    """
    token = ""
    url4req = _join_url(url, tok_url)
    timeout = _REQUEST_TIMEOUT_SECONDS
    # NOTE: for method="get" the caller substitutes the raw username/password into
    # url4req before calling get_tok. Any exception raised below must not embed
    # url4req/response body (e.g. via a raw `requests` exception), since report.failure
    # renders exception messages verbatim in the ingestion report UI.
    if method == "post":
        # this will make a POST call with username and password
        data = {"username": username, "password": password, "maxDuration": True}
        try:
            response = requests.post(
                url4req,
                proxies=proxies,
                json=data,
                verify=verify_ssl,
                timeout=timeout,
            )
        except requests.exceptions.RequestException as e:
            # from None (not from e): the requests exception message often
            # embeds the request URL, which for method="get" carries the
            # substituted password; chaining it as __cause__ would still leak
            # through exc_info-based DEBUG logging even though report.failure
            # only renders str(exc) for the top-level exception.
            raise ValueError(
                f"Failed to request token from OpenAPI endpoint ({type(e).__name__})"
            ) from None
        if response.status_code == 200:
            try:
                cont = json.loads(response.content)
                if "token" in cont:  # other authentication scheme
                    token = cont["token"]
                else:  # works only for bearer authentication scheme
                    token = f"Bearer {cont['tokens']['access']}"
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                raise ValueError(
                    f"Unexpected token response shape (status {response.status_code})"
                ) from e
    elif method == "get":
        # this will make a GET call with username and password
        try:
            response = requests.get(
                url4req, proxies=proxies, verify=verify_ssl, timeout=timeout
            )
        except requests.exceptions.RequestException as e:
            # from None (not from e): the requests exception message often
            # embeds the request URL, which for method="get" carries the
            # substituted password; chaining it as __cause__ would still leak
            # through exc_info-based DEBUG logging even though report.failure
            # only renders str(exc) for the top-level exception.
            raise ValueError(
                f"Failed to request token from OpenAPI endpoint ({type(e).__name__})"
            ) from None
        if response.status_code == 200:
            try:
                cont = json.loads(response.content)
                token = cont["token"]
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                raise ValueError(
                    f"Unexpected token response shape (status {response.status_code})"
                ) from e
    else:
        raise ValueError(f"Method unrecognised: {method}")
    if token != "":
        return token
    else:
        raise Exception(
            f"Unable to get a valid token: received status {response.status_code}"
        )


def set_metadata(
    dataset_name: str,
    fields: List[str],
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


def merge_allof_schemas(schema: Dict, sw_dict: Dict, max_depth: int = 10) -> Dict:
    """
    Merge allOf schemas into a single schema object.

    According to JSON Schema, allOf means all schemas must be valid, which means
    we should merge their properties, required fields, and other attributes.

    Each allOf member is fully resolved via _resolve_schema_refs before merge
    so nested $refs (including those inside composed targets) cannot survive.
    Map promotion (_promote_pattern_properties_to_additional) is deferred to the
    public resolve_schema_references entry point so it runs once on the merged tree.

    Args:
        schema: Schema dictionary that may contain allOf
        sw_dict: Complete OpenAPI specification for resolving references
        max_depth: Maximum recursion depth for resolving schema references (default: 10)

    Returns:
        Merged schema with all allOf entries combined
    """
    if not isinstance(schema, dict):
        return schema

    # If no allOf, return as-is
    if "allOf" not in schema:
        return schema

    if max_depth <= 0:
        logger.warning(
            "Maximum recursion depth exceeded while merging allOf schemas. "
            "Leaving members unmerged."
        )
        # Return unchanged rather than stripping "allOf": callers must still see the
        # member schemas (e.g. a pure-allOf wrapper) instead of losing them outright.
        return schema

    allof_schemas = schema.get("allOf", [])
    if allof_schemas and not isinstance(allof_schemas, list):
        # A non-list, truthy "allOf" (e.g. a dict from a generator bug) would
        # otherwise iterate as if each of its keys were a member schema -- every
        # one fails the isinstance(resolved_allof, dict) guard below and is
        # skipped, silently discarding everything the malformed allOf contained.
        logger.warning(
            "Schema 'allOf' is not a list (got %s); ignoring",
            type(allof_schemas).__name__,
        )
        return {k: v for k, v in schema.items() if k != "allOf"}
    if not allof_schemas:
        return schema

    # Start with a base schema (copy everything except allOf)
    merged_schema = {k: v for k, v in schema.items() if k != "allOf"}
    child_depth = max_depth - 1

    # oneOf/anyOf are independent constraints, not a single value to pick one winner
    # from: every allOf member's oneOf/anyOf must be satisfied simultaneously. We
    # accumulate all members' contributions here and resolve them once after the
    # loop below (see _finalize_oneof_anyof_contributions) — resolving them
    # per-member inline, by deferring a collision into merged_schema["allOf"] and
    # relying on this same function to reprocess it, previously caused the identical
    # collision to be re-detected on every recursive re-entry: a pure fixpoint that
    # only stopped when max_depth was exhausted, silently discarding all other
    # merged fields.
    oneof_anyof_contributions = _seed_oneof_anyof_contributions(merged_schema)

    # Merge each schema in allOf
    for allof_schema in allof_schemas:
        resolved_allof = _resolve_schema_refs(
            allof_schema, sw_dict, max_depth=child_depth
        )
        if not isinstance(resolved_allof, dict):
            # Boolean schemas (true/false) are valid allOf members from JSON Schema
            # draft-6+; "true" is a no-op, and treating "false" as a no-op too is a
            # deliberate simplification rather than making the whole schema
            # unsatisfiable. Either way, nothing below this point is dict-shaped.
            continue

        # Merge properties — same-named fields combine under allOf (like map keywords).
        _merge_allof_properties(merged_schema, resolved_allof, sw_dict, child_depth)

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

        # First-wins for keywords that cannot be meaningfully intersected under allOf.
        for key in _ALLOF_FIRST_WINS_KEYWORDS:
            if key in resolved_allof and key not in merged_schema:
                merged_schema[key] = resolved_allof[key]

        for key in ("oneOf", "anyOf"):
            if resolved_allof.get(key):
                oneof_anyof_contributions[key].append(resolved_allof[key])

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
                    max_depth=child_depth,
                )

        _merge_allof_map_keywords(merged_schema, resolved_allof, sw_dict, child_depth)

    _finalize_oneof_anyof_contributions(merged_schema, oneof_anyof_contributions)

    return merged_schema


def _seed_oneof_anyof_contributions(
    merged_schema: Dict[str, Any],
) -> Dict[str, List[Any]]:
    """Pop merged_schema's own oneOf/anyOf (if any) into the contributions the allOf
    member loop will add to, so a colliding member doesn't silently overwrite it.

    An empty oneOf/anyOf ([]) is not a real constraint (get_schema_metadata rejects
    it outright), so it is dropped here rather than collected.
    """
    contributions: Dict[str, List[Any]] = {"oneOf": [], "anyOf": []}
    for key in ("oneOf", "anyOf"):
        if key in merged_schema:
            sibling_value = merged_schema.pop(key)
            if sibling_value:
                contributions[key].append(sibling_value)
    return contributions


def _finalize_oneof_anyof_contributions(
    merged_schema: Dict[str, Any], contributions: Dict[str, List[Any]]
) -> None:
    """A single contributor becomes the top-level keyword (no allOf needed); 2+
    distinct contributors become sibling allOf members instead of one being picked
    as a "winner" — this is a terminal representation and is deliberately NOT
    re-merged, since re-running allOf-collision handling on it would just reproduce
    the identical collision.
    """
    for key, values in contributions.items():
        if not values:
            continue
        unique_values: List[Any] = []
        for value in values:
            if value not in unique_values:
                unique_values.append(value)
        if len(unique_values) == 1:
            merged_schema[key] = unique_values[0]
        else:
            merged_schema.setdefault("allOf", []).extend(
                {key: value} for value in unique_values
            )


# Scalar keywords that cannot collapse under allOf — keep the first. oneOf/anyOf are
# handled separately (see oneof_anyof_contributions above) since, unlike these,
# every member's contribution must be preserved rather than the first one winning.
_ALLOF_FIRST_WINS_KEYWORDS = (
    "type",
    "format",
    "description",
    "title",
    "enum",
    "default",
    "example",
    "discriminator",
    "nullable",
    "deprecated",
    "readOnly",
    "writeOnly",
)


def _merge_allof_properties(
    merged_schema: Dict[str, Any],
    resolved_allof: Dict[str, Any],
    sw_dict: Dict[str, Any],
    max_depth: int,
) -> None:
    incoming_props = resolved_allof.get("properties")
    if not isinstance(incoming_props, dict):
        return
    existing_props = merged_schema.get("properties")
    # Copy before mutate — merged_schema may still alias a member's properties dict.
    merged_schema["properties"] = (
        dict(existing_props) if isinstance(existing_props, dict) else {}
    )
    for prop_name, prop_schema in incoming_props.items():
        existing = merged_schema["properties"].get(prop_name)
        if (
            existing is not None
            and isinstance(existing, dict)
            and isinstance(prop_schema, dict)
        ):
            merged_schema["properties"][prop_name] = merge_allof_schemas(
                _combine_under_allof(existing, prop_schema),
                sw_dict,
                max_depth=max_depth,
            )
        else:
            merged_schema["properties"][prop_name] = prop_schema


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
                # Resolve the combined fragment the same way additionalProperties/
                # properties do, so a colliding pattern's merged shape does not
                # depend on how deeply this schema happens to be nested.
                prop_schema = merge_allof_schemas(
                    _combine_under_allof(existing, prop_schema),
                    sw_dict,
                    max_depth=max_depth,
                )
            merged_schema["patternProperties"][pattern] = prop_schema

    # Keep propertyNames so unresolved $ref there cannot break later jsonref loads.
    # json_schema_util does not emit fields from propertyNames; we only preserve/resolve.
    if "propertyNames" in resolved_allof:
        incoming_names = resolved_allof["propertyNames"]
        existing_names = merged_schema.get("propertyNames")
        if existing_names is None:
            merged_schema["propertyNames"] = incoming_names
        elif isinstance(existing_names, dict) and isinstance(incoming_names, dict):
            merged_schema["propertyNames"] = merge_allof_schemas(
                _combine_under_allof(existing_names, incoming_names),
                sw_dict,
                max_depth=max_depth,
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
    # contributor is a sibling not a nested member — merge_allof_schemas does not
    # expand nested member allOf and would drop the earlier schemas.
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
    ref_path: object,
    sw_dict: Dict,
    *,
    missing: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Resolve `#/definitions/X` or `#/components/schemas/X`.

    Returns None for unsupported ref formats (external files, other JSON Pointers,
    or a non-string $ref value -- e.g. an empty "$ref:" line parses as None in YAML).
    When the name is absent under a known prefix, returns `missing` (default None).
    """
    if not isinstance(ref_path, str):
        return None
    if ref_path.startswith("#/definitions/"):
        schemas_map = sw_dict.get("definitions", {})
    elif ref_path.startswith("#/components/schemas/"):
        # Two-step lookup: guard "components" being present-but-non-dict (e.g.
        # explicit "components: null") before chaining another .get onto it.
        components = sw_dict.get("components")
        schemas_map = (
            components.get("schemas", {}) if isinstance(components, dict) else {}
        )
    else:
        return None
    if not isinstance(schemas_map, dict):
        return missing
    target = schemas_map.get(ref_path.split("/")[-1])
    return target if isinstance(target, dict) else missing


def _resolve_pattern_properties(
    resolved_schema: Dict[str, Any], sw_dict: Dict[str, Any], max_depth: int
) -> None:
    pattern_properties = resolved_schema.get("patternProperties")
    if isinstance(pattern_properties, dict):
        # resolved_schema may still alias sw_dict's nested map — copy before rewrite.
        pattern_properties = dict(pattern_properties)
        resolved_schema["patternProperties"] = pattern_properties
        for pattern, prop_schema in list(pattern_properties.items()):
            pattern_properties[pattern] = _resolve_schema_refs(
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


def _normalize_map_schemas(schema: object) -> None:
    """Post-order: strip leftover $refs, then promote patternProperties."""
    if not isinstance(schema, dict):
        return
    properties = schema.get("properties")
    if isinstance(properties, dict):
        for prop_schema in properties.values():
            _normalize_map_schemas(prop_schema)
    if "items" in schema:
        _normalize_map_schemas(schema["items"])
    additional = schema.get("additionalProperties")
    if isinstance(additional, dict):
        _normalize_map_schemas(additional)
    pattern_properties = schema.get("patternProperties")
    if isinstance(pattern_properties, dict):
        for prop_schema in pattern_properties.values():
            _normalize_map_schemas(prop_schema)
    property_names = schema.get("propertyNames")
    if isinstance(property_names, dict):
        _normalize_map_schemas(property_names)
    for union_key in ("oneOf", "anyOf", "allOf"):
        members = schema.get(union_key)
        if isinstance(members, list):
            for member in members:
                _normalize_map_schemas(member)
    _promote_pattern_properties_to_additional(schema)



# Carried through verbatim: literal example/default instance data, never
# schema-interpreted by _resolve_schema_refs (see its docstring) or by
# merge_allof_schemas (_ALLOF_FIRST_WINS_KEYWORDS) either, so a "$ref" key
# inside one of these is real data, not an unresolved schema reference.
_OPAQUE_DATA_KEYWORDS = ("example", "default")


def _strip_unresolved_refs(
    schema: object, *, is_property_map: bool = False
) -> Tuple[object, bool]:
    """Recursively remove any leftover "$ref" *keyword* anywhere in the tree.

    This walk is intentionally generic (every dict value and list item, not just the
    JSON-Schema keywords _normalize_map_schemas understands) so it also covers keys
    normalization doesn't visit (e.g. "not", "if"/"then"/"else", "contains") — a
    depth-limited resolution can leave a raw $ref under any of those, and an
    unresolved $ref reaching jsonref crashes the whole endpoint.

    ``is_property_map`` is True only for the dict directly under "properties" /
    "patternProperties" — i.e. a map keyed by property *names*, not schema
    keywords. A property can legitimately be named "$ref"; that key must not be
    mistaken for a leftover reference. "example"/"default" values are skipped
    entirely (kept exactly as-is) for the same reason: they're opaque instance
    data, not schema, so a "$ref" inside one is real data too.

    Returns (possibly-rebuilt schema, True if any $ref keyword was found and
    removed). Does NOT mutate the input in place: a node under one of the
    unvisited keywords above may still be the exact same dict object as a shared
    sw_dict component (neither _resolve_schema_refs nor _normalize_map_schemas
    walks into "not"/"if"/"then"/"else"/"contains", so they never copy it
    either), and deleting a key from it in place would permanently corrupt that
    shared component for every other endpoint resolved later in the same run. A
    branch with nothing to strip is returned unchanged (same object), so this
    only allocates along paths that actually contained a $ref.
    """
    if isinstance(schema, dict):
        found = ("$ref" in schema) and not is_property_map
        new_dict = {}
        for key, value in schema.items():
            if key == "$ref" and not is_property_map:
                continue
            if key in _OPAQUE_DATA_KEYWORDS:
                new_dict[key] = value
                continue
            new_value, child_found = _strip_unresolved_refs(
                value, is_property_map=key in ("properties", "patternProperties")
            )
            found = found or child_found
            new_dict[key] = new_value
        return (new_dict if found else schema), found
    elif isinstance(schema, list):
        found = False
        new_list = []
        for item in schema:
            new_item, child_found = _strip_unresolved_refs(item)
            found = found or child_found
            new_list.append(new_item)
        return (new_list if found else schema), found
    return schema, False


def _resolve_schema_refs(schema: Dict, sw_dict: Dict, max_depth: int = 10) -> Dict:
    """Expand $refs / allOf only — no patternProperties→additionalProperties promotion.

    Promotion is lossy for mixed properties+patternProperties schemas and must run
    once on the fully merged tree via resolve_schema_references / _normalize_map_schemas.
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
        # missing=None so a found empty {} target is distinct from "not found".
        referenced_schema = _lookup_local_ref_target(ref_path, sw_dict)

        if referenced_schema is not None:
            resolved_referenced = _resolve_schema_refs(
                referenced_schema, sw_dict, max_depth=max_depth - 1
            )
            siblings = {k: v for k, v in resolved_schema.items() if k != "$ref"}
            if not siblings:
                return resolved_referenced
            # Sibling keywords (incl. nested $refs / allOf) are resolved when
            # merge_allof_schemas fully expands each allOf member.
            return _resolve_schema_refs(
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
            properties[prop_name] = _resolve_schema_refs(
                prop_schema, sw_dict, max_depth=max_depth - 1
            )

    # Recursively resolve references in array items
    if "items" in resolved_schema:
        resolved_schema["items"] = _resolve_schema_refs(
            resolved_schema["items"], sw_dict, max_depth=max_depth - 1
        )

    # Recursively resolve references in additionalProperties
    if "additionalProperties" in resolved_schema and isinstance(
        resolved_schema["additionalProperties"], dict
    ):
        resolved_schema["additionalProperties"] = _resolve_schema_refs(
            resolved_schema["additionalProperties"], sw_dict, max_depth=max_depth - 1
        )

    # Handle union types (oneOf, anyOf) before allOf: merge_allof_schemas may
    # relocate this schema's own oneOf/anyOf into a terminal allOf wrapper (when an
    # allOf member also contributes one) that nothing downstream walks back into —
    # so any $ref inside it must already be resolved before that happens, or those
    # fields are lost when _strip_unresolved_refs later strips the leftover $ref.
    for union_key in ["oneOf", "anyOf"]:
        members = resolved_schema.get(union_key)
        if isinstance(members, list):
            resolved_schema[union_key] = [
                _resolve_schema_refs(union_schema, sw_dict, max_depth=max_depth - 1)
                for union_schema in members
            ]

    # Handle allOf by merging schemas (after oneOf/anyOf are resolved above)
    if "allOf" in resolved_schema:
        resolved_schema = merge_allof_schemas(
            resolved_schema, sw_dict, max_depth=max_depth
        )

    # After allOf so keywords contributed by members are resolved too.
    _resolve_pattern_properties(resolved_schema, sw_dict, max_depth)

    # Resolve propertyNames so leftover $refs cannot break jsonref in json_schema_util.
    property_names = resolved_schema.get("propertyNames")
    if isinstance(property_names, dict):
        resolved_schema["propertyNames"] = _resolve_schema_refs(
            property_names, sw_dict, max_depth=max_depth - 1
        )

    return resolved_schema


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
        patternProperties→additionalProperties promotion runs once after full merge.
    """
    resolved_schema = _resolve_schema_refs(schema, sw_dict, max_depth=max_depth)
    if isinstance(resolved_schema, dict):
        _normalize_map_schemas(resolved_schema)
        # Postcondition: jsonref must never see a leftover $ref after normalize. A
        # generic sweep (not an assert) so a depth-limited $ref under a keyword
        # normalize doesn't structurally walk (e.g. "not") degrades gracefully
        # instead of crashing the endpoint — and isn't compiled out under -O.
        stripped_schema, stripped = _strip_unresolved_refs(resolved_schema)
        assert isinstance(stripped_schema, dict)
        resolved_schema = stripped_schema
        if stripped:
            logger.warning(
                "Unresolved schema $ref(s) remained after normalization; "
                "removed to avoid jsonref failure"
            )
    return resolved_schema


# Keywords that imply an object (or composition) schema even without type: object.
_OBJECT_SHAPE_KEYS = (
    "properties",
    "patternProperties",
    "additionalProperties",
    "allOf",
    "oneOf",
    "anyOf",
)


def _looks_like_object_or_composition_schema(schema: Dict) -> bool:
    if schema.get("type") == "object":
        return True
    for key in _OBJECT_SHAPE_KEYS:
        if key not in schema:
            continue
        value = schema[key]
        # An empty/no-op contribution (e.g. "properties": {}, "oneOf": [],
        # "additionalProperties": false) carries no field information — accepting
        # it here would suppress the example-data/live-API fallbacks in
        # get_workunits_internal for a schema that yields zero extractable fields.
        if key == "additionalProperties":
            if value is not False:
                return True
        elif value:
            return True
    return False


def get_schema_from_response(
    response_schema: object, sw_dict: Dict, max_depth: int = 10
) -> Optional[Dict]:
    """
    Extract the actual schema definition from a response schema.
    Handles both direct schemas and references.

    Args:
        response_schema: Schema dictionary from response (typed as `object`, not
            `Dict`, since a bare `true`/`false` is a valid top-level JSON Schema)
        sw_dict: Complete OpenAPI specification for resolving references
        max_depth: Maximum recursion depth for resolving schema references (default: 10)
    """
    if not response_schema:
        return None
    if not isinstance(response_schema, dict):
        # A bare `true`/`false` is a valid top-level JSON Schema (matches
        # anything / nothing), consistent with how merge_allof_schemas treats a
        # boolean allOf member -- but it carries no extractable fields here.
        return None

    # Handle array responses. resolve_schema_references (not a bare $ref lookup) so a
    # $ref with sibling keywords under "items" is not silently dropped.
    if response_schema.get("type") == "array":
        items_schema = response_schema.get("items", {})
        if not isinstance(items_schema, dict):
            # A bare `true`/`false` "items" schema carries no extractable fields
            # here, same as a bare boolean top-level schema above -- and
            # resolve_schema_references would otherwise return it unchanged
            # (e.g. `True`), which is truthy and gets mistaken for a resolved
            # schema by callers that check for one.
            return None
        return resolve_schema_references(items_schema, sw_dict, max_depth)

    # Handle references (before object-shape fallthrough — $ref may carry siblings,
    # which resolve_schema_references (not a bare $ref lookup) preserves).
    if "$ref" in response_schema:
        return resolve_schema_references(response_schema, sw_dict, max_depth)

    # type is optional in JSON Schema — bare properties / allOf / oneOf are valid objects.
    if _looks_like_object_or_composition_schema(response_schema):
        return resolve_schema_references(response_schema, sw_dict, max_depth)

    return None

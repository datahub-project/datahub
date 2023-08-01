import json
import logging
import re
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

import requests
import yaml
from requests.auth import HTTPBasicAuth

from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    OtherSchemaClass,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
)

logger = logging.getLogger(__name__)

GET_METHOD = "get"
CONTENT_TYPE_JSON = "application/json"
CONTENT_TYPE_CSV = "text/csv"


def flatten(d: dict, prefix: str = "") -> Generator:
    for k, v in d.items():
        if isinstance(v, dict):
            yield from flatten(v, f"{prefix}.{k}")
        else:
            yield f"{prefix}-{k}".strip(".")


def flatten2list(d: dict) -> list:
    """
    This function explodes dictionary keys such as:
        d = {"first":
            {"second_a": 3, "second_b": 4},
         "another": 2,
         "anotherone": {"third_a": {"last": 3}}
         }

    yeilds:

        ["first.second_a",
         "first.second_b",
         "another",
         "anotherone.third_a.last"
         ]
    """
    return [d[1:] if d[0] == "-" else d for d in flatten(d)]


def request_call(
    url: str,
    token: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> requests.Response:
    headers = {"accept": "application/json"}

    if username is not None and password is not None:
        return requests.get(
            url, headers=headers, auth=HTTPBasicAuth(username, password)
        )

    elif token is not None:
        headers["Authorization"] = f"Bearer {token}"
        return requests.get(url, headers=headers)
    else:
        return requests.get(url, headers=headers)


def get_swag_json(
    url: str,
    token: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    swagger_file: str = "",
) -> Dict:
    tot_url = url + swagger_file
    if token is not None:
        response = request_call(url=tot_url, token=token)
    else:
        response = request_call(url=tot_url, username=username, password=password)

    if response.status_code != 200:
        raise Exception(f"Unable to retrieve {tot_url}, error {response.status_code}")
    try:
        dict_data = json.loads(response.content)
    except json.JSONDecodeError:  # it's not a JSON!
        dict_data = yaml.safe_load(response.content)
    return dict_data


def check_sw_version(sw_dict: dict) -> None:
    v_split = (
        sw_dict["swagger"].split(".")
        if sw_dict.get("swagger", "")
        else sw_dict["openapi"].split(".")
    )

    version_major, version_minor, *_ = tuple(int(v) for v in v_split)

    if version_major == 3 and version_minor > 0:
        raise NotImplementedError(
            "This plugin is not compatible with Swagger version >3.0"
        )


def get_endpoints(specification: dict) -> dict:  # noqa: C901
    """
    Get all the URLs accepting the "GET" method, together with their description and the tags
    """
    url_details = {}

    check_sw_version(specification)

    for api_path, path_details in specification["paths"].items():
        # will track only the "get" methods, which are the ones that give us data
        if path_get_details := path_details.get(GET_METHOD):
            api_response = path_get_details["responses"].get("200") or path_get_details[
                "responses"
            ].get(200)
            if api_response is None:
                continue

            desc = path_get_details.get("description") or path_get_details.get(
                "summary", ""
            )

            tags = path_get_details.get("tags", [])

            url_details[api_path] = {
                "description": desc,
                "tags": tags,
            }

            if api_response.get("schema"):
                url_details[api_path]["schema"] = api_response["schema"]

            # trying if dataset is defined in swagger...
            if response_content := api_response.get("content"):
                if json_schema := response_content.get(CONTENT_TYPE_JSON, {}).get(
                    "schema"
                ):
                    url_details[api_path]["schema"] = json_schema
                elif response_content.get(CONTENT_TYPE_JSON):
                    example = response_content[CONTENT_TYPE_JSON].get(
                        "example"
                    ) or response_content[CONTENT_TYPE_JSON].get("examples")
                    if example:
                        if isinstance(example, dict):
                            url_details[api_path]["data"] = example
                        elif isinstance(example, list):
                            # taking the first example
                            url_details[api_path]["data"], *_ = example
                    else:
                        logger.warning(
                            f"Field in swagger file does not give consistent data --- {api_path}"
                        )
                elif response_content.get(CONTENT_TYPE_CSV):
                    url_details[api_path]["data"] = response_content[CONTENT_TYPE_CSV][
                        "schema"
                    ]
            elif api_response.get("examples"):
                url_details[api_path]["data"] = (
                    api_response["examples"].get(CONTENT_TYPE_JSON)
                    or api_response["examples"]
                )

            # checking whether there are defined parameters to execute the call...
            if path_get_details.get("parameters"):
                url_details[api_path]["parameters"] = path_get_details["parameters"]

    return dict(sorted(url_details.items()))


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
        if examples[ex2use].get(clean_name):
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
    if not dets:
        return url
    dets_w_id = [det for det in dets if "id" in det]  # the fields containing "id"
    if len(dets) == len(dets_w_id):
        # if we only have fields containing IDs, we guess to use "1"s
        return compose_url_attr(url, ["1"] * len(dets_w_id))
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
        if not dict_data:
            logger.warning(f"Empty data --- {dataset_name}")
            return [], {}
        # so we take the fields of the first element,
        # if it's a dict
        response_data, *_ = dict_data
        if isinstance(response_data, dict):
            return flatten2list(response_data), response_data
        elif isinstance(response_data, str):
            # this is actually data
            return ["contains_a_string"], {"contains_a_string": response_data}
        else:
            raise ValueError("unknown format")
    if len(dict_data.keys()) > 1:
        # the elements are directly inside the dict
        return flatten2list(dict_data), dict_data
    dst_key, *_ = dict_data.keys()  # the first and unique key is the dataset's name

    try:
        return flatten2list(dict_data[dst_key]), dict_data[dst_key]
    except AttributeError:
        # if the content is a list, we should treat each element as a dataset.
        # ..but will take the keys of the first element (to be improved)
        if isinstance(dict_data[dst_key], list):
            if not dict_data[dst_key]:
                return [], {}  # it's empty!
            else:
                return flatten2list(dict_data[dst_key][0]), dict_data[dst_key][0]
        else:
            logger.warning(f"Unable to get the attributes --- {dataset_name}")
            return [], {}


def get_tok(
    url: str,
    username: str = "",
    password: str = "",
    tok_url: str = "",
    method: str = "post",
) -> str:
    """
    Trying to post username/password to get auth.
    """
    token = ""
    url4req = url + tok_url
    if method == "post":
        # this will make a POST call with username and password
        data = {"username": username, "password": password}
        # url2post = url + "api/authenticate/"
        response = requests.post(url4req, data=data)
        if response.status_code == 200:
            cont = json.loads(response.content)
            token = cont["tokens"]["access"]
    elif method == "get":
        # this will make a GET call with username and password
        response = requests.get(url4req)
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
    dataset_name: str, fields: List, platform: str = "api"
) -> SchemaMetadata:
    canonical_schema: List[SchemaField] = []

    for column in fields:
        field = SchemaField(
            fieldPath=column,
            nativeDataType="str",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            description="",
            recursive=False,
        )
        canonical_schema.append(field)

    schema_metadata = SchemaMetadata(
        schemaName=dataset_name,
        platform=f"urn:li:dataPlatform:{platform}",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=canonical_schema,
    )
    return schema_metadata


OBJECT_TYPE = "object"
ARRAY_TYPE = "array"
UNKNOWN_TYPE = "unknown"

TYPES_MAPPING = {
    "string": StringTypeClass,
    "integer": NumberTypeClass,
    "number": NumberTypeClass,
    "boolean": BooleanTypeClass,
    ARRAY_TYPE: ArrayTypeClass,
    OBJECT_TYPE: MapTypeClass,
    UNKNOWN_TYPE: NullTypeClass,
}


class SchemaMetadataExtractor:
    """
    Class for extracting metadata from schemas, defined in definitions
    Recursively going through all fields without max depth limitations, avoiding circle dependencies
    """

    def __init__(
        self,
        dataset_name: str,
        endpoint_schema: dict,
        full_specification: dict,
        platform: str = "api",
    ) -> None:
        self.dataset_name = dataset_name
        self.endpoint_schema = endpoint_schema
        self.full_specification = full_specification
        self.platform = platform
        self.canonical_schema: list[SchemaField] = []
        self.schemas_stack: list[str] = []

    def get_schema_by_ref(self, schema_ref: str) -> tuple[str, dict]:
        _, *schema_path_parts = schema_ref.split("/")
        schema_name = ""
        schema_data = self.full_specification
        for part in schema_path_parts:
            schema_name = part
            schema_data = schema_data.get(part, {})
        if not schema_data:
            logger.warning(f"Schema is empty --- {schema_name}")
        return schema_name, schema_data

    def parse_nested_schema(
        self,
        schema_ref: str,
        field_path: str = "",
    ) -> None:
        schema_name, inner_schema = self.get_schema_by_ref(schema_ref)
        if schema_name in self.schemas_stack:
            return
        self.parse_schema(inner_schema, field_path, current_schema_name=schema_name)

    def parse_array_type(
        self,
        items: Union[dict, str],
        field_path: str = "",
        description: str = "",
        current_schema_name: str = "",
    ) -> None:
        if isinstance(items, dict) and items.get("$ref", ""):
            _, inner_schema = self.get_schema_by_ref(items.get("$ref", ""))
            if inner_schema.get("oneOf") or inner_schema.get("allOf"):
                nested_type = OBJECT_TYPE
            else:
                nested_type = inner_schema.get("type", UNKNOWN_TYPE)
        else:
            nested_type = (
                items.get("type", OBJECT_TYPE) if isinstance(items, dict) else items
            )
        if field_path:
            field = SchemaField(
                fieldPath=field_path,
                type=SchemaFieldDataTypeClass(
                    type=ArrayTypeClass(nestedType=[nested_type])
                ),
                nativeDataType=repr(ARRAY_TYPE),
                description=description,
                recursive=False,
            )
            self.canonical_schema.append(field)
        if not isinstance(items, dict) or nested_type != OBJECT_TYPE:
            return
        if items.get("$ref"):
            self.schemas_stack.append(current_schema_name)
            self.parse_nested_schema(items["$ref"], field_path)
            self.schemas_stack.pop()
        if items.get("properties"):
            self.parse_schema(items, field_path, current_schema_name)

    def parse_all_one_of(
        self, schemas: list[dict[str, Any]]
    ) -> tuple[dict[str, dict[str, Any]], int]:
        result_schema: dict[str, dict[str, Any]] = {}
        schemas_added_to_stack = 0
        for schema in schemas:
            if schema.get("$ref", ""):
                schema_name, schema = self.get_schema_by_ref(schema.get("$ref", ""))
                self.schemas_stack.append(schema_name)
                schemas_added_to_stack += 1
            if schema.get("properties", {}):
                result_schema["properties"] = {
                    **result_schema.get("properties", {}),
                    **schema["properties"],
                }
            else:
                result_schema = {**result_schema, **schema}
        return result_schema, schemas_added_to_stack

    def parse_properties(
        self,
        properties: dict,
        base_path: str = "",
        current_schema_name: str = "",
    ) -> None:
        for column_name, column_props in properties.items():
            field_path = f"{base_path}.{column_name}" if base_path else column_name
            if schema_with_of := (
                column_props.get("allOf", []) or column_props.get("oneOf", [])
            ):
                schema, schemas_in_stack = self.parse_all_one_of(schema_with_of)
                self.parse_schema(schema, field_path, current_schema_name)
                if schemas_in_stack > 0:
                    self.schemas_stack = self.schemas_stack[:-schemas_in_stack]
                continue
            else:
                column_type = column_props.get("type", UNKNOWN_TYPE)

            if column_props.get("$ref"):
                self.schemas_stack.append(current_schema_name)
                self.parse_nested_schema(
                    column_props["$ref"],
                    field_path,
                )
                self.schemas_stack.pop()
            elif column_type == OBJECT_TYPE and column_props.get("properties"):
                self.parse_schema(column_props, field_path, current_schema_name)
            elif column_type == ARRAY_TYPE:
                self.parse_array_type(
                    column_props.get("items", ""),
                    field_path,
                    column_props.get("description", ""),
                    current_schema_name,
                )
            else:
                if column_type == UNKNOWN_TYPE:
                    logger.warning(
                        f"Unknown type \"{column_props.get('type')}\" for field --- {field_path}"
                    )
                field = SchemaField(
                    fieldPath=field_path,
                    type=SchemaFieldDataTypeClass(
                        type=TYPES_MAPPING.get(column_type, NullTypeClass)()
                    ),
                    nativeDataType=repr(column_type),
                    description=column_props.get("description", ""),
                    recursive=False,
                )
                self.canonical_schema.append(field)

    def parse_ref(self, ref: str, base_path: str, current_schema_name: str) -> None:
        if not ref:
            return
        self.schemas_stack.append(current_schema_name)
        self.parse_nested_schema(ref, base_path)
        self.schemas_stack.pop()

    def parse_flatten_schema(self, schema_to_parse: dict, field_path: str) -> None:
        field_type = schema_to_parse.get("type", "")
        if field_type in (ARRAY_TYPE, OBJECT_TYPE, "") or schema_to_parse.get(
            "properties", ""
        ):
            return
        field = SchemaField(
            fieldPath=field_path,
            type=SchemaFieldDataTypeClass(
                type=TYPES_MAPPING.get(field_type, NullTypeClass)()
            ),
            nativeDataType=repr(field_type),
            description=schema_to_parse.get("description", ""),
            recursive=False,
        )
        self.canonical_schema.append(field)

    def parse_schema(
        self,
        schema_to_parse: dict,
        base_path: str = "",
        current_schema_name: str = "",
    ) -> None:
        self.parse_ref(schema_to_parse.get("$ref", ""), base_path, current_schema_name)
        self.parse_properties(
            schema_to_parse.get("properties", {}), base_path, current_schema_name
        )
        if schema_to_parse.get("type") == ARRAY_TYPE or schema_to_parse.get("items"):
            self.parse_array_type(
                schema_to_parse["items"],
                base_path,
                schema_to_parse.get("description", ""),
                current_schema_name,
            )
        if schema_with_of := (
            schema_to_parse.get("allOf", []) or schema_to_parse.get("oneOf", [])
        ):
            schema, number_of_schemas_in_stack = self.parse_all_one_of(schema_with_of)
            self.parse_schema(schema, base_path, current_schema_name)
            if number_of_schemas_in_stack > 0:
                self.schemas_stack = self.schemas_stack[:-number_of_schemas_in_stack]
        self.parse_flatten_schema(schema_to_parse, base_path)

    def extract_metadata(self) -> Optional[SchemaMetadata]:
        self.parse_schema(self.endpoint_schema)
        if self.canonical_schema:
            if (num_fields := len(self.canonical_schema)) > 1000:
                logger.warning(
                    f"Dataset {self.dataset_name} contains {num_fields} fields"
                )

            schema_metadata = SchemaMetadata(
                schemaName=self.dataset_name,
                platform=f"urn:li:dataPlatform:{self.platform}",
                version=0,
                hash="",
                platformSchema=OtherSchemaClass(rawSchema=""),
                fields=self.canonical_schema,
            )
            return schema_metadata
        return None

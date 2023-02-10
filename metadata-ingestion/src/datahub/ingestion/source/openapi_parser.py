import json
import re
import warnings
from gc import freeze
from typing import Any, Dict, Generator, List, Tuple, Iterable, Optional, Type, Union, ValuesView

import requests
import yaml
from requests.auth import HTTPBasicAuth

from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    OtherSchemaClass,
    SchemaField,
    SchemaMetadata, SchemaFieldDataType,
)
from datahub.metadata.schema_classes import SchemaFieldDataTypeClass, StringTypeClass, NullTypeClass, BooleanTypeClass, \
    ArrayTypeClass, NumberTypeClass, RecordTypeClass, UnionTypeClass, EnumTypeClass

_field_type_mapping: Dict[str, Type] = {
    list: ArrayTypeClass,
    bool: BooleanTypeClass,
    type(None): NullTypeClass,
    int: NumberTypeClass,
    float: NumberTypeClass,
    str: StringTypeClass,
    dict: RecordTypeClass,
    "enum": EnumTypeClass,
    "string": StringTypeClass,
    "String": StringTypeClass,
    "record": RecordTypeClass,
    "integer": NumberTypeClass,
}


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
    fl_l = list(flatten(d))
    return [d[1:] if d[0] == "-" else d for d in fl_l]


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


def get_url_basepath(sw_dict: dict) -> str:
    try:
        return sw_dict["basePath"]
    except KeyError:  # no base path defined
        return ""


def check_sw_version(sw_dict: dict) -> None:
    if "swagger" in sw_dict:
        v_split = sw_dict["swagger"].split(".")
    else:
        v_split = sw_dict["openapi"].split(".")

    version = [int(v) for v in v_split]

    if version[0] == 3 and version[1] > 0:
        raise NotImplementedError(
            "This plugin is not compatible with Swagger version >3.0"
        )


def get_endpoints(sw_dict: dict, get_operations_only: bool) -> dict:  # noqa: C901
    """
    Get all the URLs accepting the "GET", "POST", "PUT" and "PATCH" methods, together with their description and the tags
    """
    url_details = {}

    check_sw_version(sw_dict)

    for p_k, p_o in sw_dict["paths"].items():
        # will track only the "get" methods, which are the ones that give us data
        if "get" in p_o.keys():
            if "200" in p_o["get"]["responses"].keys():
                base_res = p_o["get"]["responses"]["200"]
            elif 200 in p_o["get"]["responses"].keys():
                # if you read a plain yml file the 200 will be an integer
                base_res = p_o["get"]["responses"][200]
            else:
                # the endpoint does not have a 200 response
                continue

            if "description" in p_o["get"].keys():
                desc = p_o["get"]["description"]
            elif "summary" in p_o["get"].keys():
                desc = p_o["get"]["summary"]
            else:  # still testing
                desc = ""

            try:
                tags = p_o["get"]["tags"]
            except KeyError:
                tags = []

            tags.append("get")

            try:
                operation_id = p_o["get"]["operationId"]
            except KeyError:
                operation_id = []

            url_details[p_k] = {"description": desc, "tags": tags, "operationId": operation_id}

            # trying if dataset is defined in swagger...
            if "content" in base_res.keys():
                res_cont = base_res["content"]
                if "application/json" in res_cont.keys():
                    ex_field = None
                    if "example" in res_cont["application/json"]:
                        ex_field = "example"
                    elif "examples" in res_cont["application/json"]:
                        ex_field = "examples"
                    elif "schema" in res_cont["application/json"]:
                        ex_field = "schema"

                    if ex_field:
                        if isinstance(res_cont["application/json"][ex_field], dict):
                            url_details[p_k]["data"] = res_cont["application/json"][
                                ex_field
                            ]
                        elif isinstance(res_cont["application/json"][ex_field], list):
                            # taking the first example
                            url_details[p_k]["data"] = res_cont["application/json"][
                                ex_field
                            ][0]
                    else:
                        warnings.warn(
                            f"Field in swagger file does not give consistent data --- {p_k}"
                        )
                elif "text/csv" in res_cont.keys():
                    url_details[p_k]["data"] = res_cont["text/csv"]["schema"]
            elif "examples" in base_res.keys():
                url_details[p_k]["data"] = base_res["examples"]["application/json"]

            # checking whether there are defined parameters to execute the call...
            if "parameters" in p_o["get"].keys():
                url_details[p_k]["parameters"] = p_o["get"]["parameters"]

        # will track only the "post" methods
        elif "post" in p_o.keys():
            if get_operations_only:
                continue

            if "200" in p_o["post"]["responses"].keys():
                base_res = p_o["post"]["responses"]["200"]
            elif 200 in p_o["post"]["responses"].keys():
                # if you read a plain yml file the 200 will be an integer
                base_res = p_o["post"]["responses"][200]
            else:
                # the endpoint does not have a 200 response
                continue

            if "description" in p_o["post"].keys():
                desc = p_o["post"]["description"]
            elif "summary" in p_o["post"].keys():
                desc = p_o["post"]["summary"]
            else:  # still testing
                desc = ""

            try:
                tags = p_o["post"]["tags"]
            except KeyError:
                tags = []

            tags.append("post")

            try:
                operation_id = p_o["post"]["operationId"]
            except KeyError:
                operation_id = []

            url_details[p_k + "__post_request"] = {"description": desc, "tags": tags, "operationId": operation_id}

            # trying if dataset is defined in swagger...
            if "content" in base_res.keys():
                res_cont = base_res["content"]
                if "application/json" in res_cont.keys():
                    ex_field = None
                    if "example" in res_cont["application/json"]:
                        ex_field = "example"
                    elif "examples" in res_cont["application/json"]:
                        ex_field = "examples"

                    if ex_field:
                        if isinstance(res_cont["application/json"][ex_field], dict):
                            url_details[p_k + "__post_request"]["data"] = res_cont["application/json"][
                                ex_field
                            ]
                        elif isinstance(res_cont["application/json"][ex_field], list):
                            # taking the first example
                            url_details[p_k + "__post_request"]["data"] = res_cont["application/json"][
                                ex_field
                            ][0]
                    else:
                        warnings.warn(
                            f"Field in swagger file does not give consistent data --- {p_k}"
                        )
                elif "text/csv" in res_cont.keys():
                    url_details[p_k + "__post_request"]["data"] = res_cont["text/csv"]["schema"]
            elif "examples" in base_res.keys():
                url_details[p_k + "__post_request"]["data"] = base_res["examples"]["application/json"]

            # checking whether there are defined parameters to execute the call...
            if "parameters" in p_o["post"].keys():
                url_details[p_k + "__post_request"]["parameters"] = p_o["post"]["parameters"]

            if "requestBody" in p_o["post"].keys():
                res_cont = p_o["post"]["requestBody"]["content"]
                if "application/cloudevents+json" in res_cont.keys():
                    ex_field = None
                    if "schema" in res_cont["application/cloudevents+json"]:
                        ex_field = "schema"

                    if ex_field:
                        if isinstance(res_cont["application/cloudevents+json"][ex_field], dict):
                            url_details[p_k + "__post_request"]["data"] = res_cont["application/cloudevents+json"][
                                ex_field
                            ]
                        elif isinstance(res_cont["application/cloudevents+json"][ex_field], list):
                            # taking the first example
                            url_details[p_k + "__post_request"]["data"] = res_cont["application/cloudevents+json"][
                                ex_field
                            ][0]
                    else:
                        warnings.warn(
                            f"Field in swagger file does not give consistent data --- {p_k}"
                        )
                elif "application/json" in res_cont.keys():
                    ex_field = None
                    if "schema" in res_cont["application/json"]:
                        ex_field = "schema"

                    if ex_field:
                        if isinstance(res_cont["application/json"][ex_field], dict):
                            url_details[p_k + "__post_request"]["data"] = res_cont["application/json"][
                                ex_field
                            ]
                        elif isinstance(res_cont["application/json"][ex_field], list):
                            # taking the first example
                            url_details[p_k + "__post_request"]["data"] = res_cont["application/json"][
                                ex_field
                            ][0]
                    else:
                        warnings.warn(
                            f"Field in swagger file does not give consistent data --- {p_k}"
                        )

        # will track only the "put" methods
        elif "put" in p_o.keys():
            if get_operations_only:
                continue

            if "200" in p_o["put"]["responses"].keys():
                base_res = p_o["put"]["responses"]["200"]
            elif 200 in p_o["put"]["responses"].keys():
                # if you read a plain yml file the 200 will be an integer
                base_res = p_o["put"]["responses"][200]
            else:
                # the endpoint does not have a 200 response
                continue

            if "description" in p_o["put"].keys():
                desc = p_o["put"]["description"]
            elif "summary" in p_o["put"].keys():
                desc = p_o["put"]["summary"]
            else:  # still testing
                desc = ""

            try:
                tags = p_o["put"]["tags"]
            except KeyError:
                tags = []

            tags.append("put")

            try:
                operation_id = p_o["put"]["operationId"]
            except KeyError:
                operation_id = []

            url_details[p_k + "__put_response"] = {"description": desc, "tags": tags, "operationId": operation_id}
            url_details[p_k + "__put_request"] = {"description": desc, "tags": tags, "operationId": operation_id}

            # trying if dataset is defined in swagger...
            if "content" in base_res.keys():
                res_cont = base_res["content"]
                if "application/json" in res_cont.keys():
                    ex_field = None
                    if "example" in res_cont["application/json"]:
                        ex_field = "example"
                    elif "examples" in res_cont["application/json"]:
                        ex_field = "examples"
                    elif "schema" in res_cont["application/json"]:
                        ex_field = "schema"

                    if ex_field:
                        if isinstance(res_cont["application/json"][ex_field], dict):
                            url_details[p_k + "__put_response"]["data"] = res_cont["application/json"][
                                ex_field
                            ]
                        elif isinstance(res_cont["application/json"][ex_field], list):
                            # taking the first example
                            url_details[p_k + "__put_response"]["data"] = res_cont["application/json"][
                                ex_field
                            ][0]
                    else:
                        warnings.warn(
                            f"Field in swagger file does not give consistent data --- {p_k}"
                        )
                elif "text/csv" in res_cont.keys():
                    url_details[p_k + "__put_response"]["data"] = res_cont["text/csv"]["schema"]
            elif "examples" in base_res.keys():
                url_details[p_k + "__put_response"]["data"] = base_res["examples"]["application/json"]

            # checking whether there are defined parameters to execute the call...
            if "parameters" in p_o["put"].keys():
                url_details[p_k + "__put_response"]["parameters"] = p_o["put"]["parameters"]
                url_details[p_k + "__put_request"]["parameters"] = p_o["put"]["parameters"]

            if "requestBody" in p_o["put"].keys():
                res_cont = p_o["put"]["requestBody"]["content"]
                if "application/cloudevents+json" in res_cont.keys():
                    ex_field = None
                    if "schema" in res_cont["application/cloudevents+json"]:
                        ex_field = "schema"

                    if ex_field:
                        if isinstance(res_cont["application/cloudevents+json"][ex_field], dict):
                            url_details[p_k + "__put_request"]["data"] = res_cont["application/cloudevents+json"][
                                ex_field
                            ]
                        elif isinstance(res_cont["application/cloudevents+json"][ex_field], list):
                            # taking the first example
                            url_details[p_k + "__put_request"]["data"] = res_cont["application/cloudevents+json"][
                                ex_field
                            ][0]
                    else:
                        warnings.warn(
                            f"Field in swagger file does not give consistent data --- {p_k}"
                        )
                elif "application/json" in res_cont.keys():
                    ex_field = None
                    if "schema" in res_cont["application/json"]:
                        ex_field = "schema"

                    if ex_field:
                        if isinstance(res_cont["application/json"][ex_field], dict):
                            url_details[p_k + "__put_request"]["data"] = res_cont["application/json"][
                                ex_field
                            ]
                        elif isinstance(res_cont["application/json"][ex_field], list):
                            # taking the first example
                            url_details[p_k + "__put_request"]["data"] = res_cont["application/json"][
                                ex_field
                            ][0]
                    else:
                        warnings.warn(
                            f"Field in swagger file does not give consistent data --- {p_k}"
                        )

        # will track only the "patch" methods
        elif "patch" in p_o.keys():
            if get_operations_only:
                continue

            if "200" in p_o["patch"]["responses"].keys():
                base_res = p_o["patch"]["responses"]["200"]
            elif 200 in p_o["patch"]["responses"].keys():
                # if you read a plain yml file the 200 will be an integer
                base_res = p_o["patch"]["responses"][200]
            else:
                # the endpoint does not have a 200 response
                continue

            if "description" in p_o["patch"].keys():
                desc = p_o["patch"]["description"]
            elif "summary" in p_o["patch"].keys():
                desc = p_o["patch"]["summary"]
            else:  # still testing
                desc = ""

            try:
                tags = p_o["patch"]["tags"]
            except KeyError:
                tags = []

            tags.append("patch")

            try:
                operation_id = p_o["patch"]["operationId"]
            except KeyError:
                operation_id = []

            url_details[p_k + "__patch_request"] = {"description": desc, "tags": tags, "operationId": operation_id}

            # trying if dataset is defined in swagger...
            if "content" in base_res.keys():
                res_cont = base_res["content"]
                if "application/json" in res_cont.keys():
                    ex_field = None
                    if "example" in res_cont["application/json"]:
                        ex_field = "example"
                    elif "examples" in res_cont["application/json"]:
                        ex_field = "examples"

                    if ex_field:
                        if isinstance(res_cont["application/json"][ex_field], dict):
                            url_details[p_k + "__patch_request"]["data"] = res_cont["application/json"][
                                ex_field
                            ]
                        elif isinstance(res_cont["application/json"][ex_field], list):
                            # taking the first example
                            url_details[p_k + "__patch_request"]["data"] = res_cont["application/json"][
                                ex_field
                            ][0]
                    else:
                        warnings.warn(
                            f"Field in swagger file does not give consistent data --- {p_k}"
                        )
                elif "text/csv" in res_cont.keys():
                    url_details[p_k + "__patch_request"]["data"] = res_cont["text/csv"]["schema"]
            elif "examples" in base_res.keys():
                url_details[p_k + "__patch_request"]["data"] = base_res["examples"]["application/json"]

            # checking whether there are defined parameters to execute the call...
            if "parameters" in p_o["patch"].keys():
                url_details[p_k + "__patch_request"]["parameters"] = p_o["patch"]["parameters"]

            if "requestBody" in p_o["patch"].keys():
                res_cont = p_o["patch"]["requestBody"]["content"]
                if "application/cloudevents+json" in res_cont.keys():
                    ex_field = None
                    if "schema" in res_cont["application/cloudevents+json"]:
                        ex_field = "schema"

                    if ex_field:
                        if isinstance(res_cont["application/cloudevents+json"][ex_field], dict):
                            url_details[p_k + "__patch_request"]["data"] = res_cont["application/cloudevents+json"][
                                ex_field
                            ]
                        elif isinstance(res_cont["application/cloudevents+json"][ex_field], list):
                            # taking the first example
                            url_details[p_k + "__patch_request"]["data"] = res_cont["application/cloudevents+json"][
                                ex_field
                            ][0]
                    else:
                        warnings.warn(
                            f"Field in swagger file does not give consistent data --- {p_k}"
                        )
                elif "application/json" in res_cont.keys():
                    ex_field = None
                    if "schema" in res_cont["application/json"]:
                        ex_field = "schema"

                    if ex_field:
                        if isinstance(res_cont["application/json"][ex_field], dict):
                            url_details[p_k + "__patch_request"]["data"] = res_cont["application/json"][
                                ex_field
                            ]
                        elif isinstance(res_cont["application/json"][ex_field], list):
                            # taking the first example
                            url_details[p_k + "__patch_request"]["data"] = res_cont["application/json"][
                                ex_field
                            ][0]
                    else:
                        warnings.warn(
                            f"Field in swagger file does not give consistent data --- {p_k}"
                        )

    return dict(sorted(url_details.items()))


def get_schemas(sc_dict: dict) -> dict:
    schema_details = {}

    if "components" in sc_dict:
        for p_k, p_o in sc_dict["components"]["schemas"].items():
            schema_details[p_k] = {}

            try:
                required_fields = p_o["required"]
            except KeyError:
                required_fields = []

            try:
                schema_fields = p_o["properties"]
            except KeyError:
                schema_fields = []

            try:
                enum_fields = p_o["enum"]
            except KeyError:
                enum_fields = []

            try:
                description_fields = p_o["description"]
            except KeyError:
                description_fields = []

            schema_details[p_k]["detail"] = {"required": required_fields, "properties": schema_fields,
                                             "enum": enum_fields, "description": description_fields}
    else:
        schema_details["empty_schema"] = {}
        schema_details["empty_schema"]["detail"] = {"required": "", "properties": "",
                                         "enum": "", "description": "Swagger file doesn't contains a components path"}

    return dict(schema_details.items())


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
        if clean_name in examples[ex2use].keys():
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
    splitted = re.split(r"\{[^}]+\}", raw_url)
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
    dets = re.findall(r"(\{[^}]+\})", url)  # searching the fields between parenthesis
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
        warnings.warn(f"Empty data --- {dataset_name}")
        return [], {}
    elif isinstance(dict_data, list):
        # it's maybe just a list
        if len(dict_data) == 0:
            warnings.warn(f"Empty data --- {dataset_name}")
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
    if len(dict_data.keys()) > 1:
        # the elements are directly inside the dict
        return flatten2list(dict_data), dict_data
    dst_key = list(dict_data.keys())[
        0
    ]  # the first and unique key is the dataset's name

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
            warnings.warn(f"Unable to get the attributes --- {dataset_name}")
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


def get_field_type(
        field_type: Union[Type, str]
) -> SchemaFieldDataType:

    TypeClass: Optional[Type] = _field_type_mapping.get(field_type)

    if TypeClass is None:
        TypeClass = NullTypeClass

    return SchemaFieldDataType(type=TypeClass())


def set_metadata(
    dataset_name: str, fields: List, operation_id: str, schemas_details: dict, platform: str = "api"
) -> SchemaMetadata:
    canonical_schema: List[SchemaField] = []

    for column in fields:
        if column == "type":
            continue

        elif column == "items":
            json_str = fields["items"]
            for key, value in json_str.items():
                if key == "$ref":
                    column_schema = value.rsplit('/', 1)[1]
                    if column_schema in schemas_details:
                        schema = schemas_details[column_schema]["detail"]["properties"]
                        schema_enum = schemas_details[column_schema]["detail"]["enum"]
                        schema_description = schemas_details[column_schema]["detail"]["description"]
                        if len(schema_description) == 0:
                            schema_description = ""
                        if len(schema) == 0 and len(schema_enum) > 0:
                            field = SchemaField(
                                fieldPath=column_schema,
                                nativeDataType=column_schema,
                                type=SchemaFieldDataTypeClass(type=EnumTypeClass()),
                                description=schema_description,
                                recursive=False,
                            )
                            canonical_schema.append(field)
                        elif len(schema) == 0 and len(schema_enum) == 0:
                            field = SchemaField(
                                fieldPath=column_schema,
                                nativeDataType="String",
                                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                                description=schema_description,
                                recursive=False,
                            )
                            canonical_schema.append(field)
                        else:
                            for f_k, f_v in schema.items():
                                field_name = f_k
                                try:
                                    description = f_v["description"]
                                except KeyError:
                                    description = ""

                                try:
                                    format = f_v["format"]
                                except KeyError:
                                    format = None

                                try:
                                    field_type = f_v["type"]
                                except KeyError:
                                    try:
                                        field_type = f_v["$ref"]
                                        ft_name = field_type.rsplit('/', 1)[1]
                                        canonical_schema = add_subschema(ft_name, schemas_details, canonical_schema, False, None, field_name)
                                        continue
                                    except KeyError:
                                        try:
                                            field_type = f_v["oneOf"]
                                            if len(field_type) > 0:
                                                item = field_type[0]
                                                ft = item["$ref"]
                                                s_name = ft.rsplit('/', 1)[1]
                                                canonical_schema = add_subschema(s_name, schemas_details, canonical_schema, False, None, field_name)
                                                continue
                                        except KeyError:
                                            try:
                                                field_type = f_v["allOf"]
                                                if len(field_type) > 0:
                                                    item = field_type[0]
                                                    ft = item["$ref"]
                                                    s_name = ft.rsplit('/', 1)[1]
                                                    canonical_schema = add_subschema(s_name, schemas_details, canonical_schema, False, None, field_name)
                                                    continue
                                            except KeyError:
                                                field_type = "String"
                                if format is None:
                                    format = field_type
                                field = SchemaField(
                                    fieldPath=field_name,
                                    nativeDataType=format,
                                    type=get_field_type(field_type),
                                    description=description,
                                    recursive=False,
                                )
                                canonical_schema.append(field)
                            continue

        elif column == "$ref":
            json_str = fields["$ref"]
            column_schema = json_str.rsplit('/', 1)[1]
            canonical_schema = add_subschema(column_schema, schemas_details, canonical_schema, False, None, None)
            continue
        else:
            field = SchemaField(
                fieldPath=column,
                nativeDataType="String",
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


def add_subschema(column_schema: str, schemas_details: dict, canonical_schema: List[SchemaField], use_expanded_name: bool,
                  expanded_name: str, field_name: str):
    if column_schema in schemas_details:
        schema = schemas_details[column_schema]["detail"]["properties"]
        schema_enum = schemas_details[column_schema]["detail"]["enum"]
        schema_description = schemas_details[column_schema]["detail"]["description"]
        if len(schema_description) == 0:
            schema_description = ""
        if len(schema) == 0 and len(schema_enum) > 0:
            field = SchemaField(
                fieldPath=field_name,
                nativeDataType=column_schema,
                type=SchemaFieldDataTypeClass(type=EnumTypeClass()),
                description=schema_description,
                recursive=False,
            )
            canonical_schema.append(field)
        elif len(schema) == 0 and len(schema_enum) == 0:
            field = SchemaField(
                fieldPath=column_schema,
                nativeDataType="String",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                description=schema_description,
                recursive=False,
            )
            canonical_schema.append(field)
        else:
            if use_expanded_name and expanded_name is not None:
                field = SchemaField(
                    fieldPath=expanded_name,
                    nativeDataType=column_schema,
                    type=SchemaFieldDataTypeClass(type=RecordTypeClass()),
                    description=schema_description,
                    recursive=False,
                )
                canonical_schema.append(field)
            for f_k, f_v in schema.items():
                field_name = f_k
                try:
                    description = f_v["description"]
                except KeyError:
                    description = ""

                try:
                    format = f_v["format"]
                except KeyError:
                    format = None

                if use_expanded_name and expanded_name is not None:
                    field_name = expanded_name + "." + field_name

                try:
                    field_type = f_v["type"]
                except KeyError:
                    try:
                        field_type = f_v["$ref"]
                        subcolumn_schema = field_type.rsplit('/', 1)[1]
                        if subcolumn_schema in schemas_details:
                            subschema = schemas_details[subcolumn_schema]["detail"]["properties"]
                            subschema_enum = schemas_details[subcolumn_schema]["detail"]["enum"]
                        else:
                            subschema = subcolumn_schema
                        if len(subschema_enum) == 0:
                            field = SchemaField(
                                fieldPath=field_name,
                                nativeDataType=subcolumn_schema,
                                type=SchemaFieldDataTypeClass(type=RecordTypeClass()),
                                description=description,
                                recursive=False,
                            )
                            canonical_schema.append(field)

                        if len(subschema) == 0 and len(subschema_enum) > 0:
                            field = SchemaField(
                                # fieldPath=field_name + '.' + subcolumn_schema,
                                # nativeDataType="Enum",
                                fieldPath=field_name,
                                nativeDataType=subcolumn_schema,
                                type=SchemaFieldDataTypeClass(type=EnumTypeClass()),
                                description=description,
                                recursive=False,
                            )
                            canonical_schema.append(field)
                        elif len(subschema) == 0 and len(subschema_enum) == 0:
                            field = SchemaField(
                                fieldPath=field_name + '.' + subcolumn_schema,
                                nativeDataType="String",
                                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                                description=description,
                                recursive=False,
                            )
                            canonical_schema.append(field)
                        else:
                            for s_k, s_o in subschema.items():
                                subschema_name = s_k
                                try:
                                    description = s_o["description"]
                                except KeyError:
                                    description = ""

                                try:
                                    subschema_format = s_o["format"]
                                except KeyError:
                                    subschema_format = None

                                try:
                                    subschema_type = s_o["type"]
                                    subschema_type_control_ref = s_o["items"]["$ref"]
                                    if len(subschema_type_control_ref) > 0:
                                        ref_schema = subschema_type_control_ref.rsplit('/', 1)[1]
                                        canonical_schema = add_next_subschema(ref_schema, schemas_details, canonical_schema, field_name, subschema_name)
                                        continue
                                except KeyError:
                                    try:
                                        subschema_type = s_o["$ref"]
                                        sub_schema = subschema_type.rsplit('/', 1)[1]
                                        canonical_schema = add_next_subschema(sub_schema, schemas_details, canonical_schema, field_name, subschema_name)
                                        continue

                                    except KeyError:
                                        try:
                                            subschema_type = s_o["oneOf"]
                                            if len(subschema_type) > 0:
                                                item = subschema_type[0]
                                                ft = item["$ref"]
                                                s_name = ft.rsplit('/', 1)[1]
                                                canonical_schema = add_next_subschema(s_name, schemas_details, canonical_schema, field_name, subschema_name)
                                            continue
                                        except KeyError:
                                            try:
                                                subschema_type = s_o["allOf"]
                                                if len(subschema_type) > 0:
                                                    item = subschema_type[0]
                                                    ft = item["$ref"]
                                                    s_name = ft.rsplit('/', 1)[1]
                                                    canonical_schema = add_next_subschema(s_name, schemas_details, canonical_schema, field_name, subschema_name)
                                                continue
                                            except KeyError:
                                                subschema_type = "String"
                                if subschema_format is None:
                                    subschema_format = subschema_type
                                field = SchemaField(
                                    fieldPath=field_name + '.' + subschema_name,
                                    nativeDataType=subschema_format,
                                    type=get_field_type(subschema_type),
                                    description=description,
                                    recursive=False,
                                )
                                canonical_schema.append(field)

                        continue
                    except KeyError:
                        try:
                            field_type = f_v["oneOf"]
                            if len(field_type) > 0:
                                item = field_type[0]
                                ft = item["$ref"]
                                s_name = ft.rsplit('/', 1)[1]
                                canonical_schema = add_subschema(s_name, schemas_details, canonical_schema, True, field_name, field_name)
                            continue
                        except KeyError:
                            try:
                                field_type = f_v["allOf"]
                                if len(field_type) > 0:
                                    item = field_type[0]
                                    ft = item["$ref"]
                                    s_name = ft.rsplit('/', 1)[1]
                                    canonical_schema = add_subschema(s_name, schemas_details, canonical_schema, True, field_name, field_name)
                                continue
                            except KeyError:
                                field_type = "String"
                if format is None:
                    format = field_type
                field = SchemaField(
                    fieldPath=field_name,
                    nativeDataType=format,
                    type=get_field_type(field_type),
                    description=description,
                    recursive=False,
                )
                canonical_schema.append(field)
    return canonical_schema


def add_next_subschema(sub_schema: str, schemas_details: dict, canonical_schema: List[SchemaField], field_name: str, subschema_name: str):
    if sub_schema in schemas_details:
        s_schema = schemas_details[sub_schema]["detail"]["properties"]
        s_schema_enum = schemas_details[sub_schema]["detail"]["enum"]
        schema_description = schemas_details[sub_schema]["detail"]["description"]
        if len(schema_description) == 0:
            schema_description = ""
    else:
        s_schema = sub_schema
        schema_description = ""
    if len(s_schema_enum) == 0:
        field = SchemaField(
            fieldPath=field_name + '.' + subschema_name,
            nativeDataType=sub_schema,
            type=SchemaFieldDataTypeClass(type=RecordTypeClass()),
            description=schema_description,
            recursive=False,
        )
        canonical_schema.append(field)

    if len(s_schema) == 0 and len(s_schema_enum) > 0:
        field = SchemaField(
            # fieldPath=field_name + '.' + subschema_name + '.' + sub_schema,
            # nativeDataType="Enum",
            fieldPath=field_name + '.' + subschema_name,
            nativeDataType=sub_schema,
            type=SchemaFieldDataTypeClass(type=EnumTypeClass()),
            description=schema_description,
            recursive=False,
        )
        canonical_schema.append(field)
    elif len(s_schema) == 0 and len(s_schema_enum) == 0:
        field = SchemaField(
            fieldPath=field_name + '.' + subschema_name + '.' + sub_schema,
            nativeDataType="String",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            description=schema_description,
            recursive=False,
        )
        canonical_schema.append(field)
    else:
        for k, v in s_schema.items():
            name = k
            try:
                description = v["description"]
            except KeyError:
                description = ""

            try:
                format = v["format"]
            except KeyError:
                format = None

            try:
                type = v["type"]
                type_control_items_ref = v["items"]["$ref"]
                if len(type_control_items_ref) > 0:
                    ref_schema = type_control_items_ref.rsplit('/', 1)[1]
                    canonical_schema = add_next_subschema(ref_schema, schemas_details, canonical_schema, field_name + '.' + subschema_name,
                                                          name)
                    continue
            except KeyError:
                try:
                    type = v["$ref"]
                    schema_name = type.rsplit('/', 1)[1]
                    canonical_schema = add_next_subschema(schema_name, schemas_details, canonical_schema, field_name + '.' + subschema_name,
                                                          name)
                    continue
                except KeyError:
                    try:
                        type = v["oneOf"]
                        if len(type) > 0:
                            item = type[0]
                            ft = item["$ref"]
                            s_name = ft.rsplit('/', 1)[1]
                            canonical_schema = add_next_subschema(s_name, schemas_details, canonical_schema, field_name + '.' + subschema_name,
                                                                  name)
                            continue
                    except KeyError:
                        try:
                            type = v["allOf"]
                            if len(type) > 0:
                                item = type[0]
                                ft = item["$ref"]
                                s_name = ft.rsplit('/', 1)[1]
                                canonical_schema = add_next_subschema(s_name, schemas_details, canonical_schema, field_name + '.' + subschema_name,
                                                                      name)
                                continue
                        except KeyError:
                            type = "String"
            if format is None:
                format = type
            field = SchemaField(
                fieldPath=field_name + '.' + subschema_name + '.' + name,
                nativeDataType=format,
                type=get_field_type(type),
                description=description,
                recursive=False,
            )
            canonical_schema.append(field)

    return canonical_schema

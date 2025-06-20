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
            ex_field = None
            if "example" in res_cont["application/json"]:
                ex_field = "example"
            elif "examples" in res_cont["application/json"]:
                ex_field = "examples"

            if ex_field:
                if isinstance(res_cont["application/json"][ex_field], dict):
                    data = res_cont["application/json"][ex_field]
                elif isinstance(res_cont["application/json"][ex_field], list):
                    # taking the first example
                    data = res_cont["application/json"][ex_field][0]
            else:
                logger.warning(
                    f"Field in swagger file does not give consistent data --- {key}"
                )
        elif "text/csv" in res_cont:
            data = res_cont["text/csv"]["schema"]
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
    dataset_name: str, fields: List, platform: str = "api"
) -> SchemaMetadata:
    canonical_schema: List[SchemaField] = []
    seen_paths = set()

    # Process all flattened fields
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

        # Add the leaf field if not already seen
        if field_path not in seen_paths:
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

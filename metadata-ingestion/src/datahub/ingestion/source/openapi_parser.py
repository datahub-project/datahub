import requests
from requests.auth import HTTPBasicAuth
import yaml
import re
import warnings
from tqdm.auto import tqdm
import jinja2
import time
import json
from collections import Counter
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField, SchemaMetadata, OtherSchemaClass
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.schema_classes import StringTypeClass, SchemaFieldDataTypeClass


from typing import List, Union, Generator, Dict
from dataclasses import dataclass


# class DFieldIterator:
#     def __init__(self, ddataset):
#         # object reference
#         self._ddataset = ddataset
#         # member variable to keep track of current index
#         self._index = 0
#
#     def __next__(self):
#         if self._index < (len(self._ddataset._datafields)):
#             f_name = list(self._ddataset._datafields.keys())[self._index]
#             result = self._ddataset._datafields[f_name]
#             self._index += 1
#             return result
#         # End of Iteration
#         raise StopIteration
#
#
# @dataclass
# class DField:
#     """
#     Class containing the fields metadata belonging to a dataset,
#     in a DataHub friendly format
#     """
#     name: str = ""
#     description: str = ""
#     type: str = "str"
#     #
#     # def __init__(self, name: str, description: str = "") -> None:
#     #     self.name = name
#     #     self.description = description


def flatten(d: dict, prefix: str = "") -> Generator:
    for k, v in d.items():
        if isinstance(v, dict):
            yield from flatten(v, prefix + "." + k)
        else:
            yield (prefix + "-" + k).strip(".")


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


# class DDataset:
#     """
#     Class containing metadata informations in a DataHub friendly format
#     """
#     service_name = ""
#     dataset_name = ""
#     main_url = ""  # this will go in the "documents" aspect
#     description = ""  # this will go in the "DatasetProperties" aspect
#
#     owner = "TestUser"
#
#     def __init__(self, service_name: str, dataset_name: str, main_url: str = "") -> None:
#         self._datafields = {}
#         self._tags = list()
#         self._timestamp = int(time.time())
#
#         self.service_name = service_name
#         self.dataset_name = dataset_name.replace("/", ".")
#         if self.dataset_name[-1] == ".":
#             self.dataset_name = self.dataset_name[:-1]
#         self.main_url = main_url
#
#     def add_description(self, description: str) -> None:
#         self.description = description
#
#     def add_tags(self, tags: list) -> None:
#         clean_tags = [tag.replace(" ", "_") for tag in tags]
#         self._tags.extend(clean_tags)
#         self._tags =list(set(self._tags))  # need only unique values
#
#     def add_fields(self, fields: dict) -> None:
#         for f_k in fields:
#             self._datafields[f_k] = DField(name=f_k)  # using dict to emulate an ordered set
#
#     def __iter__(self):
#         ''' Returns the Iterator object for the _datafields'''
#         return DFieldIterator(self)


def request_call(url: str, token: str = None, username: str = None, password: str = None) :
    
    headers = {'accept': 'application/json'}

    if username is not None and password is not None:
        response = requests.get(url, headers=headers, auth=HTTPBasicAuth(username, password))
    elif token is not None:
        headers['Authorization'] = "Bearer " + token
        response = requests.get(url, headers=headers)
    else:
        response = requests.get(url, headers=headers)
    return response


def get_swag_json(url: str, token: str = None, username: str = None, password: str = None, swagger_file: str = "") :
    tot_url = url+swagger_file
    if token is not None:
        response = request_call(url=tot_url, token=token)
    else:
        response = request_call(url=tot_url, username=username, password=password)

    if response.status_code == 200:
        try: 
            dict_data = json.loads(response.content)
        except json.JSONDecodeError:  # it's not a JSON!
            dict_data = yaml.safe_load(response.content)  
        return dict_data
    else:
        raise Exception(f"Unable to retrieve {tot_url}, error {response.status_code}") 


def get_url_basepath(sw_dict: dict) -> str:
    try:
        return sw_dict["basePath"]
    except KeyError: # no base path defined
        return ""


def get_endpoints(sw_dict: dict) -> dict:
    """
    Get all the URLs accepting the "GET" method, together with their description and the tags
    """
    url_details = {}

    for p_k, p_o in sw_dict["paths"].items():
        # will track only the "get" methods, which are the ones that give us data
        if "get" in p_o.keys():

            if "description" in p_o["get"].keys():
                desc = p_o["get"]["description"]
            else:  # still testing
                desc = p_o["get"]["summary"]

            tags = p_o["get"]["tags"]
            
            url_details[p_k] = {"description":  desc, "tags": tags}

            try:
                base_res = p_o["get"]["responses"]['200']
            except KeyError:  # if you read a plain yml file the 200 will be an integer
                base_res = p_o["get"]["responses"][200]

            # trying if dataset is defined in swagger...
            if "content" in base_res.keys():
                res_cont = base_res["content"]
                if "application/json" in res_cont.keys():
                    if "example" in res_cont["application/json"]:
                        if isinstance(res_cont["application/json"]["example"], dict):
                            url_details[p_k]["data"] = res_cont["application/json"]["example"]
                        elif isinstance(res_cont["application/json"]["example"], list):
                            # taking the first example
                            url_details[p_k]["data"] = res_cont["application/json"]["example"][0]
                    else:
                        warnings.warn(f"endpoint {p_k} does not give consistent data")
                elif "text/csv" in res_cont.keys():
                    if p_k == "/api/tasks/":
                        print("asd")
                    url_details[p_k]["data"] = res_cont["text/csv"]["schema"]

            # if dataset is not defined in swagger, we have to extrapolate it.

            # checking whether there are defined parameters to execute the call... 
            if "parameters" in p_o["get"].keys():
                url_details[p_k]["parameters"] = p_o["get"]["parameters"]
            
    return url_details


def guessing_url_name(url: str, examples: dict) -> str:
    """
    given a url and dict of extracted data, we try to guess a working URL. Example:
    url2complete = "/advancedcomputersearches/name/{name}/id/{id}"
    extr_data = {"advancedcomputersearches": {'id': 202, 'name': '_unmanaged'}}
    -->> guessed_url = /advancedcomputersearches/name/_unmanaged/id/202'
    """
    if url[0] == '/':
        url2op = url[1:]  # operational url does not need the very first /
    else:
        url2op = url
    divisions = url2op.split("/")

    # the very first part of the url should stay the same.
    root = url2op.split("{")[0]  

    needed_n = [a for a in divisions if not a.find("{")]  # search for stuff like "{example}"
    cleaned_needed_n = [name[1:-1] for name in needed_n]  # no parenthesis, {example} -> example
    
    # in the cases when the parameter name is specified, we have to correct the root.
    # in the example, advancedcomputersearches/name/ -> advancedcomputersearches/
    for field in cleaned_needed_n:
        if field in root:
            div_pos = root.find(field)
            if div_pos > 0:
                root = root[:div_pos - 1]  # like "base/field" should become "base"

    if root in examples.keys():
        # if our root is contained in our samples examples...
        ex2use = root
    elif root[:-1] in examples.keys():
        ex2use = root[:-1]
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
    url_guess_id = maybe_theres_simple_id(url_guess)  # try to fill IDs with "1"s...
    return url_guess_id


def clean_url(url: str) -> str:
    protocols = ["http://", "https://"]
    for prot in protocols:
        if prot in url:
            parts = url.split(prot)
            return prot+parts[1].replace("//", "/")
    raise Exception(f"Unable to understand URL {url}")


def extract_fields(response, dataset_name: str) -> (List, Dict):
    """
    Given a URL, this function will extract the fields contained in the
    response of the call to that URL, supposing that the response is a JSON.
    
    The list in the output tuple will contain the fields name.
    The dict in the output tuple will contain a sample of data.
    """
    dict_data = json.loads(response.content)
    if isinstance(dict_data, str):
        # no sense
        warnings.warn(f"Empty data in {dataset_name}")
        return [], {}
    elif isinstance(dict_data, list):
        # it's maybe just a list
        if len(dict_data) == 0:
            warnings.warn(f"Empty data in {dataset_name}")
            return [], {}
        # so we take the fields of the first element,
        # if it's a dict
        if isinstance(dict_data[0], dict):
            return flatten2list(dict_data[0]), dict_data[0]
        elif isinstance(dict_data[0], str):
            # this is actually data
            return "contains_a_string", dict_data[0]
        else:
            raise ValueError("unknown format")  
    if len(dict_data.keys()) > 1:
        # the elements are directly inside the dict
        return flatten2list(dict_data), dict_data
    dst_key = list(dict_data.keys())[0]  # the first and unique key is the dataset's name

    try:
        return flatten2list(dict_data[dst_key]), dict_data[dst_key]
    except AttributeError:
        # if the content is a list, we should treat each element as a dataset.
        # ..but will take the keys of the first element (to be improved)
        if isinstance(dict_data[dst_key], list):
            if len(dict_data[dst_key]) > 0:
                # for elem in dict_data[dst_key]:
                #    d2r = DDataset(service_name=service_name, dataset_name=dataset_name+)
                return flatten2list(dict_data[dst_key][0]), dict_data[dst_key][0]
            else:
                return [], {}  # it's empty!
        else:
            warnings.warn("Unable to get the attributes of {dataset_name}")
            return [], {}


def get_tok(url: str, username: str = "", password: str = "") -> Union[str, None]:
    """
    Trying to post username/password to get auth.
    Simplified version: it expect a POST at api/authenticate
    """
    data = {"username": username, "password": password}
    url2post = url+"api/authenticate/"
    response = requests.post(url2post, data=data)
    if response.status_code == 200:
        cont = json.loads(response.content)
        return cont['tokens']['access']
    else:
        return None


def set_metadata(dataset_name: str, fields: List, platform: str = "api") -> SchemaMetadata:
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

    actor = "urn:li:corpuser:etl"
    sys_time = int(time.time() * 1000)
    schema_metadata = SchemaMetadata(
        schemaName=dataset_name,
        platform=f"urn:li:dataPlatform:{platform}",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        created=AuditStamp(time=sys_time, actor=actor),
        lastModified=AuditStamp(time=sys_time, actor=actor),
        fields=canonical_schema,
    )
    return schema_metadata



# def launch_mce(service_name: str, dataset_name: str, endpoint_dets: dict, ddata: DDataset):
#     dataset_snapshot = DatasetSnapshot(
#         urn=f"urn:li:dataset:(urn:li:dataPlatform:api,{service_name}.{dataset_name},PROD)",
#         aspects=[],
#     )
#     dataset_properties = DatasetPropertiesClass(
#         description=endpoint_dets["description"],
#         customProperties={},
#         tags=endpoint_dets["tags"]
#     )
#     dataset_snapshot.aspects.append(dataset_properties)
#
#     schema_metadata = set_metadata(dataset_name, ddata)
#     dataset_snapshot.aspects.append(schema_metadata)
#
#     mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
#     wu = ApiWorkUnit(id=dataset_name, mce=mce)


# def extract_datasets(url: str, get_token: bool = False, service_name: str = "", username: str = "",
#                      swagger_file: str = "", password: str = "", forced_examples: dict = None,
#                      ignore_endps: list = None) -> dict:
#     if forced_examples is None:
#         forced_examples = {}
#     if ignore_endps is None:
#         ignore_endps = []
#     emitted_w = Counter()  # taking trace of the emitted warnings
#
#     if get_token:  # token based authentication, to be tested
#         token = get_tok(url=url, username=username, password=password)
#
#         sw_dict = get_swag_json(url, token=token, swagger_file=swagger_file)  # load the swagger file
#     else:
#         sw_dict = get_swag_json(url, username=username, password=password, swagger_file=swagger_file)
#
#     url_basepath = get_url_basepath(sw_dict)
#
#     # Getting all the URLs accepting the "GET" method,
#     # together with their description and the tags
#     url_endpoints = get_endpoints(sw_dict)
#
#     # here we put a sample from the listing of urls. To be used for later guessing of comosed urls.
#     root_dataset_samples = {}
#     datasets = {}
#
#     d_datasets = []  # the list of datasets to be delivered
#
#     # looping on all the urls. Each URL will be a dataset, for our definition
#     for endpoint_k, endpoint_dets in tqdm(url_endpoints.items(), desc="Checking urls..."):
#
#         dataset_name = endpoint_k[1:]
#
#         if dataset_name in ignore_endps:
#             continue
#
#         ddata = DDataset(service_name=service_name,
#                          dataset_name=dataset_name, main_url=url+url_basepath+dataset_name)
#         ddata.add_description(endpoint_dets["description"])
#         ddata.add_tags(endpoint_dets["tags"])
#
#         if "data" in endpoint_dets.keys():
#             # we are lucky! data is defined in the swagger for this endpoint
#             ddata.add_fields(flatten2list(endpoint_dets["data"]))
#             d_datasets.append(ddata)
#         # elif "parameters" in endpoint_dets.keys():
#         #     # half of a luck: we have explicitely declared parameters
#         #     enp_ex_pars = ""
#         #     for param_def in endpoint_dets["parameters"]:
#         #         enp_ex_pars += ""
#         elif "{" not in endpoint_k:  # if the API does not explicitely require parameters
#             tot_url = clean_url(url + url_basepath + endpoint_k)
#             if get_token:
#                 # to be implemented
#                 # response = request_call(tot_url, token=token)
#                 raise Exception("You should implement this!")
#             else:
#                 response = request_call(tot_url, username=username, password=password)
#             if response.status_code == 200:
#                 datasets[dataset_name], root_dataset_samples[dataset_name] = extract_fields(response, dataset_name)
#                 if not datasets[dataset_name]:
#                     emitted_w.update({"No Fields": 1})
#                 ddata.add_fields(datasets[dataset_name])
#                 d_datasets.append(ddata)
#             elif response.status_code == 504:
#                 warnings.warn(f"Timeout for reaching {tot_url}")
#                 emitted_w.update({"Timeout": 1})
#             elif response.status_code == 500:
#                 warnings.warn(f"Server error for {tot_url}")
#                 emitted_w.update({"Server error": 1})
#             elif response.status_code == 400:
#                 warnings.warn(f"Unknown error for {tot_url}")
#                 emitted_w.update({"Unknown Eroor": 1})
#             else:
#                 raise Exception(f"Unable to retrieve {tot_url}, response code {response.status_code}")
#         else:
#             if endpoint_k not in forced_examples.keys():
#                 # start guessing...
#                 url_guess = try_guessing(endpoint_k, root_dataset_samples)  # try to guess informations
#                 tot_url = clean_url(url + url_basepath + url_guess)
#                 if get_token:
#                     # to be implemented
#                     # response = request_call(tot_url, token=token)
#                     raise Exception("You should implement this!")
#                 else:
#                     response = request_call(tot_url, username=username, password=password)
#                 if response.status_code == 200:
#                     datasets[dataset_name], _ = extract_fields(response, dataset_name)
#                     if not datasets[dataset_name]:
#                         emitted_w.update({"No Fields": 1})
#                     ddata.add_fields(datasets[dataset_name])
#                     d_datasets.append(ddata)
#                 elif response.status_code == 403:
#                     warnings.warn(f"Not authorised to get {endpoint_k}.")
#                     emitted_w.update({"Not authorised": 1})
#                 else:
#                     warnings.warn(f"Unable to find an example for {endpoint_k}"
#                                   f" in. Please add it to the list of forced examples.")
#                     emitted_w.update({"No example": 1})
#             else:
#                 composed_url = compose_url_attr(raw_url=endpoint_k, attr_list=forced_examples[endpoint_k])
#                 tot_url = clean_url(url + url_basepath + composed_url)
#                 if get_token:
#                     # to be implemented
#                     # response = request_call(tot_url, token=token)
#                     raise Exception("You should implement this!")
#                 else:
#                     response = request_call(tot_url, username=username, password=password)
#                 if response.status_code == 200:
#                     datasets[dataset_name], _ = extract_fields(response, dataset_name)
#                     if not datasets[dataset_name]:
#                         emitted_w.update({"No Fields": 1})
#                     ddata.add_fields(datasets[dataset_name])
#                     d_datasets.append(ddata)
#                 elif response.status_code == 403:
#                     warnings.warn(f"Not authorised to get {endpoint_k}.")
#                     emitted_w.update({"Not authorised": 1})
#
#         # launch_mce(service_name, dataset_name, endpoint_dets, ddata)
#
#     # loading the template
#     template_loader = jinja2.FileSystemLoader(searchpath=".")
#     template_env = jinja2.Environment(loader=template_loader)
#     template = template_env.get_template("openapi_parser/template.j2")
#
#     rt = template.render({"datasets": d_datasets})
#     json_out = json.loads(rt)
#     print(emitted_w)
#     return json_out


# def meta_extract_datasets(source: Source) -> None:
#     print(source)
#     dict_out = extract_datasets(url=source.url, username=source.username, password=source.password,
#                                 service_name=source.name, swagger_file=source.swagger_file,
#                                 ignore_endps=source.ignore_endpoints, forced_examples=source.forced_examples)
#     with open(f"{source.name}_dataset.json", "w") as f:
#         json.dump(dict_out, f)

# flake8: noqa
import logging
from typing import Any, Dict, List, Optional, Union
import re
from pydantic import BaseModel, validator

log = logging.getLogger("ingest")
logformatter = logging.Formatter("%(asctime)s;%(levelname)s;%(funcName)s;%(message)s")
log.setLevel(logging.DEBUG)


class FieldParam(BaseModel):
    field_name: str
    field_type: str
    field_description: str = ""


class FieldParamEdited(BaseModel):
    key: int
    fieldName: str
    nativeDataType: Optional[str]
    datahubType: str
    fieldTags: Optional[List[str]]
    fieldGlossaryTerms: Optional[List[str]]
    fieldDescription: Optional[str]
    editKey: str


class create_dataset_params(BaseModel):
    dataset_name: str
    fields: List[FieldParam]
    dataset_owner: str = "no_owner"
    dataset_description: str = ""
    dataset_location: str = ""
    dataset_origin: str = ""
    hasHeader: str = "n/a"
    headerLine: int = 1
    browsepathList: List[Dict[str,str]]
    user_token: str
    platformSelect: str
    parentContainer: str = ""
    frequency: str = ""
    dataset_frequency_details: str = ""



class dataset_status_params(BaseModel):
    dataset_name: str
    requestor: str
    user_token: str
    desired_state: bool


class browsepath_params(BaseModel):
    dataset_name: str
    requestor: str
    user_token: str
    browsePaths: List[Dict[str,str]]


class add_sample_params(BaseModel):
    dataset_name: str
    requestor: str
    user_token: str
    samples: Dict
    timestamp: int


class delete_sample_params(BaseModel):
    dataset_name: str
    requestor: str
    user_token: str
    timestamp: int


class schema_params(BaseModel):
    dataset_name: str
    requestor: str
    user_token: str
    dataset_fields: List[FieldParamEdited]


class prop_params(BaseModel):
    dataset_name: str
    requestor: str
    user_token: str
    properties: List[Dict]


class container_param(BaseModel):
    dataset_name: str
    requestor: str
    user_token: str
    container: str    

class name_param(BaseModel):
    dataset_name: str
    requestor: str
    user_token: str
    displayName: str 
# class echo_param(BaseModel):
#     user_input: Any

#     class Config:
#         arbitary_types_allowed = True


def determine_type(input: str) -> str:
    """
    this list will grow when we have more dataset types in the form
    """
    m = re.search('urn:li:dataPlatform:(.+)', input)
    if m:
        platform = m.group(1)
        if platform.islower(): # should not allow any uppercase.
            return platform

    return 'error'

class MyFilter(object):
    def __init__(self, level):
        self.__level = level

    def filter(self, logRecord):
        return logRecord.levelno <= self.__level
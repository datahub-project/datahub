# flake8: noqa
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, validator


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
    dataset_type: Union[str, Dict[str, str]]
    fields: List[FieldParam]
    dataset_owner: str = "no_owner"
    dataset_description: str = ""
    dataset_location: str = ""
    dataset_origin: str = ""
    hasHeader: str = "n/a"
    headerLine: int = 1
    browsepathList: List[str]
    user_token: str

    class Config:
        schema_extra = {
            "example": {
                "dataset_name": "name of dataset",
                "dataset_type": "text/csv",
                "dataset_description": "What this dataset is about...",
                "dataset_owner": "12345",
                "dataset_location": "the file can be found here @...",
                "dataset_origin": "this dataset found came from...\
                    ie internet",
                "hasHeader": "no",
                "headerLine": 1,
                "browsepathList": ["/user/", "/csv/"],
                "dataset_fields": [
                    {
                        "field_name": "columnA",
                        "field_type": "string",
                        "field_description": "what is column A about",
                    },
                    {
                        "field_name": "columnB",
                        "field_type": "num",
                        "field_description": "what is column B about",
                    },
                ],
            }
        }

    # @validator('dataset_name')
    # def dataset_name_alphanumeric(cls, v):
    #     assert len(set(v).difference(ascii_letters+digits+' -_/\\'))==0,
    # 'dataset_name must be alphanumeric/space character only'
    #     return v
    # @validator('dataset_type')
    # def dataset_type_alphanumeric(cls, v):
    #     assert v.isalpha(), 'dataset_type must be alphabetical string only'
    #     return v


class dataset_status_params(BaseModel):
    dataset_name: str
    requestor: str
    user_token: str
    desired_state: bool


class browsepath_params(BaseModel):
    dataset_name: str
    requestor: str
    user_token: str
    browsePaths: List[str]

class add_sample_params(BaseModel):
    dataset_name: str
    requestor: str
    user_token: str
    samples: List[Dict]
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
    description: str
    properties: List[Dict]


class echo_param(BaseModel):
    user_input: Any

    class Config:
        arbitary_types_allowed = True


def determine_type(type_input: Union[str, Dict[str, str]]) -> str:
    """
    this list will grow when we have more dataset types in the form
    """
    if isinstance(type_input, Dict):
        type_input_str = type_input.get("dataset_type", "")
    if (type_input_str.lower() == "text/csv") or (
        type_input_str.lower() == "application/octet-stream"
    ):
        return "csv"
    if type_input_str.lower() == "json":
        return "json"
    else:
        return "undefined"

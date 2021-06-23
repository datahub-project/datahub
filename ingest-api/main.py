import uvicorn
from fastapi import FastAPI
from typing import Optional, List, Union
from typing_extensions import TypedDict
from pydantic import BaseModel, validator
from helper.mce_convenience import delete_mce, make_schema_mce, make_dataset_urn, \
                    make_user_urn, make_dataset_description_mce, recover_mce, \
                    make_browsepath_mce, make_ownership_mce, make_platform, get_sys_time 
from datahub.emitter.rest_emitter import DatahubRestEmitter


import logging
from logging.handlers import TimedRotatingFileHandler

logformatter = logging.Formatter('%(asctime)s;%(levelname)s;%(message)s')

log = TimedRotatingFileHandler('./log/debug.log', when='midnight', interval=1, backupCount=14)
log.setLevel(logging.DEBUG)
log.setFormatter(logformatter)
streamLogger = logging.StreamHandler()
streamLogger.setFormatter(logformatter)
streamLogger.setLevel(logging.DEBUG)

rootLogger = logging.getLogger("__name__")
rootLogger.addHandler(log)    
rootLogger.addHandler(streamLogger)
rootLogger.setLevel(logging.DEBUG)

rootLogger.info("started!")

#todo - to refactor the classes here into additional modules when it gets unwieldy
#env file, ie for pointing to the GMS rest endpoint.

rest_endpoint = "http://datahub-gms:8080"
datahub_url = "http://localhost:9002"
api_emitting_port = 80

app = FastAPI(title="Datahub secret API",
    description="For generating datasets",
    version="0.0.1",)

@app.get("/hello")
async def hello_world() -> None:
    """
    Just a hello world endpoint to ensure that the api is running. 
    """
    ## how to check that this dataset exist? - curl to GMS? 
    rootLogger.info("hello world called")
    return  "hello world"


class FieldParam(TypedDict):
    field_name: str
    field_type: str
    field_description: Optional[str]

class create_dataset_params(BaseModel):
    dataset_name: str 
    dataset_type: str 
    dataset_fields: List[FieldParam] 
    dataset_owner: str
    dataset_description: Union[None,str]     
    dataset_location: Union[None,str]
    dataset_origin: Union[None,str]

    class Config:
        schema_extra = {
            "example": {
                "dataset_name": "name of dataset",
                "dataset_type": "text/csv",
                "dataset_description": "What this dataset is about...",
                "dataset_owner": "12345",
                "dataset_location": "the file can be found here @...",
                "dataset_origin": "this dataset found came from... ie internet",
                "dataset_fields": [{
                    "field_name": "columnA",
                    "field_type": "string",
                    "field_description": "what is column A about"
                },
                {
                    "field_name": "columnB",
                    "field_type": "num",
                    "field_description": "what is column B about"
                }
                ]
            }
        }
    # @validator('dataset_name')
    # def dataset_name_alphanumeric(cls, v):
    #     assert len(set(v).difference(ascii_letters+digits+' -_/\\'))==0, 'dataset_name must be alphanumeric/space character only'
    #     return v
    # @validator('dataset_type')
    # def dataset_type_alphanumeric(cls, v):
    #     assert v.isalpha(), 'dataset_type must be alphabetical string only'
    #     return v

def determine_type(type_string:str) -> str:
    if type_string.lower()=='text/csv':
        return 'csv'
    else:
        return 'new_undefined_type'

@app.post("/make_dataset")
async def create_item(item: create_dataset_params) -> None:
    """
    This endpoint is meant for manually defined or parsed file datasets.
    """
    rootLogger.info("make_dataset_request_received {}".format(item))
    item.dataset_type = determine_type(item.dataset_type)
    rootLogger.debug("type ok {}".format(item.dataset_type))
    item.dataset_name = "{}_{}".format(item.dataset_name,str(get_sys_time()))
    datasetName = make_dataset_urn(item.dataset_type, item.dataset_name)
    platformName = make_platform(item.dataset_type)        
    browsePath = "/{}/{}".format(item.dataset_type, item.dataset_name) 
    #not sure what happens when multiple different datasets all lead to the same browsepath, probably disaster.    
    requestor = make_user_urn(item.dataset_owner)
    rootLogger.debug("defined")
    try:
        properties = {"dataset_origin": item.dict().get("dataset_origin", ""), "dataset_location": item.dict().get("dataset_location", "")}
        
        all_mce=[]
        dataset_description = item.dataset_description if item.dataset_description else ""
        all_mce.append(make_dataset_description_mce(dataset_name = datasetName,
                                                    description = item.dataset_description, 
                                                    customProperties=properties))
        rootLogger.debug("properties ok")
        all_mce.append(make_ownership_mce(actor = requestor, 
                                            dataset_urn = datasetName))
        all_mce.append(make_browsepath_mce(dataset_urn=datasetName, 
                                            path=[browsePath]))
        for field in item.dataset_fields:
            field["fieldPath"] = field.pop("field_name")
        all_mce.append(make_schema_mce(dataset_urn = datasetName,
                                        platformName = platformName,
                                        actor = requestor,
                                        fields = item.dataset_fields,
                                        ))
        rootLogger.debug("all mce ok")
        emitter = DatahubRestEmitter(rest_endpoint)

        for mce in all_mce:
            emitter.emit_mce(mce)   
        emitter._session.close()
    except Exception as e:
        rootLogger.debug(e)
    rootLogger.info("make_dataset_request_completed_for {} requested_by {}".format(item.dataset_name, item.dataset_owner))      
    return "dataset can be found at {}/dataset/{}".format(datahub_url, make_dataset_urn(item.dataset_type, item.dataset_name))


class delete_dataset_params(BaseModel):
    dataset_name: str
    requestor: str
    platform: str

@app.post("/delete_dataset")
async def delete_item(item: delete_dataset_params) -> None:
    """
    This endpoint is to support soft delete of datasets. Still require a database/ES chron job to remove the entries though, it only suppresses it from search and UI
    """
    ## how to check that this dataset exist? - curl to GMS? 
    rootLogger.info("remove_dataset_request_received {}".format(item))
    datasetName = make_dataset_urn(item.platform, item.dataset_name)
    mce = delete_mce(dataset_name = datasetName)
    emitter = DatahubRestEmitter(rest_endpoint)
    emitter.emit_mce(mce)
    emitter._session.close() 
    rootLogger.info("remove_dataset_request_completed_for {} requested_by {}".format(item.dataset_name, item.requestor))      
    return "dataset has been removed from search and UI. please refresh the webpage."

@app.post("/recover_dataset")
async def recover_item(item: delete_dataset_params) -> None:
    """
    This endpoint is meant for undoing soft deletes. 
    """
    ## how to check that this dataset exist? - curl to GMS? 
    rootLogger.info("recover_dataset_request_received {}".format(item))
    datasetName = make_dataset_urn(item.platform, item.dataset_name)
    mce = recover_mce(dataset_name = datasetName)
    emitter = DatahubRestEmitter(rest_endpoint)
    emitter.emit_mce(mce)
    emitter._session.close() 
    rootLogger.info("recover_dataset_request_completed_for {} requested_by {}".format(item.dataset_name, item.requestor))      
    return "dataset has been restored"

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=api_emitting_port)
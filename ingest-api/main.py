import uvicorn
from fastapi import FastAPI
from typing import Optional, List
from typing_extensions import TypedDict
from pydantic import BaseModel, validator
from helper.mce_convenience import delete_mce, make_schema_mce, make_dataset_urn, \
                    make_user_urn, make_dataset_description_mce, recover_mce, \
                    make_browsepath_mce, make_ownership_mce, make_platform, get_sys_time 
from datahub.emitter.rest_emitter import DatahubRestEmitter
from string import ascii_letters, digits

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
async def recover_item() -> None:
    """
    Just a hello world to ensure that the api is running. 
    """
    ## how to check that this dataset exist? - curl to GMS? 
    rootLogger.info("hello world called")
    return  "hello world"


class FieldParam(TypedDict):
    name: str
    type: str
    description: Optional[str]

class create_dataset_params(BaseModel):
    dataset_name: str 
    dataset_type: str 
    fields: List[FieldParam] 
    owner: str
    description: Optional[str]     
    
    class Config:
        schema_extra = {
            "example": {
                "dataset_name": "name of dataset",
                "dataset_type": "csv",
                "description": "What this dataset is about...",
                "owner": "12345",
                "fields": [{
                    "name": "columnA",
                    "type": "string",
                    "description": "what is column A about"
                },
                {
                    "name": "columnB",
                    "type": "num",
                    "description": "what is column B about"
                }
                ]
            }
        }
    @validator('dataset_name')
    def dataset_name_alphanumeric(cls, v):
        assert len(set(v).difference(ascii_letters+digits+' '))==0, 'dataset_name must be alphanumeric/space character only'
        return v
    @validator('dataset_type')
    def dataset_type_alphanumeric(cls, v):
        assert v.isalpha(), 'dataset_type must be alphabetical string only'
        return v

@app.post("/make_dataset")
async def create_item(item: create_dataset_params) -> None:
    """
    This endpoint is meant for manually defined or parsed file datasets.
    """
    rootLogger.info("make_dataset_request_received {}".format(item))
    item.dataset_type = item.dataset_type.lower()
    item.dataset_name = "{}_{}".format(item.dataset_name,str(get_sys_time()))
    datasetName = make_dataset_urn(item.dataset_type, item.dataset_name)
    platformName = make_platform(item.dataset_type)        
    browsePath = "/{}/{}".format(item.dataset_type, item.dataset_name) 
    #not sure what happens when multiple different datasets all lead to the same browsepath, probably disaster.    
    requestor = make_user_urn(item.owner)

    all_mce=[]
    if item.description:
        all_mce.append(make_dataset_description_mce(dataset_name = datasetName,
                                                    description = item.description))
    all_mce.append(make_ownership_mce(actor = requestor, 
                                        dataset_urn = datasetName))
    all_mce.append(make_browsepath_mce(dataset_urn=datasetName, 
                                        path=[browsePath]))
    for field in item.fields:
        field["fieldPath"] = field.pop("name")
    all_mce.append(make_schema_mce(dataset_urn = datasetName,
                                    platformName = platformName,
                                    actor = requestor,
                                    fields = item.fields,
                                    ))
    emitter = DatahubRestEmitter(rest_endpoint)

    for mce in all_mce:
        emitter.emit_mce(mce)   
    emitter._session.close()
    rootLogger.info("make_dataset_request_completed_for {} requested_by {}".format(item.dataset_name, item.owner))      
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
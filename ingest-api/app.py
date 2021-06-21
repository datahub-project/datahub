import uvicorn
from fastapi import FastAPI
from typing import Optional, List
from typing_extensions import TypedDict
from pydantic import BaseModel, validator
from helper.mce_convenience import delete_mce, make_schema_mce, make_dataset_urn, \
                    make_user_urn, make_dataset_description_mce, \
                    make_browsepath_mce, make_ownership_mce, make_platform, get_sys_time 
from datahub.emitter.rest_emitter import DatahubRestEmitter
from string import ascii_letters, digits

import logging
from logging.handlers import TimedRotatingFileHandler

logformatter = logging.Formatter('%(asctime)s;%(levelname)s;%(message)s')
log = TimedRotatingFileHandler('./log/debug.log', 'midnight', 1, backupCount=5)
log.setLevel(logging.INFO)
log.setFormatter(logformatter)


logger = logging.getLogger('main')
logger.addHandler(log)    
logger.setLevel(logging.DEBUG)

#todo - add a logger for this endpoint
#add a docker volume also since this is the best place to capture input activity.


app = FastAPI(title="Datahub secret API",
    description="For generating datasets",
    version="0.0.1",)


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
    emitter = DatahubRestEmitter("http://localhost:8080")

    for mce in all_mce:
        emitter.emit_mce(mce)   
    emitter._session.close()      
    return "dataset can be found at http://localhost:9002/dataset/{}".format(make_dataset_urn(item.dataset_type, item.dataset_name))


class delete_dataset_params(BaseModel):
    dataset_name: str
    requestor: str
    platform: str

@app.post("/delete_dataset")
async def delete_item(item: delete_dataset_params) -> None:
    """
    This endpoint is meant for manually defined or parsed file datasets.
    """
    ## how to check that this dataset exist?
    datasetName = make_dataset_urn(item.platform, item.dataset_name)
    mce = delete_mce(dataset_name = datasetName)
    emitter = DatahubRestEmitter("http://localhost:8080")
    emitter.emit_mce(mce)
    emitter._session.close() 
    #rootLogger.info("{} has requested to remove dataset {} from {}".format(requestor, dataset_name, platform))
    return "dataset has been removed from search and UI"

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
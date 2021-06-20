import uvicorn
from fastapi import FastAPI
from typing import Optional, List
from typing_extensions import TypedDict
from pydantic import BaseModel
from helper.mce_convenience import make_schema_mce, make_dataset_urn, \
                    make_user_urn, make_dataset_description_mce, \
                    make_browsepath_mce, make_ownership_mce 
from datahub.emitter.rest_emitter import DatahubRestEmitter

app = FastAPI(title="Datahub secret API",
    description="For generating datasets",
    version="0.0.1",)


class FieldParam(TypedDict):
    name: str
    type: str
    description: Optional[str]

class dataset_params(BaseModel):
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


@app.post("/make_dataset")
async def create_item(item: dataset_params) -> None:
    """
    This endpoint is meant for manually defined or parsed file datasets.
    """
    all_mce=[]
    if item.description:
        all_mce.append(make_dataset_description_mce(
                dataset_name = make_dataset_urn(item.dataset_type, item.dataset_name),
                description = item.description
            ))
    all_mce.append(make_ownership_mce(owner = item.owner, dataset_urn = make_dataset_urn(item.dataset_type, item.dataset_name)))
    all_mce.append(make_browsepath_mce(dataset_urn=make_dataset_urn(item.dataset_type, item.dataset_name), 
                                        path=["/{}/{}".format(item.dataset_type, item.dataset_name)]))
    for field in item.fields:
        field["fieldPath"] = field.pop("name")
    all_mce.append(make_schema_mce(datset_urn = make_dataset_urn(item.dataset_type, item.dataset_name),
                    platformName = item.dataset_type,
                    actor = item.owner,
                    fields = item.fields,
                    ))
    emitter = DatahubRestEmitter("http://localhost:8080")

    for mce in all_mce:
        emitter.emit_mce(mce)        
    return "dataset can be found at http://localhost:9002/dataset/{}".format(make_dataset_urn(item.dataset_type, item.dataset_name))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
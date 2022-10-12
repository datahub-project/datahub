# flake8: noqa
# command to run in CLI mode is 
# gunicorn -c config.py main:app --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8001
# also need export PROMETHEUS_MULTIPROC_DIR=/home/admini/development/datahub/fastapi/tmp
import logging
import os
from os import environ
import time
from logging.handlers import TimedRotatingFileHandler
import requests
import uvicorn
from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import \
    DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (ChangeTypeClass, GlossaryTermAssociationClass, GlossaryTermsClass,
                                             SystemMetadataClass)
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from starlette_exporter import PrometheusMiddleware, handle_metrics

from ingest_api.helper.mce_convenience import *
from ingest_api.helper.models import *
from ingest_api.helper.security import authenticate_action, verify_token
from urllib3.exceptions import InsecureRequestWarning 

CLI_MODE = False if environ.get("RUNNING_IN_DOCKER") else True

frequency_enum = ["Adhoc","Periodic","Onetime","Unknown"]
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# when running ingest-api from CLI, need to set some params.
# cos dataset_profile_index name varies depending on ES. If there is an existing index (and datahub is instantiated on top, then it will append a UUID to it)
if not CLI_MODE:
    for env_var in [
        "ACCEPT_ORIGINS",
        "RUNNING_IN_DOCKER",
        "JWT_SECRET",
        "DATAHUB_AUTHENTICATE_INGEST",
        "DATAHUB_FRONTEND",
        "ELASTIC_HOST",
        "DATAHUB_BACKEND",
        "ANNOUNCEMENT_URL",
        "DATASET_PROFILE_INDEX"
    ]:
        if not os.environ[env_var]:
            raise Exception(
                f"{env_var} is not defined \
                to operate in Container mode"
            )
    if not os.path.exists("/var/log/ingest/"):
        os.mkdir("/var/log/ingest/")
    if not os.path.exists("/var/log/ingest/json"):
        os.mkdir("/var/log/ingest/json")
    log_path = "/var/log/ingest/ingest_api.log"
    log_path_simple = "/var/log/ingest/ingest_api.simple.log"
else:
    os.environ["ELASTIC_HOST"] = "http://localhost:9200"
    os.environ["DATAHUB_BACKEND"] = "http://localhost:8080"
    os.environ["ANNOUNCEMENT_URL"] = "https://xaluil.gitlab.io/announce/"
    os.environ["DATAHUB_FRONTEND"] = "http://172.19.0.1:9002"
    os.environ["DATASET_PROFILE_INDEX"] = "dataset_datasetprofileaspect_v1"
    
    if not os.path.exists("./logs/"):
        os.mkdir(f"{os.getcwd()}/logs/")
    if not os.path.exists("./prom/"):
        os.mkdir(f"{os.getcwd()}/prom/")        
    os.environ["PROMETHEUS_MULTIPROC_DIR"]=f'{os.getcwd()}/prom'
    log_path = f"{os.getcwd()}/logs/ingest_api.log"
    log_path_simple = f"{os.getcwd()}/logs/ingest_api.simple.log"

rest_endpoint = os.environ["DATAHUB_BACKEND"]
api_emitting_port = 8001
# logging - 1 console logger showing info-level+, and 2 logger logging INFO+ AND DEBUG+ levels
# --------------------------------------------------------------
rootLogger = logging.getLogger("ingest")
logformatter = logging.Formatter("%(asctime)s;%(levelname)s;%(funcName)s;%(message)s")
rootLogger.setLevel(logging.DEBUG)
streamLogger = logging.StreamHandler()
streamLogger.setFormatter(logformatter)
streamLogger.setLevel(logging.INFO)  #docker logs will show simplified
rootLogger.addHandler(streamLogger)
log_simple = TimedRotatingFileHandler(
    log_path_simple, when="midnight", interval=1, backupCount=730
)
# log_simple.addFilter(MyFilter(logging.INFO))  # only capture INFO
log_simple.setLevel(logging.INFO)
log_simple.setFormatter(logformatter)
rootLogger.addHandler(log_simple)
log = TimedRotatingFileHandler(
    log_path, when="midnight", interval=1, backupCount=730
)
log.setLevel(logging.DEBUG)
log.setFormatter(logformatter)
rootLogger.addHandler(log)

rootLogger.info(f"CLI mode : {CLI_MODE}")
rootLogger.info("started ingest_api!")
# --------------------------------------------------------------
if not os.environ["PROMETHEUS_MULTIPROC_DIR"]:
    raise Exception("Gunicorn cannot start without multiprocessor directory set!")

app = FastAPI(
    title="Datahub secret API",
    openapi_url=None,
    redoc_url=None
)
origins = [
    "http://localhost:9002",
    "http://172.19.0.1:9002",
    "http://172.19.0.1:3000",
    "http://localhost:3000",
]
if environ.get("ACCEPT_ORIGINS") is not None:
    new_origin = environ["ACCEPT_ORIGINS"]
    origins.append(new_origin)
    rootLogger.info(f"{new_origin} is added to CORS allow_origins")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["POST", "GET", "OPTIONS"],
)
# add prometheus monitoring to ingest-api via starlette-exporter
app.add_middleware(PrometheusMiddleware)
app.add_route("/custom/metrics", handle_metrics)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    """
    This is meant to log malformed POST requests
    """
    exc_str = f"{exc}".replace("\n", " ").replace("   ", " ")

    rootLogger.error(exc_str)
    rootLogger.error(
        f"malformed POST request \
        {request.body} from {request.client} from {request.remote_addr}" 
    )
    return PlainTextResponse(str(exc_str), status_code=400)


@app.get("/custom/hello")
async def hello_world() -> None:
    """
    Just a hello world endpoint to ensure that the api is running.
    """
    # how to check that this dataset exist? - curl to GMS?
    # rootLogger.info("hello world is called")
    # rootLogger.info("/custom/hello is called!")
    return JSONResponse(
        content={
            "message": "Hello world",
            "timestamp": int(time.time() * 1000)
        },
        status_code=200,
    )
# import responses
@app.get("/custom/announce")
async def echo_announce() -> None:
    """
    Just a hello world endpoint to ensure that the api is running.
    """
    # verify = False cos accessing HTTPS page from container.
    try:
        response = requests.get(os.environ["ANNOUNCEMENT_URL"], verify=False)         
        received = response.json()
        return JSONResponse(
            content={
                "message": received["message"],
                "timestamp": received["timestamp"]
            },
            status_code=200,
        )
    except:        
        rootLogger.error(f"announcement upstream returns error")
        return JSONResponse(
            content={
                "message": "Unable to fetch Gitlab Pages Announcements at this time",
                "timestamp": 0
            },
            status_code=200,
        )

@app.post("/custom/update_browsepath")
async def update_browsepath(item: browsepath_params):

    rootLogger.info("update_browsepath_request_received from {} for {}".format(item.requestor, item.dataset_name))
    rootLogger.debug("update_browsepath_request_received {}".format(item))
    datasetName = item.dataset_name
    token = item.user_token
    user = item.requestor
    if authenticate_action(token=token, user=user, dataset=datasetName):
        dataset_snapshot = DatasetSnapshot(
            urn=item.dataset_name,
            aspects=[],
        )
        all_paths = []
        for path in item.browsePaths:
            all_paths.append(path['browsepath'] + "dataset")
        browsepath_aspect = make_browsepath_mce(path=all_paths)
        dataset_snapshot.aspects.append(browsepath_aspect)
        metadata_record = MetadataChangeEvent(
            proposedSnapshot=dataset_snapshot,
            systemMetadata=SystemMetadataClass(
                runId=f"{datasetName}_browsepath_{str(int(time.time()))}"
            ),
        )
        response = emit_mce_respond(
            metadata_record=metadata_record,
            owner=item.requestor,
            event="UI Update Browsepath",
            token=item.user_token,
        )
        
        return JSONResponse(
            content={"message": response.get("message", "")}, status_code=response['status_code']
        ) 
    else:
        rootLogger.error(
            f"authentication failed for request\
            (update_browsepath) from {user}"
        )
        return JSONResponse(content={"message": "Authentication Failed"}, status_code=401)


@app.post("/custom/update_samples")
async def update_samples(item: add_sample_params):

    rootLogger.info("update_sample_request_received from {}".format(item.requestor))
    rootLogger.debug("update_sample_request_received {}".format(item))
    datasetName = item.dataset_name
    token = item.user_token
    user = item.requestor
    if authenticate_action(token=token, user=user, dataset=datasetName):
        generated_mcp = make_profile_mcp(
            sample_values=item.samples,
            timestamp=item.timestamp,
            datasetName=datasetName,
        )
        response = emit_mcp_respond(
            metadata_record=generated_mcp,
            owner=item.requestor,
            event="Update Dataset Profile",
            token=item.user_token,
        )
        
        return JSONResponse(
            content={"message": response.get("message", "")}, status_code=response["status_code"]
        ) 
    else:
        rootLogger.error(
            f"authentication failed for request\
            (update_samples) from {user}"
        )
        return JSONResponse(content={"message": "Authentication Failed"}, status_code=401)


@app.post("/custom/delete_samples")
async def delete_samples(item: delete_sample_params):

    rootLogger.info("delete_sample_request_received from {}".format(item.requestor))
    rootLogger.debug("delete_sample_request_received {}".format(item))
    datasetName = item.dataset_name
    token = item.user_token
    user = item.requestor
    headers = {
        "Content-Type": "application/json",
    }
    elastic_host = os.environ["ELASTIC_HOST"]
    elastic_username = os.environ.get("ELASTIC_USERNAME")
    elastic_password = os.environ.get("ELASTIC_PASSWORD")
    if elastic_username:
        scheme, host = elastic_host.split("//")
        elastic_host = f"{scheme}//{elastic_username}:{elastic_password}@{host}"
    profile_index = os.environ["DATASET_PROFILE_INDEX"]
    if authenticate_action(token=token, user=user, dataset=datasetName):
        data = """{{"query":{{"bool":{{"must":[{{"match":{{"timestampMillis":{timestamp}}}}},
            {{"match":{{"urn":"{urn}"}}}}]}}}}}}""".format(
            urn=datasetName, timestamp=str(item.timestamp)
        )
        response = requests.post(
            "{es_host}/{profile_index}/_delete_by_query".format(
                es_host=elastic_host, profile_index=profile_index
            ),
            headers=headers,
            data=data,
        )
        results = response.json()
        rootLogger.error(f"ES delete outcome: {results}")
        return JSONResponse(
            content={"message": ""}, status_code=response.status_code
        )        
    else:
        rootLogger.error(
            f"authentication failed for request\
            (delete_sample) from {user}"
        )
        return JSONResponse(content={"message": "Authentication Failed"}, status_code=401)


@app.post("/custom/update_schema")
async def update_schema(item: schema_params):

    rootLogger.info(
        "update_dataset_schema_request_received from {}".format(item.requestor)
    )
    rootLogger.debug("update_dataset_schema_request_received {}".format(item))
    datasetName = item.dataset_name
    token = item.user_token
    user = item.requestor
    if authenticate_action(token=token, user=user, dataset=datasetName):
        dataset_snapshot = DatasetSnapshot(
            urn=datasetName,
            aspects=[],
        )
        rootLogger.info(
            f"token check:\
            {verify_token(item.user_token, item.requestor)}"
        )
        platformName = derive_platform_name(datasetName)
        rootLogger.info(item.dataset_fields)
        field_params = update_field_param_class(item.dataset_fields)
        rootLogger.info(field_params)
        schemaMetadata_aspect = make_schema_mce(
            platformName=platformName,
            actor=item.requestor,
            fields=field_params,
        )
        rootLogger.info(schemaMetadata_aspect)
        dataset_snapshot.aspects.append(schemaMetadata_aspect)
        metadata_record = MetadataChangeEvent(
            proposedSnapshot=dataset_snapshot,
            systemMetadata=SystemMetadataClass(
                runId=f"{datasetName}_schema_{str(int(time.time()))}"
            ),
        )
        response = emit_mce_respond(
            metadata_record=metadata_record,
            owner=item.requestor,
            event="UI Update Schema",
            token=item.user_token,
        )        
        return JSONResponse(
            content=response.get("message", ""), status_code=response["status_code"]
        )
    else:
        rootLogger.error(
            f"authentication failed for request(update_schema) from {user}"
        )
        return JSONResponse(content={"message": "Authentication Failed"}, status_code=401)


@app.post("/custom/update_properties")
async def update_prop(item: prop_params):
    # i expect the following:
    # name: do not touch
    # schema will generate schema metatdata (not the editable version)
    # properties: get description from graphql and props from form.
    # This will form DatasetProperty (Not EditableDatasetProperty)
    # platform info: needed for schema

    rootLogger.info(
        "update_dataset_property_request_received from {}".format(item.requestor)
    )
    rootLogger.debug("update_dataset_property_request_received {}".format(item))
    datasetName = item.dataset_name
    token = item.user_token
    user = item.requestor
    if authenticate_action(token=token, user=user, dataset=datasetName):
        graph = build_datahub_graph(token)
        existing_prop = graph.get_aspect_v2(
            entity_urn=datasetName,
            aspect="datasetProperties",
            aspect_type=DatasetPropertiesClass,
        )
        if not existing_prop:
            existing_prop=DatasetPropertiesClass()

        properties = item.properties
        all_properties = {}
        for prop in properties:
            if "propertyKey" and "propertyValue" in prop:
                all_properties[prop.get("propertyKey")] = prop.get("propertyValue")
        new_prop = DatasetPropertiesClass(
            customProperties = all_properties,
            name = existing_prop.name if existing_prop.name else None,
            description = existing_prop.description if existing_prop.description else None,
            tags = existing_prop.tags if existing_prop.tags else None,
        )
        mcp = MetadataChangeProposalWrapper(
            aspect = new_prop,
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=datasetName,
            aspectName="datasetProperties",
            systemMetadata=SystemMetadataClass(
                runId=f"{datasetName}_prop_{str(int(time.time()))}"
            ),
        )
        response = emit_mcp_respond(
            metadata_record=mcp,
            owner=item.requestor,
            event="UI Update Properties",
            token=item.user_token,
        )
        rootLogger.error(response)
        return JSONResponse(
            content={"message": response["message"]}, status_code=response["status_code"]
        )
    else:
        rootLogger.error(
            f"authentication failed for request\
            (update_properties) from {user}"
        )
        return JSONResponse(content={"message": "Authentication Failed"}, status_code=401)


def build_datahub_graph(token: str) -> DataHubGraph:
    graph = DataHubGraph(DatahubClientConfig(server=rest_endpoint, token=token))
    return graph

def emit_mce_respond(
    metadata_record: MetadataChangeEvent, owner: str, event: str, token: str
) -> dict():
    datasetName = metadata_record.proposedSnapshot.urn
    for mce in metadata_record.proposedSnapshot.aspects:
        if not mce.validate():
            rootLogger.error(f"{mce.__class__} is not defined properly")
            return {
                "message": f"MCE was incorrectly defined.\
                    {event} was aborted",
                "status_code": 400,
            }

    if CLI_MODE:
        generate_json_output_mce(metadata_record, "./logs/")
    else:
        generate_json_output_mce(metadata_record, "/var/log/ingest/json/")
    try:
        rootLogger.debug(metadata_record)
        emitter = DatahubRestEmitter(rest_endpoint, token=token)
        emitter.emit_mce(metadata_record)
        emitter._session.close()
    except Exception as e:
        rootLogger.error(e)
        return {
            "message": f"{event} failed because upstream error {e}",
            "status_code": 500,
        }
            
    rootLogger.info(
        f"{event} {datasetName} requested_by {owner} completed successfully"
    )
    return {        
        "message": f"{event} completed successfully",
        "status_code" : 201,
    }

def emit_mcp_respond(
    metadata_record: MetadataChangeProposalWrapper, owner: str, event: str, token: str
) -> dict():
    datasetName = metadata_record.entityUrn
    if CLI_MODE:
        generate_json_output_mcp(metadata_record, "./logs/")
    else:
        generate_json_output_mcp(metadata_record, "/var/log/ingest/json/")
    try:
        rootLogger.debug(metadata_record)
        emitter = DatahubRestEmitter(rest_endpoint, token=token)
        emitter.emit_mcp(metadata_record)
        emitter._session.close()
    except Exception as e:
        rootLogger.error(e)
        return {
                "message": f"{event} failed because upstream error {e}",                
                "status_code": 500,
        }
            
    rootLogger.info(
        f"{event} {datasetName} requested_by {owner} completed successfully"
    )
    return {        
        "message": f"{event} completed successfully",
        "status_code": 201,
    }       

@app.post("/custom/make_dataset")
async def create_item(item: create_dataset_params) -> None:
    """
    This endpoint is meant for manually defined or parsed file datasets.
    #todo - to revisit to see if refactoring is needed when make_json is up.
    """
    rootLogger.info("make_dataset_request_received from {}".format(item.dataset_owner))
    rootLogger.debug("make_dataset_request_received {}".format(item))
    dataset_type = determine_type(item.platformSelect)
    token = item.user_token
    user = item.dataset_owner
    requestor = make_user_urn(user)
    if verify_token(token, user): # check user is who he say he is
        dataset_name_uuid = "{}_{}".format(item.dataset_name, str(get_sys_time()))
        datasetDisplayName = item.dataset_name
        datasetUrn = make_dataset_urn(dataset_type, dataset_name_uuid)
        platformName = make_platform(dataset_type)
        browsepathList = [
            item['browsepath'] + "/" if not item['browsepath'].endswith("/") else item['browsepath']
            for item in item.browsepathList
        ]
        # this line is in case the endpoint is called by API and not UI,
        # which will enforce ending with /.
        browsepaths = [path + "dataset" for path in browsepathList]        
        properties = {
            "dataset_origin": item.dict().get("dataset_origin", ""),
            "dataset_location": item.dict().get("dataset_location", ""),
        }
        dataset_description = (
            item.dataset_description if item.dataset_description else ""
        )
        dataset_snapshot = DatasetSnapshot(
            urn=datasetUrn,
            aspects=[],
        )
        if item.dict().get("frequency","") in frequency_enum:
            properties["dataset_frequency"] = item.dict().get("frequency", "") + ": " + item.dict().get("dataset_frequency_details", "")
            freq = item.dict().get("frequency")
            frequency = GlossaryTermsClass(
                terms = [
                    GlossaryTermAssociationClass(urn=f"urn:li:glossaryTerm:Metadata.Dataset.Frequency.{freq}")
                ],
                auditStamp=AuditStampClass(
                    time=0,
                    actor=f"urn:li:corpuser:{item.dataset_owner}"
                )
            )
            dataset_snapshot.aspects.append(frequency)
        dataset_snapshot.aspects.append(
            make_dataset_description_mce(
                dataset_name=datasetDisplayName,
                description="",
                customProperties=properties,
            )
        )
        dataset_snapshot.aspects.append(
            make_editable_dataset_description(description=dataset_description)
        )
        dataset_snapshot.aspects.append(
            make_ownership_mce(actor=requestor, dataset_urn=datasetUrn)
        )
        dataset_snapshot.aspects.append(make_browsepath_mce(path=browsepaths))
        field_params = []
        for existing_field in item.fields:
            current_field = {}
            current_field.update(existing_field.dict())
            current_field["fieldPath"] = current_field.pop("field_name")
            if "field_description" not in current_field:
                current_field["field_description"] = ""
            field_params.append(current_field)

        dataset_snapshot.aspects.append(
            create_new_schema_mce(
                platformName=platformName,
                actor=requestor,
                fields=field_params,
            )
        )
        metadata_record = MetadataChangeEvent(
            proposedSnapshot=dataset_snapshot,
            systemMetadata=SystemMetadataClass(
                runId=f"{requestor}_make_{str(int(time.time()))}"
            ),
        )
        # i am emitting 2 times now, 1 for dataset and 1 for container MCP, 
        # which unfortunately do not fit into MCE snapshot, as a new aspect
        response1 = emit_mce_respond(
            metadata_record=metadata_record,
            owner=requestor,
            event="Create Dataset",
            token=token,
        )
        container_mcp = MetadataChangeProposalWrapper(
            aspect = make_container_aspect(item.parentContainer),
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=datasetUrn,
            aspectName="container",
            systemMetadata=SystemMetadataClass(
                runId=f"{dataset_name_uuid}_container_{str(int(time.time()))}"
            ),
        )
        response2 = emit_mcp_respond(
            metadata_record=container_mcp,
            owner=requestor,
            event=f"Make-Dataset: update_container:{item.parentContainer}",
            token=item.user_token,
        )
        
        if response1["status_code"]==201 and response2["status_code"]==201:
            return JSONResponse(
                content={"message": "completed" }, status_code=201
            )
    else:
        rootLogger.error("make_dataset request failed from {}".format(requestor))
        return JSONResponse(content={"message": "Authentication Failed"}, status_code=401)


@app.post("/custom/update_dataset_status")
async def delete_item(item: dataset_status_params) -> None:
    """
    This endpoint is to support soft delete of datasets. Still require
    a database/ES chron job to remove the entries though, it only
    suppresses it from search and UI
    """
    rootLogger.info("remove_dataset_request_received from {}".format(item.requestor))
    rootLogger.debug("remove_dataset_request_received {}".format(item))
    datasetName = item.dataset_name
    token = item.user_token
    user = item.requestor
    if authenticate_action(token=token, user=user, dataset=datasetName):
        mce = make_status_mce(
            dataset_name=item.dataset_name, desired_status=item.desired_state
        )
        response = emit_mce_respond(
            metadata_record=mce,
            owner=item.requestor,
            event=f"Status Update removed:{item.desired_state}",
            token=item.user_token,
        )
        rootLogger.error(response)
        return JSONResponse(
            content={"message": response["message"]}, status_code=response["status_code"]
        )
    else:
        rootLogger.error(
            f"authentication failed for request\
            (update_schema) from {user}"
        )
        return JSONResponse(content={"message": "Authentication Failed"}, status_code=401)

@app.post("/custom/update_containers")
async def update_container(item: container_param) -> None:
    """
    This endpoint is to support update/create container aspect. 
    """
    rootLogger.info("update_container_request_received from {}".format(item.requestor))
    rootLogger.debug("update_container_request_received {}".format(item))
    datasetName = item.dataset_name
    token = item.user_token
    user = item.requestor
    container = item.container
    if authenticate_action(token=token, user=user, dataset=datasetName):
        mcp = MetadataChangeProposalWrapper(
            aspect = make_container_aspect(container),
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=datasetName,
            aspectName="container",
            systemMetadata=SystemMetadataClass(
                runId=f"{datasetName}_container_{str(int(time.time()))}"
            ),
        )
        response = emit_mcp_respond(
            metadata_record=mcp,
            owner=item.requestor,
            event=f"Update container:{item.container}",
            token=item.user_token,
        )
        rootLogger.error(response)
        return JSONResponse(
            content={"message": response["message"]}, status_code=response["status_code"]
        )
    else:
        rootLogger.error(
            f"authentication failed for request\
            (update_container) from {user}"
        )
        return JSONResponse(content={"message": "Authentication Failed"}, status_code=401)

@app.post("/custom/update_dataset_name")
async def update_container(item: name_param) -> None:
    """
    This endpoint is to support update of dataset name. 
    """
    rootLogger.info("update_displayName_request_received from {}".format(item.requestor))
    rootLogger.debug("update_displayName_request_received {}".format(item))
    datasetName = item.dataset_name
    token = item.user_token
    user = item.requestor
    displayName = item.displayName
    if authenticate_action(token=token, user=user, dataset=datasetName):
        graph = build_datahub_graph(token)
        existing_prop = graph.get_aspect_v2(
            entity_urn=datasetName,
            aspect="datasetProperties",
            aspect_type=DatasetPropertiesClass,
        )
        if not existing_prop:
            existing_prop=DatasetPropertiesClass()
        new_prop = DatasetPropertiesClass(
            customProperties = existing_prop.customProperties if existing_prop.customProperties else None,
            name = displayName,
            description = existing_prop.description if existing_prop.description else None,
            tags = existing_prop.tags if existing_prop.tags else None,
        )
        mcp = MetadataChangeProposalWrapper(
            aspect = new_prop,
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=datasetName,
            aspectName="datasetProperties",
            systemMetadata=SystemMetadataClass(
                runId=f"{datasetName}_rename_{str(int(time.time()))}"
            ),
        )
        response = emit_mcp_respond(
            metadata_record=mcp,
            owner=item.requestor,
            event=f"Update dataset name:{item.displayName}",
            token=item.user_token,
        )
        rootLogger.error(response)
        return JSONResponse(
            content={"message": response["message"]}, status_code=response["status_code"]
        )
    else:
        rootLogger.error(
            f"authentication failed for request\
            (update_name) from {user}"
        )
        return JSONResponse(content={"message": "Authentication Failed"}, status_code=401)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
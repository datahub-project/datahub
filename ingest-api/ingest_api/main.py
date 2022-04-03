# flake8: noqa
# command to run in CLI mode is 
# gunicorn -c config.py main:app --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8001
# also need export PROMETHEUS_MULTIPROC_DIR=/home/admini/development/datahub/fastapi/tmp
import logging
import os
import time
from logging.handlers import TimedRotatingFileHandler
from os import environ
from urllib.parse import urljoin
import requests
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import \
    DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (ChangeTypeClass,
                                             SystemMetadataClass)
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from starlette_exporter import PrometheusMiddleware, handle_metrics

from ingest_api.helper.mce_convenience import (create_new_schema_mce,
                                               derive_platform_name,
                                               generate_json_output_mce,
                                               generate_json_output_mcp,
                                               get_sys_time,
                                               make_browsepath_mce,
                                               make_dataset_description_mce,
                                               make_dataset_urn,
                                               make_ownership_mce,
                                               make_platform, make_profile_mcp,
                                               make_schema_mce,
                                               make_status_mce, make_user_urn,
                                               update_field_param_class)
from ingest_api.helper.models import (add_sample_params, browsepath_params,
                                      create_dataset_params,
                                      dataset_status_params,
                                      delete_sample_params, determine_type,
                                      prop_params, schema_params)
from ingest_api.helper.security import authenticate_action, verify_token


class MyFilter(object):
    def __init__(self, level):
        self.__level = level

    def filter(self, logRecord):
        return logRecord.levelno <= self.__level


CLI_MODE = False if environ.get("RUNNING_IN_DOCKER") else True

rootLogger = logging.getLogger("ingest")
logformatter = logging.Formatter("%(asctime)s;%(levelname)s;%(funcName)s;%(message)s")
rootLogger.setLevel(logging.INFO)

streamLogger = logging.StreamHandler()
streamLogger.setFormatter(logformatter)
streamLogger.addFilter(MyFilter(logging.INFO))  #docker logs will show simplified
rootLogger.addHandler(streamLogger)
rootLogger.info(f"CLI mode : {CLI_MODE}")

# when running ingest-api from CLI, need to set some params.
if not CLI_MODE:
    for env_var in [
        "ACCEPT_ORIGINS",
        "RUNNING_IN_DOCKER",
        "JWT_SECRET",
        "DATAHUB_AUTHENTICATE_INGEST",
        "DATAHUB_FRONTEND",
        "ELASTIC_HOST",
        "DATAHUB_BACKEND",
        "ANNOUNCEMENT_URL"
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
    if not os.path.exists("./logs/"):
        os.mkdir(f"{os.getcwd()}/logs/")
    log_path = f"{os.getcwd()}/logs/ingest_api.log"
    log_path_simple = f"{os.getcwd()}/logs/ingest_api.simple.log"

rest_endpoint = os.environ["DATAHUB_BACKEND"]

log = TimedRotatingFileHandler(
    log_path, when="midnight", interval=1, backupCount=730
)
log.setLevel(logging.DEBUG)
log.setFormatter(logformatter)
rootLogger.addHandler(log)

log_simple = TimedRotatingFileHandler(
    log_path_simple, when="midnight", interval=1, backupCount=730
)
log_simple.addFilter(MyFilter(logging.INFO))  # only capture INFO
log_simple.setFormatter(logformatter)
rootLogger.addHandler(log_simple)

rootLogger.info("started ingest_api!")

app = FastAPI(
    title="Datahub secret API",
    description="For generating datasets",
    version="0.0.2",
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
    rootLogger.info("/hello is called!")
    return JSONResponse(
        content={
            "message": "<b>Hello world</b>",
            "timestamp": int(time.time() * 1000)
            # 'timestamp': 1636964967000
        },
        status_code=200,
    )

@app.get("/custom/announce")
async def echo_announce() -> None:
    """
    Just a hello world endpoint to ensure that the api is running.
    """        
    response = requests.get(os.environ["ANNOUNCEMENT_URL"])
    if response.status_code!=200:
        rootLogger.info(f"announcement upstream returns {str(response.status_code)}")
        JSONResponse(
        content={
            "message": "Unable to fetch Gitlab Pages Announcements at this time",
            "timestamp": int(time.time() * 1000)
        },
        status_code=200,
    )
    else:
        received = response.json()
        return JSONResponse(
            content={
                "message": received["message"],
                "timestamp": received["timestamp"]
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
            all_paths.append(path + "dataset")
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
        results = response.json()
        return JSONResponse(
            content={"message": results.get("message", "")}, status_code=response.status_code
        ) 
    else:
        rootLogger.info(
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
        results = response.json()
        return JSONResponse(
            content={"message": results.get("message", "")}, status_code=response.status_code
        ) 
    else:
        rootLogger.info(
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
    if authenticate_action(token=token, user=user, dataset=datasetName):
        data = """{{"query":{{"bool":{{"must":[{{"match":{{"timestampMillis":{timestamp}}}}},
            {{"match":{{"urn":"{urn}"}}}}]}}}}}}""".format(
            urn=datasetName, timestamp=str(item.timestamp)
        )
        response = requests.post(
            "{es_host}/dataset_datasetprofileaspect_v1/_delete_by_query".format(
                es_host=elastic_host
            ),
            headers=headers,
            data=data,
        )
        results = response.json()
        return JSONResponse(
            content={"message": results.get("message", "")}, status_code=response.status_code
        )        
    else:
        rootLogger.info(
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
        results = response.json()
        return JSONResponse(
            content=results.get("message", ""), status_code=response.status_code
        )
    else:
        rootLogger.info(
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
        dataset_snapshot = DatasetSnapshot(
            urn=datasetName,
            aspects=[],
        )
        description = item.description
        properties = item.properties
        all_properties = {}
        for prop in properties:
            if "propertyKey" and "propertyValue" in prop:
                all_properties[prop.get("propertyKey")] = prop.get("propertyValue")
        property_aspect = make_dataset_description_mce(
            dataset_name=datasetName,
            description=description,
            customProperties=all_properties,
        )
        dataset_snapshot.aspects.append(property_aspect)
        metadata_record = MetadataChangeEvent(
            proposedSnapshot=dataset_snapshot,
            systemMetadata=SystemMetadataClass(
                runId=f"{datasetName}_prop_{str(int(time.time()))}"
            ),
        )
        response = emit_mce_respond(
            metadata_record=metadata_record,
            owner=item.requestor,
            event="UI Update Properties",
            token=token,
        )
        results = response.json()
        return JSONResponse(
            content={"message": results.get("message", "")}, status_code=response.status_code
        ) 
    else:
        rootLogger.info(
            f"authentication failed for request(update_props) from {user}"
        )
        return JSONResponse(content={"message": "Authentication Failed"}, status_code=401)


def emit_mce_respond(
    metadata_record: MetadataChangeEvent, owner: str, event: str, token: str
) -> dict():
    datasetName = metadata_record.proposedSnapshot.urn
    for mce in metadata_record.proposedSnapshot.aspects:
        if not mce.validate():
            rootLogger.error(f"{mce.__class__} is not defined properly")
            return JSONResponse(
                content = {
                    "messsage": f"MCE was incorrectly defined.\
                        {event} was aborted",
                },
                status_code = 400,
            )

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
        return JSONResponse(
            content={
                "messsage": f"{event} failed because upstream error {e}",
            },
            status_code= 500,
        )
    rootLogger.info(
        f"{event} {datasetName} requested_by {owner} completed successfully"
    )
    return JSONResponse(
        content={        
        "messsage": f"{event} completed successfully",
        },
        status_code = 201,
    )

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
        return JSONResponse(
            content = {
                "messsage": f"{event} failed because upstream error {e}",                
            },
            status_code = 500
        )
    rootLogger.info(
        f"{event} {datasetName} requested_by {owner} completed successfully"
    )
    return JSONResponse(
        content = {        
            "messsage": f"{event} completed successfully",
        },
        status_code = 201,
    )  

@app.post("/custom/make_dataset")
async def create_item(item: create_dataset_params) -> None:
    """
    This endpoint is meant for manually defined or parsed file datasets.
    #todo - to revisit to see if refactoring is needed when make_json is up.
    """
    rootLogger.info("make_dataset_request_received from {}".format(item.requestor))
    rootLogger.debug("make_dataset_request_received {}".format(item))
    item.dataset_type = determine_type(item.dataset_type)
    token = item.user_token
    user = item.dataset_owner
    if verify_token(token, user):
        item.dataset_name = "{}_{}".format(item.dataset_name, str(get_sys_time()))
        datasetName = make_dataset_urn(item.dataset_type, item.dataset_name)
        platformName = make_platform(item.dataset_type)
        item.browsepathList = [
            item + "/" if not item.endswith("/") else item
            for item in item.browsepathList
        ]
        # this line is in case the endpoint is called by API and not UI,
        # which will enforce ending with /.
        browsepaths = [path + "dataset" for path in item.browsepathList]

        requestor = make_user_urn(item.dataset_owner)
        headerRowNum = (
            "n/a"
            if item.dict().get("hasHeader", "n/a") == "no"
            else str(item.dict().get("headerLine", "n/a"))
        )
        properties = {
            "dataset_origin": item.dict().get("dataset_origin", ""),
            "dataset_location": item.dict().get("dataset_location", ""),
            "has_header": item.dict().get("hasHeader", "n/a"),
            "header_row_number": headerRowNum,
        }
        if item.dataset_type == "json":  # json has no headers
            properties.pop("has_header")
            properties.pop("header_row_number")

        dataset_description = (
            item.dataset_description if item.dataset_description else ""
        )
        dataset_snapshot = DatasetSnapshot(
            urn=datasetName,
            aspects=[],
        )
        dataset_snapshot.aspects.append(
            make_dataset_description_mce(
                dataset_name=datasetName,
                description=dataset_description,
                customProperties=properties,
            )
        )

        dataset_snapshot.aspects.append(
            make_ownership_mce(actor=requestor, dataset_urn=datasetName)
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
        response = emit_mce_respond(
            metadata_record=metadata_record,
            owner=requestor,
            event="Create Dataset",
            token=token,
        )
        results = response.json()
        
        return JSONResponse(
            content={"message": f"{datasetName}"}, status_code=response.status_code
        ) 
    else:
        rootLogger.info("make_dataset request failed from {}".format(requestor))
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
        results = response.json()
        return JSONResponse(
            content={"message": results.get("message", "")}, status_code=response.status_code
        )
    else:
        rootLogger.info(
            f"authentication failed for request\
            (update_schema) from {user}"
        )
        return JSONResponse(content={"message": "Authentication Failed"}, status_code=401)

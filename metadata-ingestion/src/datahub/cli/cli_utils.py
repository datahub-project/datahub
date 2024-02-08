import json
import logging
import os
import os.path
import sys
import typing
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, Union

import click
import requests
from deprecated import deprecated
from requests.models import Response
from requests.sessions import Session

from datahub.cli import config_utils
from datahub.emitter.aspect import ASPECT_MAP, TIMESERIES_ASPECT_MAP
from datahub.emitter.request_helper import make_curl_command
from datahub.emitter.serialization_helper import post_json_transform
from datahub.metadata.schema_classes import _Aspect
from datahub.utilities.urns.urn import Urn, guess_entity_type

log = logging.getLogger(__name__)

ENV_METADATA_HOST_URL = "DATAHUB_GMS_URL"
ENV_METADATA_HOST = "DATAHUB_GMS_HOST"
ENV_METADATA_PORT = "DATAHUB_GMS_PORT"
ENV_METADATA_PROTOCOL = "DATAHUB_GMS_PROTOCOL"
ENV_METADATA_TOKEN = "DATAHUB_GMS_TOKEN"
ENV_DATAHUB_SYSTEM_CLIENT_ID = "DATAHUB_SYSTEM_CLIENT_ID"
ENV_DATAHUB_SYSTEM_CLIENT_SECRET = "DATAHUB_SYSTEM_CLIENT_SECRET"

config_override: Dict = {}

# TODO: Many of the methods in this file duplicate logic that already lives
# in the DataHubGraph client. We should refactor this to use the client instead.
# For the methods that aren't duplicates, that logic should be moved to the client.


def set_env_variables_override_config(url: str, token: Optional[str]) -> None:
    """Should be used to override the config when using rest emitter"""
    config_override[ENV_METADATA_HOST_URL] = url
    if token is not None:
        config_override[ENV_METADATA_TOKEN] = token


def get_details_from_env() -> Tuple[Optional[str], Optional[str]]:
    host = os.environ.get(ENV_METADATA_HOST)
    port = os.environ.get(ENV_METADATA_PORT)
    token = os.environ.get(ENV_METADATA_TOKEN)
    protocol = os.environ.get(ENV_METADATA_PROTOCOL, "http")
    url = os.environ.get(ENV_METADATA_HOST_URL)
    if port is not None:
        url = f"{protocol}://{host}:{port}"
        return url, token
    # The reason for using host as URL is backward compatibility
    # If port is not being used we assume someone is using host env var as URL
    if url is None and host is not None:
        log.warning(
            f"Do not use {ENV_METADATA_HOST} as URL. Use {ENV_METADATA_HOST_URL} instead"
        )
    return url or host, token


def first_non_null(ls: List[Optional[str]]) -> Optional[str]:
    return next((el for el in ls if el is not None and el.strip() != ""), None)


def get_system_auth() -> Optional[str]:
    system_client_id = os.environ.get(ENV_DATAHUB_SYSTEM_CLIENT_ID)
    system_client_secret = os.environ.get(ENV_DATAHUB_SYSTEM_CLIENT_SECRET)
    if system_client_id is not None and system_client_secret is not None:
        return f"Basic {system_client_id}:{system_client_secret}"
    return None


def get_url_and_token():
    gms_host_env, gms_token_env = get_details_from_env()
    if len(config_override.keys()) > 0:
        gms_host = config_override.get(ENV_METADATA_HOST_URL)
        gms_token = config_override.get(ENV_METADATA_TOKEN)
    elif config_utils.should_skip_config():
        gms_host = gms_host_env
        gms_token = gms_token_env
    else:
        config_utils.ensure_datahub_config()
        gms_host_conf, gms_token_conf = config_utils.get_details_from_config()
        gms_host = first_non_null([gms_host_env, gms_host_conf])
        gms_token = first_non_null([gms_token_env, gms_token_conf])
    return gms_host, gms_token


def get_token():
    return get_url_and_token()[1]


def get_session_and_host():
    session = requests.Session()

    gms_host, gms_token = get_url_and_token()

    if gms_host is None or gms_host.strip() == "":
        log.error(
            f"GMS Host is not set. Use datahub init command or set {ENV_METADATA_HOST_URL} env var"
        )
        return None, None

    session.headers.update(
        {
            "X-RestLi-Protocol-Version": "2.0.0",
            "Content-Type": "application/json",
        }
    )
    if isinstance(gms_token, str) and len(gms_token) > 0:
        session.headers.update(
            {"Authorization": f"Bearer {gms_token.format(**os.environ)}"}
        )

    return session, gms_host


def test_connection():
    (session, host) = get_session_and_host()
    url = f"{host}/config"
    response = session.get(url)
    response.raise_for_status()


def test_connectivity_complain_exit(operation_name: str) -> None:
    """Test connectivity to metadata-service, log operation name and exit"""
    # First test connectivity
    try:
        test_connection()
    except Exception as e:
        click.secho(
            f"Failed to connect to DataHub server at {get_session_and_host()[1]}. Run with datahub --debug {operation_name} ... to get more information.",
            fg="red",
        )
        log.debug(f"Failed to connect with {e}")
        sys.exit(1)


def parse_run_restli_response(response: requests.Response) -> dict:
    response_json = response.json()
    if response.status_code != 200:
        if isinstance(response_json, dict):
            if "message" in response_json:
                click.secho("Failed to execute operation", fg="red")
                click.secho(f"{response_json['message']}", fg="red")
            else:
                click.secho(f"Failed with \n{response_json}", fg="red")
        else:
            response.raise_for_status()
        exit()

    if not isinstance(response_json, dict):
        click.echo(
            f"Received error, please check your {config_utils.CONDENSED_DATAHUB_CONFIG_PATH}"
        )
        click.echo()
        click.echo(response_json)
        exit()

    summary = response_json.get("value")
    if not isinstance(summary, dict):
        click.echo(
            f"Received error, please check your {config_utils.CONDENSED_DATAHUB_CONFIG_PATH}"
        )
        click.echo()
        click.echo(response_json)
        exit()

    return summary


def format_aspect_summaries(summaries: list) -> typing.List[typing.List[str]]:
    local_timezone = datetime.now().astimezone().tzinfo
    return [
        [
            row.get("urn"),
            row.get("aspectName"),
            datetime.fromtimestamp(row.get("timestamp") / 1000).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            + f" ({local_timezone})",
        ]
        for row in summaries
    ]


def post_rollback_endpoint(
    payload_obj: dict,
    path: str,
) -> typing.Tuple[typing.List[typing.List[str]], int, int, int, int, typing.List[dict]]:
    session, gms_host = get_session_and_host()
    url = gms_host + path

    payload = json.dumps(payload_obj)
    response = session.post(url, payload)

    summary = parse_run_restli_response(response)
    rows = summary.get("aspectRowSummaries", [])
    entities_affected = summary.get("entitiesAffected", 0)
    aspects_reverted = summary.get("aspectsReverted", 0)
    aspects_affected = summary.get("aspectsAffected", 0)
    unsafe_entity_count = summary.get("unsafeEntitiesCount", 0)
    unsafe_entities = summary.get("unsafeEntities", [])
    rolled_back_aspects = list(
        filter(lambda row: row["runId"] == payload_obj["runId"], rows)
    )

    if len(rows) == 0:
        click.secho(f"No entities found. Payload used: {payload}", fg="yellow")

    structured_rolled_back_results = format_aspect_summaries(rolled_back_aspects)
    return (
        structured_rolled_back_results,
        entities_affected,
        aspects_reverted,
        aspects_affected,
        unsafe_entity_count,
        unsafe_entities,
    )


@deprecated(reason="Use DataHubGraph.get_urns_by_filter instead")
def get_urns_by_filter(
    platform: Optional[str],
    env: Optional[str] = None,
    entity_type: str = "dataset",
    search_query: str = "*",
    include_removed: bool = False,
    only_soft_deleted: Optional[bool] = None,
) -> Iterable[str]:
    # TODO: Replace with DataHubGraph call
    session, gms_host = get_session_and_host()
    endpoint: str = "/entities?action=search"
    url = gms_host + endpoint
    filter_criteria = []
    entity_type_lower = entity_type.lower()
    if env and entity_type_lower != "container":
        filter_criteria.append({"field": "origin", "value": env, "condition": "EQUAL"})
    if (
        platform is not None
        and entity_type_lower == "dataset"
        or entity_type_lower == "dataflow"
        or entity_type_lower == "datajob"
        or entity_type_lower == "container"
    ):
        filter_criteria.append(
            {
                "field": "platform.keyword",
                "value": f"urn:li:dataPlatform:{platform}",
                "condition": "EQUAL",
            }
        )
    if platform is not None and entity_type_lower in {"chart", "dashboard"}:
        filter_criteria.append(
            {
                "field": "tool",
                "value": platform,
                "condition": "EQUAL",
            }
        )

    if only_soft_deleted:
        filter_criteria.append(
            {
                "field": "removed",
                "value": "true",
                "condition": "EQUAL",
            }
        )
    elif include_removed:
        filter_criteria.append(
            {
                "field": "removed",
                "value": "",  # accept anything regarding removed property (true, false, non-existent)
                "condition": "EQUAL",
            }
        )

    search_body = {
        "input": search_query,
        "entity": entity_type,
        "start": 0,
        "count": 10000,
        "filter": {"or": [{"and": filter_criteria}]},
    }
    payload = json.dumps(search_body)
    log.debug(payload)
    response: Response = session.post(url, payload)
    if response.status_code == 200:
        assert response._content
        results = json.loads(response._content)
        num_entities = results["value"]["numEntities"]
        entities_yielded: int = 0
        for x in results["value"]["entities"]:
            entities_yielded += 1
            log.debug(f"yielding {x['entity']}")
            yield x["entity"]
        if entities_yielded != num_entities:
            log.warning(
                f"Discrepancy in entities yielded {entities_yielded} and num entities {num_entities}. This means all entities may not have been deleted."
            )
    else:
        log.error(f"Failed to execute search query with {str(response.content)}")
        response.raise_for_status()


def get_container_ids_by_filter(
    env: Optional[str],
    entity_type: str = "container",
    search_query: str = "*",
) -> Iterable[str]:
    session, gms_host = get_session_and_host()
    endpoint: str = "/entities?action=search"
    url = gms_host + endpoint

    container_filters = []
    for container_subtype in ["Database", "Schema", "Project", "Dataset"]:
        filter_criteria = []

        filter_criteria.append(
            {
                "field": "customProperties",
                "value": f"instance={env}",
                "condition": "EQUAL",
            }
        )

        filter_criteria.append(
            {
                "field": "typeNames",
                "value": container_subtype,
                "condition": "EQUAL",
            }
        )
        container_filters.append({"and": filter_criteria})
    search_body = {
        "input": search_query,
        "entity": entity_type,
        "start": 0,
        "count": 10000,
        "filter": {"or": container_filters},
    }
    payload = json.dumps(search_body)
    log.debug(payload)
    response: Response = session.post(url, payload)
    if response.status_code == 200:
        assert response._content
        log.debug(response._content)
        results = json.loads(response._content)
        num_entities = results["value"]["numEntities"]
        entities_yielded: int = 0
        for x in results["value"]["entities"]:
            entities_yielded += 1
            log.debug(f"yielding {x['entity']}")
            yield x["entity"]
        assert (
            entities_yielded == num_entities
        ), "Did not delete all entities, try running this command again!"
    else:
        log.error(f"Failed to execute search query with {str(response.content)}")
        response.raise_for_status()


def batch_get_ids(
    ids: List[str],
) -> Iterable[Dict]:
    session, gms_host = get_session_and_host()
    endpoint: str = "/entitiesV2"
    url = gms_host + endpoint
    ids_to_get = [Urn.url_encode(id) for id in ids]
    response = session.get(
        f"{url}?ids=List({','.join(ids_to_get)})",
    )

    if response.status_code == 200:
        assert response._content
        log.debug(response._content)
        results = json.loads(response._content)
        num_entities = len(results["results"])
        entities_yielded: int = 0
        for x in results["results"].values():
            entities_yielded += 1
            log.debug(f"yielding {x}")
            yield x
        assert (
            entities_yielded == num_entities
        ), "Did not delete all entities, try running this command again!"
    else:
        log.error(f"Failed to execute batch get with {str(response.content)}")
        response.raise_for_status()


def get_incoming_relationships(urn: str, types: List[str]) -> Iterable[Dict]:
    yield from get_relationships(urn=urn, types=types, direction="INCOMING")


def get_outgoing_relationships(urn: str, types: List[str]) -> Iterable[Dict]:
    yield from get_relationships(urn=urn, types=types, direction="OUTGOING")


def get_relationships(urn: str, types: List[str], direction: str) -> Iterable[Dict]:
    session, gms_host = get_session_and_host()
    encoded_urn: str = Urn.url_encode(urn)
    types_param_string = "List(" + ",".join(types) + ")"
    endpoint: str = f"{gms_host}/relationships?urn={encoded_urn}&direction={direction}&types={types_param_string}"
    response: Response = session.get(endpoint)
    if response.status_code == 200:
        results = response.json()
        log.debug(f"Relationship response: {results}")
        num_entities = results["count"]
        entities_yielded: int = 0
        for x in results["relationships"]:
            entities_yielded += 1
            yield x
        if entities_yielded != num_entities:
            log.warn("Yielded entities differ from num entities")
    else:
        log.error(f"Failed to execute relationships query with {str(response.content)}")
        response.raise_for_status()


def get_entity(
    urn: str,
    aspect: Optional[List] = None,
    cached_session_host: Optional[Tuple[Session, str]] = None,
) -> Dict:
    session, gms_host = cached_session_host or get_session_and_host()
    if urn.startswith("urn%3A"):
        # we assume the urn is already encoded
        encoded_urn: str = urn
    elif urn.startswith("urn:"):
        encoded_urn = Urn.url_encode(urn)
    else:
        raise Exception(
            f"urn {urn} does not seem to be a valid raw (starts with urn:) or encoded urn (starts with urn%3A)"
        )

    # TODO: Replace with DataHubGraph.get_entity_raw.
    endpoint: str = f"/entitiesV2/{encoded_urn}"

    if aspect and len(aspect):
        endpoint = f"{endpoint}?aspects=List(" + ",".join(aspect) + ")"

    response = session.get(gms_host + endpoint)
    response.raise_for_status()
    return response.json()


def post_entity(
    urn: str,
    entity_type: str,
    aspect_name: str,
    aspect_value: Dict,
    cached_session_host: Optional[Tuple[Session, str]] = None,
    is_async: Optional[str] = "false",
) -> int:
    session, gms_host = cached_session_host or get_session_and_host()
    endpoint: str = "/aspects/?action=ingestProposal"

    proposal = {
        "proposal": {
            "entityType": entity_type,
            "entityUrn": urn,
            "aspectName": aspect_name,
            "changeType": "UPSERT",
            "aspect": {
                "contentType": "application/json",
                "value": json.dumps(aspect_value),
            },
        },
        "async": is_async,
    }
    payload = json.dumps(proposal)
    url = gms_host + endpoint
    curl_command = make_curl_command(session, "POST", url, payload)
    log.debug(
        "Attempting to emit to DataHub GMS; using curl equivalent to:\n%s",
        curl_command,
    )
    response = session.post(url, payload)
    if not response.ok:
        try:
            log.info(response.json()["message"].strip())
        except Exception:
            log.info(f"post_entity failed: {response.text}")
    response.raise_for_status()
    return response.status_code


def _get_pydantic_class_from_aspect_name(aspect_name: str) -> Optional[Type[_Aspect]]:
    return ASPECT_MAP.get(aspect_name)


def get_latest_timeseries_aspect_values(
    entity_urn: str,
    timeseries_aspect_name: str,
    cached_session_host: Optional[Tuple[Session, str]],
) -> Dict:
    session, gms_host = cached_session_host or get_session_and_host()
    query_body = {
        "urn": entity_urn,
        "entity": guess_entity_type(entity_urn),
        "aspect": timeseries_aspect_name,
        "latestValue": True,
    }
    end_point = "/aspects?action=getTimeseriesAspectValues"
    try:
        response = session.post(url=gms_host + end_point, data=json.dumps(query_body))
        response.raise_for_status()
        return response.json()
    except Exception:
        # Ignore exceptions
        return {}


def get_aspects_for_entity(
    entity_urn: str,
    aspects: List[str],
    typed: bool = False,
    cached_session_host: Optional[Tuple[Session, str]] = None,
) -> Dict[str, Union[dict, _Aspect]]:
    # Process non-timeseries aspects
    non_timeseries_aspects = [a for a in aspects if a not in TIMESERIES_ASPECT_MAP]
    entity_response = get_entity(
        entity_urn, non_timeseries_aspects, cached_session_host
    )
    aspect_list: Dict[str, dict] = entity_response["aspects"]

    # Process timeseries aspects & append to aspect_list
    timeseries_aspects: List[str] = [a for a in aspects if a in TIMESERIES_ASPECT_MAP]
    for timeseries_aspect in timeseries_aspects:
        timeseries_response: Dict = get_latest_timeseries_aspect_values(
            entity_urn, timeseries_aspect, cached_session_host
        )
        values: List[Dict] = timeseries_response.get("value", {}).get("values", [])
        if values:
            aspect_cls: Optional[Type] = _get_pydantic_class_from_aspect_name(
                timeseries_aspect
            )
            if aspect_cls is not None:
                ts_aspect = values[0]["aspect"]
                # Decode the json-encoded generic aspect value.
                ts_aspect["value"] = json.loads(ts_aspect["value"])
                aspect_list[timeseries_aspect] = ts_aspect

    aspect_map: Dict[str, Union[dict, _Aspect]] = {}
    for aspect_name, a in aspect_list.items():
        aspect_py_class: Optional[Type[Any]] = _get_pydantic_class_from_aspect_name(
            aspect_name
        )

        aspect_dict = a["value"]
        if not typed:
            aspect_map[aspect_name] = aspect_dict
        elif aspect_py_class:
            try:
                post_json_obj = post_json_transform(aspect_dict)
                aspect_map[aspect_name] = aspect_py_class.from_obj(post_json_obj)
            except Exception as e:
                log.error(f"Error on {json.dumps(aspect_dict)}", e)
        else:
            log.debug(f"Failed to find class for aspect {aspect_name}")

    if aspects:
        return {k: v for (k, v) in aspect_map.items() if k in aspects}
    else:
        return dict(aspect_map)


def make_shim_command(name: str, suggestion: str) -> click.Command:
    @click.command(
        name=name,
        context_settings=dict(
            ignore_unknown_options=True,
            allow_extra_args=True,
        ),
    )
    @click.pass_context
    def command(ctx: click.Context) -> None:
        """<disabled due to missing dependencies>"""

        click.secho(
            "This command is disabled due to missing dependencies. "
            f"Please {suggestion} to enable it.",
            fg="red",
        )
        ctx.exit(1)

    return command


def get_session_login_as(
    username: str, password: str, frontend_url: str
) -> requests.Session:
    session = requests.Session()
    headers = {
        "Content-Type": "application/json",
    }
    system_auth = get_system_auth()
    if system_auth is not None:
        session.headers.update({"Authorization": system_auth})
    else:
        data = '{"username":"' + username + '", "password":"' + password + '"}'
        response = session.post(f"{frontend_url}/logIn", headers=headers, data=data)
        response.raise_for_status()
    return session


def _ensure_valid_gms_url_acryl_cloud(url: str) -> str:
    if "acryl.io" not in url:
        return url
    if url.startswith("http://"):
        url = url.replace("http://", "https://")
    if url.endswith("acryl.io"):
        url = f"{url}/gms"
    return url


def fixup_gms_url(url: str) -> str:
    if url is None:
        return ""
    if url.endswith("/"):
        url = url.rstrip("/")
    url = _ensure_valid_gms_url_acryl_cloud(url)
    return url


def guess_frontend_url_from_gms_url(gms_url: str) -> str:
    gms_url = fixup_gms_url(gms_url)
    url = gms_url
    if url.endswith("/gms"):
        url = gms_url.rstrip("/gms")
    if url.endswith("8080"):
        url = url[:-4] + "9002"
    return url


def generate_access_token(
    username: str,
    password: str,
    gms_url: str,
    token_name: Optional[str] = None,
    validity: str = "ONE_HOUR",
) -> Tuple[str, str]:
    frontend_url = guess_frontend_url_from_gms_url(gms_url)
    session = get_session_login_as(
        username=username,
        password=password,
        frontend_url=frontend_url,
    )
    now = datetime.now()
    timestamp = now.astimezone().isoformat()
    if token_name is None:
        token_name = f"cli token {timestamp}"
    json = {
        "query": """mutation createAccessToken($input: CreateAccessTokenInput!) {
            createAccessToken(input: $input) {
              accessToken
              metadata {
                id
                actorUrn
                ownerUrn
                name
                description
              }
            }
        }""",
        "variables": {
            "input": {
                "type": "PERSONAL",
                "actorUrn": f"urn:li:corpuser:{username}",
                "duration": validity,
                "name": token_name,
            }
        },
    }
    response = session.post(f"{frontend_url}/api/v2/graphql", json=json)
    response.raise_for_status()
    return token_name, response.json().get("data", {}).get("createAccessToken", {}).get(
        "accessToken", None
    )

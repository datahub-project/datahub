import json
import logging
import re
from typing import Dict, Generator, Optional

import requests
import yaml
from requests.auth import HTTPBasicAuth

logger: logging.Logger = logging.getLogger(__name__)


def request_call(
        url: str, token: str = None, username: str = None, password: str = None
) -> requests.Response:
    headers = {"accept": "application/json"}

    if username is not None and password is not None:
        return requests.get(
            url, headers=headers, auth=HTTPBasicAuth(username, password)
        )

    elif token is not None:
        headers["Authorization"] = f"Bearer {token}"
        return requests.get(url, headers=headers)
    else:
        return requests.get(url, headers=headers)

def get_swag_json(
        url: str,
        token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        swagger_file: str = "",
) -> Dict:
    tot_url = url + swagger_file
    if token is not None:
        response = request_call(url=tot_url, token=token)
    else:
        response = request_call(url=tot_url, username=username, password=password)

    if response.status_code != 200:
        raise Exception(f"Unable to retrieve {tot_url}, error {response.status_code}")
    try:
        dict_data = json.loads(response.content)
    except json.JSONDecodeError:  # it's not a JSON!
        dict_data = yaml.safe_load(response.content)
    return dict_data

def get_tok(
        url: str,
        username: Optional[str] = "",
        password: Optional[str] = "",
        tok_url: Optional[str] = "",
        method: str = "post",
) -> str:
    """
    Trying to post username/password to get auth.
    """
    token = ""
    url4req = url + tok_url
    if method == "post":
        # this will make a POST call with username and password
        data = {"username": username, "password": password}
        # url2post = url + "api/authenticate/"
        response = requests.post(url4req, data=data)
        if response.status_code == 200:
            cont = json.loads(response.content)
            token = cont["tokens"]["access"]
    elif method == "get":
        # this will make a GET call with username and password
        response = requests.get(url4req)
        if response.status_code == 200:
            cont = json.loads(response.content)
            token = cont["token"]
    else:
        raise ValueError(f"Method unrecognised: {method}")
    if token != "":
        return token
    else:
        raise Exception(f"Unable to get a valid token: {response.text}")

def get_swagger_list(sw_dict: dict) -> list:  # noqa: C901
    """
    Get all the interfaces from target JSON file
    """
    resultList = []

    for jsonObject in sw_dict:
        isApiProvidedByPresent = False
        isTargetKindComponent = False

        # Check whether there's a section with "type == apiProvidedBy" under "relations"
        # (otherwise information about the system providing the given interface is missing).
        if "relations" in jsonObject:
            relationsList = jsonObject["relations"]
            for relation in relationsList:
                if "apiProvidedBy" in relation["type"]:
                    isApiProvidedByPresent = True

        # Check whether there's a key "kind" in that section and its value is "component"
        # (i.e. information about the system is still missing).
                if "kind" in jsonObject:
                    if "component" in relation["target"]["kind"]:
                        isTargetKindComponent = True
                else:
                    logger.info(
                        "Missing information about the system providing this interface!"
                    )
        else:
            continue

        #  Check whether "kind == API" for the retrieved record
        if "API" not in jsonObject["kind"]:
            logger.info(
                "Retrieved record is not of type 'API' - is the filter in Backstage URL set correctly?!"
            )
            continue

        # Check whether section "metadata" exists
        if "metadata" in jsonObject:
            metadata = jsonObject["metadata"]
            if "annotations" in metadata:
                annotations = metadata["annotations"]
                # Check whether there's "backstage.io/managed-by-location" under "annotations"...
                if "backstage.io/managed-by-location" in annotations:
                    manByLoc = annotations["backstage.io/managed-by-location"]

                    # ...and whether the provided value starts with "url:"
                    if not manByLoc.startswith("url:"):
                        logger.info(
                            "Value for 'backstage.io/managed-by-location' does not start with 'url:'!"
                        )
                        continue
                else:
                    logger.info(
                        "Element 'backstage.io/managed-by-location' not found under 'annotations', skipping item without reference to VCS / source!"
                    )
                    continue

                # Check whether there is a key "backstage.io/orphan" under "annotations"...
                if "backstage.io/orphan" in annotations:
                    orphan = annotations["backstage.io/orphan"]

                    # ...and whether it starts with "true" (probably an outdated record)
                    if orphan.startswith("true"):
                        logger.info(
                            "Value for 'backstage.io/orphan' is 'true' => skipping an outdated record!"
                        )
                        continue
            else:
                logger.info(
                    "Section 'annotations' is missing!"
                )
                continue
        else:
            logger.info(
                "Section 'metadata' is missing!"
            )
            continue

        # Check whether there's section "spec"
        if "spec" in jsonObject:
            spec = jsonObject["spec"]
            # Check whether there's information about interface type
            if "type" not in spec:
                logger.info(
                    "Interface type is missing!"
                )
                continue
            # Check whether "type == openapi" (only OpenAPI interfaces are supported)
            elif "openapi" not in spec["type"]:
                logger.warning(
                    "Unsupported interface type, skipping..."
                )
                continue

            # Check availability of "definition"
            if "definition" in spec:
                definitionSwagger = spec["definition"]
            else:
                logger.info(
                    "Interface definition missing!"
                )
                continue
        else:
            logger.info(
                "Section 'spec' missing!"
            )
            continue

        if (isApiProvidedByPresent is True) and (isTargetKindComponent is True):
            resultList.append(definitionSwagger)

    return resultList

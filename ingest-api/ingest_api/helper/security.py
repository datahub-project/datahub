# flake8: noqa

import json
import logging
import os
from datetime import datetime as dt
from urllib.parse import urljoin

import jwt
import requests
from jwt import ExpiredSignatureError, InvalidTokenError

log = logging.getLogger("ingest")
logformatter = logging.Formatter("%(asctime)s;%(levelname)s;%(funcName)s;%(message)s")
log.setLevel(logging.DEBUG)

datahub_url = os.environ["DATAHUB_FRONTEND"]
CLI_MODE = False if os.environ.get("RUNNING_IN_DOCKER") else True
if CLI_MODE:
    os.environ["JWT_SECRET"] = "WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI94="
    os.environ["DATAHUB_AUTHENTICATE_INGEST"] = "True"
    os.environ["DATAHUB_FRONTEND"] = "http://172.19.0.1:9002"


def verify_token(token: str, user: str):
    token_secret = os.environ["JWT_SECRET"]
    log.error(f"signature secret is {token_secret}")
    try:
        payload = jwt.decode(token, token_secret, algorithms="HS256")
        if payload["actorId"] == "impossible":
            raise Exception(
                "User Impossible has occurred. Something has gone very wrong."
            )
        exp_datetime = dt.fromtimestamp(int(payload["exp"]))
        if payload["actorId"] == user:
            log.error(
                f"token verified for {user}, expires \
                    {exp_datetime.strftime('%Y:%m:%d %H:%M')}"
            )
            return True
        log.error("user id does not match token payload user id!")
        return False
    except ExpiredSignatureError:
        log.error("token has expired!")
        return False
    except InvalidTokenError:
        log.error(f"Invalid token for {user}")
        return False
    except Exception as e:
        log.error(f"I cant figure out this token for {user}, error {e}")
        return False


def authenticate_action(token: str, user: str, dataset: str):
    if "DATAHUB_AUTHENTICATE_INGEST" in os.environ:
        must_authenticate_actions = (
            True if os.environ["DATAHUB_AUTHENTICATE_INGEST"] == "yes" else False
        )
    else:
        must_authenticate_actions = False
    log.error(f"Authenticate user setting is {must_authenticate_actions}")
    log.error(f"Dataset being updated is {dataset}, requestor is {user}")
    if must_authenticate_actions:
        if verify_token(token, user) and query_dataset_owner(token, dataset, user):
            log.error(f"user {user} is authorized to do something")
            return True
        else:
            log.error(f"user {user} is NOT authorized to do something")
            return False
    else:  # no need to authenticate, so always true
        return True


def query_dataset_owner(token: str, dataset_urn: str, user: str):
    """
    Currently only queries users associated with dataset.
    Does not query members of groups that owns the dataset,
    because that query is more complicated - to be expanded next time.
    Also, currently UI checks also only check for individual owners,
    not owner groups.
    """
    log.info(f"UI endpoint is {datahub_url}")
    query_endpoint = urljoin(datahub_url, "/api/graphql")
    log.info(f"I will query {query_endpoint} as {user}")
    headers = {}
    headers["Authorization"] = f"Bearer {token}"
    headers["Content-Type"] = "application/json"
    query = """
        query owner($urn: String!){
            dataset(urn: $urn) {
                ownership{
                    owners{
                        __typename
                        owner{
                        ... on CorpUser{
                            username
                            }
                        }
                    }
                }
            }
        }
    """
    variables = {"urn": dataset_urn}
    resp = requests.post(
        query_endpoint, headers=headers, json={"query": query, "variables": variables}
    )
    log.info(f"resp.status_code is {resp.status_code}")
    if resp.status_code != 200:
        return False
    data_received = json.loads(resp.text)
    owners_list = data_received["data"]["dataset"]["ownership"]["owners"]
    log.info(f"The list of owners for this dataset is {owners_list}")
    owners = [item["owner"]["username"] for item in owners_list]
    if user not in owners:
        log.error("Ownership Step: False")
        return False
    log.info("Ownership Step: True")
    return True

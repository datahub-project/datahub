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
    # log.debug(f"signature secret is {token_secret}")
    try:
        payload = jwt.decode(token, token_secret, algorithms="HS256")
        # userUrn = impossible is set in the frontend-react as a temporary userUrn while graphQL is returning identity info.
        if payload["actorId"] == "impossible": 
            log.error("User Impossible has appeared. Something has gone very wrong.")
        exp_datetime = dt.fromtimestamp(int(payload["exp"]))
        if payload["actorId"] == user:
            log.info(
                f"token verified for {user}, expires {exp_datetime.strftime('%Y:%m:%d %H:%M')}"
            )
            return True
        log.error(f"user id does not match token payload user id! token:{token}")
        return False
    except ExpiredSignatureError:
        log.error(f"token has expired! token:{token}")
        return False
    except InvalidTokenError:
        log.error(f"Invalid token for {user} token:{token}")
        return False
    except Exception as e:
        log.error(f"I cant figure out this token for {user}, so its an error {e}. token:{token}")
        return False


def authenticate_action(token: str, user: str, dataset: str):
    if "DATAHUB_AUTHENTICATE_INGEST" in os.environ:
        must_authenticate_actions = (
            True if os.environ["DATAHUB_AUTHENTICATE_INGEST"] == "yes" else False
        )
    else:
        must_authenticate_actions = False
    log.debug(f"Authenticate user setting is {must_authenticate_actions}")
    log.debug(f"Dataset being updated is {dataset}, requestor is {user}")
    if must_authenticate_actions:
        if verify_token(token, user) and query_dataset_owner(token, dataset, user):
            log.debug(f"user {user} is authorized to do something")
            return True
        else:
            log.debug(f"user {user} is NOT authorized to do something")
            return False
    else:  # no need to authenticate, so always true
        return True


def query_dataset_owner(token: str, dataset_urn: str, user: str):
    """
    Queries for owners of dataset. If there are group owners, then will fire another query to check if user is member of group.        
    """
    # log.debug(f"UI endpoint is {datahub_url}")
    user_urn = f"urn:li:corpuser:{user}"
    query_endpoint = urljoin(datahub_url, "/api/graphql")
    log.debug(f"I will query {query_endpoint} as {user}")
    
    owners_list = query_dataset_ownership(token, dataset_urn, query_endpoint)
    log.debug(f"The list of owners for this dataset is {owners_list}")
    individual_owners = [item["owner"]["urn"] for item in owners_list if item["owner"]["__typename"]=="CorpUser"]
    if user_urn in individual_owners:
        log.debug("Individual Ownership Step: True")
        return True    
    group_owners = [item["owner"]["urn"] for item in owners_list if item["owner"]["__typename"]=="CorpGroup"]
    if len(group_owners) > 0:
        groups = query_users_groups(token, query_endpoint)
        log.debug(f"The list of groups for this user is {groups}")
        groups_urn = [item["entity"]["urn"] for item in groups]
        for item in groups_urn:
            if item in group_owners:
                log.debug(f"Group Ownership Step: True for {item}.")
                return True 
    log.error("Ownership Step: False")
    return False
    
def query_dataset_ownership(token: str, dataset_urn:str, query_endpoint:str):
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
                            __typename
                            urn
                            }
                        ... on CorpGroup{
                            __typename
                            urn
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
    log.debug(f"resp.status_code is {resp.status_code}")
    if resp.status_code != 200:
        return []
    data_received = json.loads(resp.text)
    log.error(f"received from graphql ownership info: {data_received}")
    owners_list = data_received["data"]["dataset"]["ownership"]["owners"]
    return owners_list

def query_users_groups(token: str, query_endpoint: str):
    headers = {}
    headers["Authorization"] = f"Bearer {token}"
    headers["Content-Type"] = "application/json"
    query = """
        query test ($urn: String!){
            corpUser(urn:$urn){
                relationships(input:{
                types: "IsMemberOfGroup"
                direction: OUTGOING      
                }){
                count
                relationships
                    {
                        entity{
                            urn
                        }
                    }
                }
            }
        }
    """    
    resp = requests.post(
        query_endpoint, headers=headers, json={"query": query }
    )
    log.debug(f"group membership resp.status_code is {resp.status_code}")
    if resp.status_code != 200:
        return []
    data_received = json.loads(resp.text)
    if len(data_received)>0:
        groups_list = data_received["data"]["CorpUser"]["relationships"]["relationships"]
        return groups_list
    log.debug(f"group membership list is empty")
    return []

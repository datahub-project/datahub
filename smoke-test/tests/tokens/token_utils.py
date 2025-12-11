# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from tests.utils import get_frontend_url


def getUserId(session):
    response = session.get(
        f"{get_frontend_url()}/openapi/operations/identity/user/urn",
        params={"skipCache": "true"},
    )

    response.raise_for_status()
    return response.json()


def removeUser(session, urn):
    # Remove user
    json = {
        "query": """mutation removeUser($urn: String!) {
            removeUser(urn: $urn)
        }""",
        "variables": {"urn": urn},
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)

    response.raise_for_status()
    return response.json()


def listUsers(session):
    input = {
        "start": 0,
        "count": 20,
    }

    # list users
    json = {
        "query": """query listUsers($input: ListUsersInput!) {
            listUsers(input: $input) {
              start
              count
              total
              users {
                username
              }
            }
        }""",
        "variables": {"input": input},
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)

    response.raise_for_status()
    return response.json()

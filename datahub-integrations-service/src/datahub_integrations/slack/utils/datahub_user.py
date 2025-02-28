import logging
from typing import List, Optional, Tuple

import slack_bolt
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import CorpUserUrn

from datahub_integrations.app import graph
from datahub_integrations.graphql.subscription import GET_SUBSCRIPTION_QUERY

logger = logging.getLogger(__name__)


def graph_as_user(impersonation_urn: str) -> DataHubGraph:
    return DataHubGraph(
        graph.config.copy(
            update={"extra_headers": {"X-DataHub-Impersonated-Urn": impersonation_urn}},
            deep=True,
        )
    )


def graph_as_system() -> DataHubGraph:
    return graph


def get_user_information(
    app: slack_bolt.App, user_id: str
) -> Tuple[str, Optional[str], List[str]]:
    user_response = app.client.users_info(user=user_id)
    logger.debug(f"search users by email: {user_response}")
    email = user_response["user"]["profile"]["email"]

    users = list(
        graph.get_urns_by_filter(
            entity_types=[CorpUserUrn.ENTITY_TYPE],
            extraFilters=[
                {
                    "field": "email",
                    "value": email,
                    "condition": "EQUAL",
                }
            ],
        )
    )

    if not users:
        return email, None, []
    else:
        return email, users[0], users


def get_datahub_user(app: slack_bolt.App, user_id: str) -> Optional[str]:
    _, user_urn, _ = get_user_information(app, user_id)
    return user_urn


def get_subscription_urn(entity_urn: str, user_urn: str) -> Optional[str]:
    impersonation_graph = graph_as_user(user_urn)
    response = impersonation_graph.execute_graphql(
        GET_SUBSCRIPTION_QUERY, {"input": {"entityUrn": entity_urn}}
    )
    logger.debug(f"get_subscription: {response}")
    return (response["getSubscription"].get("subscription") or {}).get(
        "subscriptionUrn"
    )

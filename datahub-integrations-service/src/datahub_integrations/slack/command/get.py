from datahub_integrations.graphql.slack import SLACK_GET_ENTITY_QUERY
from datahub_integrations.observability import (
    BotCommand,
    BotPlatform,
    datahub_query_tracker,
    otel_instrument,
)
from datahub_integrations.slack.render.render_entity import render_entity_preview


@otel_instrument(
    metric_prefix="slack_command",
    description="Slack get command execution",
    labels={"platform": BotPlatform.SLACK, "command": BotCommand.GET},
)
def handle_get_command(graph, ack, respond, entity_urn):
    # Fetch the Entity From DataHub. Display a preview.
    variables = {"urn": entity_urn}
    with datahub_query_tracker("entity", BotPlatform.SLACK):
        data = graph.execute_graphql(SLACK_GET_ENTITY_QUERY, variables=variables)
    respond(render_entity_preview(raw_entity=data["entity"], include_link=True))

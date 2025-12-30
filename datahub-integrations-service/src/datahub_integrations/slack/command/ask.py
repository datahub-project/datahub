from datahub_integrations.observability import BotCommand, BotPlatform, otel_instrument


@otel_instrument(
    metric_prefix="slack_command",
    description="Slack ask command execution",
    labels={"platform": BotPlatform.SLACK, "command": BotCommand.ASK},
)
def handle_ask_command(graph, ack, respond, entity_urn):
    respond(
        {
            "text": "Ask command is not yet implemented. Please check back soon!",
            "response_type": "ephemeral",
        }
    )
    return

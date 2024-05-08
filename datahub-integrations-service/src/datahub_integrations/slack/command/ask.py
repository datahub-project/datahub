def handle_ask_command(graph, ack, respond, entity_urn):
    ack(
        {
            "text": "Ask command is not yet implemented. Please check back soon!",
            "response_type": "ephemeral",
        }
    )
    return

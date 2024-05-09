def handle_help_command(graph, ack, respond):
    ack(
        text="Find data assets inside your organization using Acryl!",
        response_type="ephemeral",
    )

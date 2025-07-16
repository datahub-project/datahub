from datahub_integrations.slack.slack import SlackMessageUrl, parse_slack_message_url


def test_slack_url_parsing() -> None:
    # non-threaded message
    assert parse_slack_message_url(
        "https://datahub-test.slack.com/archives/C048RD1240Y/p1683591366051289"
    ) == SlackMessageUrl(
        workspace_name="datahub-test",
        conversation_id="C048RD1240Y",
        message_id="1683591366051289",
        thread_ts=None,
    )

    # threaded message
    assert parse_slack_message_url(
        "https://datahub-test.slack.com/archives/C048RD1240Y/p1683591420041569?thread_ts=1683591366.051289&cid=C048RD1240Y"
    ) == SlackMessageUrl(
        workspace_name="datahub-test",
        conversation_id="C048RD1240Y",
        message_id="1683591420041569",
        thread_ts="1683591366.051289",
    )

    # direct message
    assert parse_slack_message_url(
        "https://acryldata.slack.com/archives/D03STM94GUB/p1659986371048239"
    ) == SlackMessageUrl(
        workspace_name="acryldata",
        conversation_id="D03STM94GUB",
        message_id="1659986371048239",
        thread_ts=None,
    )

    # invalid url
    assert (
        parse_slack_message_url(
            "https://api.slack.com/authentication/config-tokens#creating"
        )
        is None
    )

from datahub_integrations.slack.slack import parse_slack_message_url


def test_slack_url_parsing() -> None:
    # non-threaded message
    assert parse_slack_message_url(
        "https://datahub-test.slack.com/archives/C048RD1240Y/p1683591366051289"
    ) == ("datahub-test", "C048RD1240Y", "1683591366051289", None)

    # threaded message
    assert parse_slack_message_url(
        "https://datahub-test.slack.com/archives/C048RD1240Y/p1683591420041569?thread_ts=1683591366.051289&cid=C048RD1240Y"
    ) == (
        "datahub-test",
        "C048RD1240Y",
        "1683591420041569",
        "1683591366.051289",
    )

    # direct message
    assert parse_slack_message_url(
        "https://acryldata.slack.com/archives/D03STM94GUB/p1659986371048239"
    ) == ("acryldata", "D03STM94GUB", "1659986371048239", None)

    # invalid url
    assert (
        parse_slack_message_url(
            "https://api.slack.com/authentication/config-tokens#creating"
        )
        is None
    )

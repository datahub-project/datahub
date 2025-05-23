from loguru import logger

from datahub_integrations.slack.config import slack_config
from datahub_integrations.slack.slack import get_slack_app, get_slack_link_preview


def _send_test_message() -> None:
    config = slack_config.get_config()
    app = get_slack_app(config)

    app.client.chat_postMessage(
        channel="U04S624AANN", text="Acryl has been connected to Slack!"
    )


if __name__ == "__main__":
    # _send_test_message()

    # preview = get_slack_link_preview(
    #     "https://datahub-test.slack.com/archives/C048RD1240Y/p1683591420041569?thread_ts=1683591366.051289&cid=C048RD1240Y"
    # )
    # preview = get_slack_link_preview(
    #     "https://datahub-test.slack.com/archives/C048RD1240Y/p1683591366051289"
    # )
    preview = get_slack_link_preview(
        "https://datahub-test.slack.com/archives/C055AD0F98E/p1682727160293169"
    )
    logger.info(preview.model_dump_json(indent=2))

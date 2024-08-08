import os

from slack_sdk import WebClient

datahub_stats = {}


def add_datahub_stats(stat_name, stat_val):
    datahub_stats[stat_name] = stat_val


def send_to_slack(passed: str):
    slack_api_token = os.getenv("SLACK_API_TOKEN")
    slack_channel = os.getenv("SLACK_CHANNEL")
    slack_thread_ts = os.getenv("SLACK_THREAD_TS")
    test_identifier = os.getenv("TEST_IDENTIFIER", "LOCAL_TEST")
    if slack_api_token is None or slack_channel is None:
        return
    client = WebClient(token=slack_api_token)

    key: str
    message = ""
    for key, val in datahub_stats.items():
        if key.startswith("num-"):
            entity_type = key.replace("num-", "")
            message += f"Num {entity_type} is {val}\n"

    if slack_thread_ts is None:
        client.chat_postMessage(
            channel=slack_channel,
            text=f"{test_identifier} Status - {passed}\n{message}",
        )
    else:
        client.chat_postMessage(
            channel=slack_channel,
            text=f"{test_identifier} Status - {passed}\n{message}",
            thread_ts=slack_thread_ts,
        )


def send_message(exitstatus):
    try:
        send_to_slack("PASSED" if exitstatus == 0 else "FAILED")
    except Exception as e:
        # We don't want to fail pytest at all
        print(f"Exception happened for sending msg to slack {e}")

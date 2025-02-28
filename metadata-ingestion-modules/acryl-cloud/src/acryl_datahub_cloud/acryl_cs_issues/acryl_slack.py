import base64
import json
import logging
import re
import sqlite3
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional, Union

import requests
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from acryl_datahub_cloud.acryl_cs_issues.models import ExternalUser, TicketComment

logger = logging.getLogger(__name__)


class SlackMessageFinder:
    def __init__(
        self,
        token: str,
        cache_db_path: str = "slack_message_cache.db",
        cache_expiry_days: int = 7,
    ):
        self.client = WebClient(token=token)
        self.cache_db_path = cache_db_path
        self.cache_expiry_days = cache_expiry_days
        self._init_cache()

    def _init_cache(self) -> None:
        with sqlite3.connect(self.cache_db_path) as conn:
            # First drop the table if it exists
            conn.execute("DROP TABLE IF EXISTS messages")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    channel_id TEXT,
                    ts TEXT,
                    text TEXT,
                    cached_at TIMESTAMP,
                    PRIMARY KEY (channel_id, ts)
                )
            """
            )

    def _cache_messages(self, channel_id: str, messages: List[Dict[str, Any]]) -> None:
        with sqlite3.connect(self.cache_db_path) as conn:
            for message in messages:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO messages (channel_id, ts, text, cached_at)
                    VALUES (?, ?, ?, ?)
                """,
                    (
                        channel_id,
                        message["ts"],
                        message["text"],
                        datetime.now().isoformat(),
                    ),
                )

    def _get_cached_messages(self, channel_id: str) -> List[Dict[str, Any]]:
        with sqlite3.connect(self.cache_db_path) as conn:
            conn.row_factory = sqlite3.Row
            cutoff_date = (
                datetime.now() - timedelta(days=self.cache_expiry_days)
            ).isoformat()
            return conn.execute(
                """
                SELECT * FROM messages
                WHERE channel_id = ? AND cached_at > ?
                ORDER BY ts DESC
            """,
                (channel_id, cutoff_date),
            ).fetchall()

    def find_message(
        self, channel_id: str, message_text: str
    ) -> Optional[Dict[str, Any]]:
        # First, try to find the message in the cache
        cached_messages = self._get_cached_messages(channel_id)
        for message in cached_messages:
            if message_text[:30].lower() in message["text"].lower():
                return self._format_message(channel_id, dict(message))

        # If not found in cache, search in Slack and update cache
        try:
            cursor = None
            all_messages = []
            while True:
                result = self.client.conversations_history(
                    channel=channel_id, cursor=cursor, limit=100
                )
                all_messages.extend(result["messages"])

                for message in result["messages"]:
                    if message_text[:30].lower() in message["text"].lower():
                        # Cache all fetched messages before returning
                        self._cache_messages(channel_id, all_messages)
                        return self._format_message(channel_id, message)

                cursor = result.get("response_metadata", {}).get("next_cursor")
                if not cursor:
                    break

            # Cache all fetched messages even if target message not found
            self._cache_messages(channel_id, all_messages)
            return None

        except SlackApiError as e:
            print(f"Error: {e}")
            return None

    def _format_message(
        self, channel_id: str, message: Dict[str, Any]
    ) -> Dict[str, Any]:
        message_ts = message["ts"]
        message_ts = message_ts.replace(".", "")
        message_url = f"https://slack.com/archives/{channel_id}/p{message_ts}"
        try:
            return {
                "text": message["text"],
                "url": message_url,
                "ts": message_ts,
            }
        except KeyError:
            logger.error(f"Error formatting message: {message}")
            raise


class AcrylSlack:
    def __init__(self, slack_token: str):
        self.slack_token = slack_token
        # Initialize Slack client
        self.client = WebClient(token=slack_token)
        self.channel_name_id_map: Optional[Dict[str, str]] = None
        self.slack_message_finder = SlackMessageFinder(token=slack_token)

    def get_slack_token(self) -> str:
        return self.slack_token

    @lru_cache(maxsize=1000)  # noqa: B019
    def _get_user_info(self, user_id: str) -> Optional[Dict[str, Optional[str]]]:
        try:
            result = self.client.users_info(user=user_id)
            user = result["user"]
            return {
                "id": user["id"],
                "name": user["name"],
                "real_name": user["profile"]["real_name"],
                "display_name": user["profile"]["display_name"],
                "image": user["profile"]["image_72"],
                "email": user["profile"].get("email", None),
            }
        except SlackApiError as e:
            print(f"Error fetching user info for {user_id}: {e}")
            return None

    def resolve_usernames(self, data: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        # Parse the JSON string if it's a string
        if isinstance(data, str):
            message_data = json.loads(data)
        else:
            message_data = data

        user_ids = set()

        # Collect all user IDs from the message and thread
        user_ids.add(message_data.get("user"))
        user_ids.add(message_data["root"].get("user"))
        user_ids.update(message_data["root"].get("reply_users", []))

        # Resolve user IDs to user info
        users_info = {}
        for user_id in user_ids:
            if user_id:
                user_info = self._get_user_info(user_id)
                if user_info:
                    users_info[user_id] = user_info

        return users_info

    def get_image_data_url(self, private_url: str) -> str:
        """
        Fetch image data from Slack and convert it to a data URL.

        :param private_url: The private Slack image URL
        :param slack_token: Your Slack API token
        :return: A data URL containing the image data
        """
        headers = {"Authorization": f"Bearer {self.slack_token}"}
        response = requests.get(private_url, headers=headers)
        if response.status_code == 200:
            image_data = base64.b64encode(response.content).decode("utf-8")
            mime_type = response.headers.get("Content-Type", "image/png")
            # <img width="20" height="15" src="data:image/jpeg;base64, iVBORw0KGgoAAAANSUhEUgAAAAUAAAAFCAYAAACNbyblAAAAHElEQVQI12P4//8/w38GIAXDIBKE0DHxgljNBAAO9TXL0Y4OHwAAAABJRU5ErkJggg=="/>
            return f"data:{mime_type};base64, {image_data}"
        return private_url  # Return original URL if fetch fails

    def __slack_message_to_ticket_comment(self, data: dict) -> TicketComment:
        # Parse the JSON string into a Python dictionary

        # Extract the text content
        text = data.get("text", "")

        # Look for patterns like "<@U068J7VQPBR>" and replace with user names
        user_mentions = re.findall(r"<@(U[A-Z0-9]+)>", text)
        for user_id in user_mentions:
            user_info = self._get_user_info(user_id)
            if user_info:
                text = text.replace(f"<@{user_id}>", f"@{user_info['name']}")

        # Extract file information
        files = data.get("files", [])
        file_info = []
        for file in files:
            file_name = file.get("name", "Unknown File")
            file_url = file.get("url_private", "")
            if file_url:
                file_info.append(f"\n\n[{file_name}]({file_url})")
            else:
                logger.warning(f"File {file} has no URL")

        # Combine text and file info into the body
        body = f"{text}\n\n{''.join(file_info)}"

        # Extract timestamp and convert to datetime object
        timestamp = float(data.get("ts", 0))
        created_at = datetime.fromtimestamp(timestamp)

        # Extract user information
        user_profile = data.get("user_profile", {})
        user_id = data.get("user", "")
        if not user_profile or not user_profile.get("real_name"):
            user_info = self._get_user_info(user_id)
            if user_info:
                user = ExternalUser(
                    platform="slack",
                    id=user_id,
                    name=user_info.get("real_name") or f"Unknown User ({user_id})",
                    image=user_info.get("image", None),
                    email=user_info.get("email", None),
                )
        else:
            user = ExternalUser(
                platform="slack",
                id=user_id,
                name=user_profile.get("real_name", "Unknown User"),
                image=user_profile.get("image_72", None),
                email=user_profile.get("email", None),
            )

        # Create and return the TicketComment
        return TicketComment(
            created_at=created_at,
            public=True,  # Assuming all Slack messages are public
            body=body,
            platform="Slack",
            author=user,
        )

    def find_channel(self, channel_name: str) -> Optional[str]:
        if not self.channel_name_id_map:
            self.channel_name_id_map = {}
            try:
                cursor = None
                while True:
                    # Call the conversations.list method using the WebClient
                    result = self.client.conversations_list(cursor=cursor)
                    for channel in result["channels"]:
                        self.channel_name_id_map[channel["name"]] = channel["id"]

                    # Check if there are more pages
                    cursor = result.get("response_metadata", {}).get("next_cursor")
                    if not cursor:
                        break

            except SlackApiError as e:
                print(f"Error: {e}")
                return None

        return self.channel_name_id_map.get(channel_name)

    def find_message(
        self, channel_id: str, message_text: str
    ) -> Optional[Dict[str, Any]]:
        message = self.slack_message_finder.find_message(channel_id, message_text)
        return message

    def get_slack_conversations_from_text(  # noqa: C901
        self,
        text: str,
    ) -> Dict[str, Union[bool, str, List[TicketComment]]]:
        """
        Extracts Slack conversation URLs and list of messages from the given
        text per URL.

        If no conversation URLs are found, an empty dictionary is returned.

        Example Slack URLs:
        - https://acryldata.slack.com/archives/C06LH87S6SG/p1721920964955729?thread_ts=1721443902.665799&cid=C06LH87S6SG
        - https://acryldata.slack.com/archives/C06LH87S6SG/p1721922184323209

        Args:
        text (str): The text to search for Slack URLs.

        Returns:
        Dict[str, Union[bool, str, List[TicketComment]]]: A dictionary
        containing
        - is_thread (bool): Whether the conversation is a thread
        - url (str): The URL of the conversation
        - messages (List[TicketComment]): A list of messages in the conversation

        TODO-s:
        - Add support for detecting User mentions even when there is no Slack
        URL in the text and use that to hunt for messages in Slack to link to
        the ticket (This is important because there are many cases where the
        agent will create a ticket from Slack and not provide the URL of the
        Slack conversation)
        - Make the return type a dataclass

        """

        pattern = re.compile(
            r".*(https://.*\.slack\.com/archives/([A-Z0-9]+)/p(\d+)).*$", re.DOTALL
        )

        match = re.match(pattern, text)
        if "slack.com" in text and not match:
            logger.warning(f"Seems like we missed a Slack URL: {text}")

        if not match:
            # We didn't find a Slack URL
            # Look for slack message creation sentinel
            # e.g. Ticket created from Slack by Ellie O'Neil in acryl-xero
            ticket_created_from_slack = re.search(
                r"Ticket created from Slack by (.+) in (.+)", text
            )
            if ticket_created_from_slack:
                channel_id = self.find_channel(ticket_created_from_slack.group(2))
                if channel_id:
                    fuzzy_message = self.find_message(channel_id, text)
                    if fuzzy_message:
                        channel_id, message_ts = (
                            channel_id,
                            fuzzy_message["ts"],
                        )
                        message_url = f"https://acryldata.slack.com/archives/{channel_id}/p{message_ts}"
                    else:
                        logger.warning(
                            f"Message {text} not found in channel {channel_id}."
                        )
                        return {}
                else:
                    logger.warning(
                        f"Channel {ticket_created_from_slack.group(2)} not found."
                    )
                    return {}
            else:
                return {}
        else:
            message_url, channel_id, message_ts = match.groups()

        if "." not in message_ts:
            message_ts = f"{message_ts[:10]}.{message_ts[10:]}"

        # Validate channel existence
        try:
            channel_info = self.client.conversations_info(channel=channel_id)
            if not channel_info["ok"]:
                raise ValueError(
                    f"Channel validation failed: {channel_info.get('error', 'Unknown error')}"
                )
        except SlackApiError as e:
            if e.response["error"] == "channel_not_found":
                logger.warning(f"Channel {channel_id} not found or inaccessible.")
                return {}
            else:
                logger.error(f"Error validating channel: {e}")
                return {}

        try:
            # Fetch the message
            result = self.client.conversations_history(
                channel=channel_id, latest=message_ts, inclusive=True, limit=1
            )

            if not result["messages"]:
                raise ValueError("Message not found")

            message = result["messages"][0]

            response = {}

            # Check if the message is part of a thread
            if "thread_ts" in message and message["thread_ts"] == message["ts"]:
                # This is the parent message of a thread
                thread_result = self.client.conversations_replies(
                    channel=channel_id, ts=message["ts"]
                )
                response = {"is_thread": True, "messages": thread_result["messages"]}
            elif "thread_ts" in message:
                # This is a reply in a thread
                thread_result = self.client.conversations_replies(
                    channel=channel_id, ts=message["thread_ts"]
                )
                response = {"is_thread": True, "messages": thread_result["messages"]}
            else:
                # This is a standalone message
                response = {"is_thread": False, "messages": [message]}

            if response:
                response["messages"] = [
                    self.__slack_message_to_ticket_comment(m)
                    for m in response["messages"]
                ]
                response["url"] = message_url
                return response

        except SlackApiError as e:
            print(f"Error: {e}")
            return {"is_thread": False, "messages": []}

        return {}

from datetime import datetime
from typing import Optional

import requests

from acryl_datahub_cloud.acryl_cs_issues.models import (
    ExternalUser,
    Issue,
    Project,
    TicketComment,
)


class LinearAPIClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.endpoint = "https://api.linear.app/graphql"
        self.headers = {"Content-Type": "application/json", "Authorization": api_key}

    def execute_query(self, query: str, variables: Optional[dict] = None) -> dict:
        payload = {"query": query, "variables": variables}
        response = requests.post(self.endpoint, json=payload, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def get_project(self, project_id: str) -> Project:
        query = """
        query GetProject($id: String!) {
          project(id: $id) {
            id
            name
            description
            url
          }
        }
        """
        variables = {"id": project_id}
        result = self.execute_query(query, variables)
        project_data = result["data"]["project"]

        return Project(
            name=project_data["name"],
            type="linear",  # Assuming all projects from Linear are of type "linear"
            project_id=Project.ProjectId(platform="linear", id=project_data["id"]),
            description=project_data["description"],
            external_url=project_data["url"],
        )

    def get_issue(self, issue_id: str) -> Issue:
        query = """
        query GetIssueDetails($id: String!) {
          issue(id: $id) {
            identifier
            title
            description
            createdAt
            updatedAt
            url
            state {
              name
            }
            creator {
              id
              name
              email
            }
            assignee {
              id
              name
              email
            }
            project {
              id
            }
            labels {
              nodes {
                name
              }
            }
            comments {
              nodes {
                id
                body
                createdAt
                user {
                  id
                  name
                  email
                }
              }
            }
          }
        }
        """
        variables = {"id": issue_id}
        result = self.execute_query(query, variables)
        issue_data = result["data"]["issue"]

        project = (
            self.get_project(issue_data["project"]["id"])
            if issue_data["project"]
            else None
        )
        conversations = [
            TicketComment(
                id=comment["id"],
                body=comment["body"],
                created_at=datetime.fromisoformat(
                    comment["createdAt"].replace("Z", "")
                ),
                public=False,
                platform="linear",
                author=ExternalUser(
                    platform="linear",
                    id=comment["user"]["id"],
                    name=comment["user"]["name"],
                    email=comment["user"]["email"],
                ),
            )
            for comment in issue_data["comments"]["nodes"]
        ]
        return Issue(
            project=project,
            issue_id=Issue.IssueId(platform="linear", id=issue_data["identifier"]),
            customer_id="",  # You may need to add this field to the query if it's available
            status=issue_data["state"]["name"],
            subject=issue_data["title"],
            created_at=datetime.fromisoformat(issue_data["createdAt"].replace("Z", "")),
            updated_at=datetime.fromisoformat(issue_data["updatedAt"].replace("Z", "")),
            description=issue_data["description"],
            creator=(
                ExternalUser(
                    platform="linear",
                    id=issue_data["creator"]["id"],
                    name=issue_data["creator"]["name"],
                    email=issue_data["creator"]["email"],
                )
                if issue_data["creator"]
                else None
            ),
            assignee=(
                ExternalUser(
                    platform="linear",
                    id=issue_data["assignee"]["id"],
                    name=issue_data["assignee"]["name"],
                    email=issue_data["assignee"]["email"],
                )
                if issue_data["assignee"]
                else None
            ),
            conversation_list=conversations,
            external_url=issue_data["url"],
            # feature_area=(
            #     DataHubProductGlossary.FeatureArea(
            #         name=issue_data["labels"]["nodes"][0]["name"]
            #     )
            #     if issue_data["labels"]["nodes"]
            #     else None
            # ),
            category=None,  # You may need to add this field to the query if it's available
            platforms=[],  # You may need to add this field to the query if it's available
            linked_resources=[],  # You may need to add this field to the query if it's available
            last_commented_at=(
                max([conv.created_at for conv in conversations])
                if conversations
                else None
            ),
        )


# Example usage:
if __name__ == "__main__":
    import os
    import sys

    # get ticket id from command line argument
    ticket_id = sys.argv[1]

    linear_api_key = os.getenv("LINEAR_API_KEY")
    assert linear_api_key, "Please set the LINEAR_API_KEY environment variable"
    client = LinearAPIClient(linear_api_key)
    issue_data = client.get_issue(ticket_id)
    print(issue_data)

    for conversation in issue_data.conversation_list:
        print(f"Conversation ID: {conversation.id}")
        print(f"Body: {conversation.body}")
        print(f"Created At: {conversation.created_at}")
        print(f"User: {conversation.author}")
        print("-" * 50)

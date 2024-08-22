import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from zenpy import Zenpy
from zenpy.lib.api import Organization
from zenpy.lib.api_objects import Comment, Ticket


class ZendeskClient:
    def __init__(
        self,
        email: Optional[str] = None,
        token: Optional[str] = None,
        subdomain: Optional[str] = None,
    ) -> None:
        if email and token and subdomain:
            self.credentials = {
                "email": email,
                "token": token,
                "subdomain": subdomain,
            }
        else:
            self.credentials = self.load_credentials() or {
                "email": os.environ.get("ZENDESK_EMAIL"),
                "token": os.environ.get("ZENDESK_TOKEN"),
                "subdomain": os.environ.get("ZENDESK_SUBDOMAIN"),
            }
        self.zenpy_client = Zenpy(**self.credentials)
        self.organizations_cache: Dict[int, Organization] = {}

    @staticmethod
    def load_credentials() -> Dict:
        with open(".api_credentials.json") as f:
            credentials = json.load(f)
        return credentials

    def create_ticket(
        self,
        subject: str,
        description: str,
        priority: str = "normal",
        slack_thread_id: Optional[str] = None,
    ) -> Ticket:
        """
        Create a new Zendesk ticket.
        """
        new_ticket = Ticket(
            subject=subject,
            description=description,
            priority=priority,
            custom_fields=[{"slack_thread_id": slack_thread_id}],
        )
        result = self.zenpy_client.tickets.create(new_ticket)
        return result.ticket

    def update_ticket(
        self,
        ticket_id: int,
        comment: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Any:
        """
        Update an existing Zendesk ticket.
        """
        ticket = self.zenpy_client.tickets(id=ticket_id)
        for ticket in self.zenpy_client.tickets(id=ticket_id):
            print(ticket)
        if comment:
            ticket.comment = Comment(body=comment)
        if status:
            ticket.status = status
        result = self.zenpy_client.tickets.update(ticket)
        return result

    def get_tickets_last_week(
        self, organization_id: Optional[int] = None
    ) -> List[Ticket]:
        """
        Retrieve all tickets created in the last week.
        """
        # Calculate the date one week ago
        return self.get_tickets_last_period(
            organization_id=organization_id, lookback_days=7
        )

    def get_tickets_last_period(
        self, organization_id: Optional[int] = None, lookback_days: int = 7
    ) -> List[Ticket]:
        """
        Retrieve all tickets created in the last time interval.
        """
        # Calculate the date one week ago
        date_previous = datetime.now() - timedelta(days=lookback_days)

        # Query for tickets created after time interval
        # TODO: Add pagination
        # TODO: Support updated_after
        tickets = self.zenpy_client.search(
            type="ticket", created_after=date_previous, organization_id=organization_id
        )

        return tickets

    def get_ticket_with_organization(
        self, ticket_id: int
    ) -> Tuple[Ticket, Optional[Organization], Optional[List["Comment"]]]:
        """
        Retrieve a ticket and its associated organization details.
        """
        ticket = self.zenpy_client.tickets(id=ticket_id)
        if ticket.organization_id:
            if ticket.organization_id in self.organizations_cache:
                organization = self.organizations_cache[ticket.organization_id]
            else:
                organization = self.zenpy_client.organizations(
                    id=ticket.organization_id
                )
                self.organizations_cache[ticket.organization_id] = organization

            comments = self.zenpy_client.tickets.comments(ticket=ticket_id)
            return ticket, organization, comments
        return ticket, None, None

    def get_organization_id(self, organization_name: str) -> Optional[int]:
        """
        Retrieve the ID of an organization by name.
        """
        organizations = [
            o
            for o in self.zenpy_client.search(
                type="organization", name=organization_name
            )
        ]
        if organizations:
            return organizations[0].id
        return None

    def get_organizations(self) -> List[Organization]:
        """
        Retrieve all organizations.
        """
        organizations = self.zenpy_client.organizations()
        return organizations

    def get_user(self, user_id: int) -> Any:
        """
        Retrieve a user by ID.
        """
        user = self.zenpy_client.users(id=user_id)
        return user

    def print_ticket_details(
        self, ticket: Ticket, organization: Optional[Organization] = None
    ) -> None:
        """
        Print details of a ticket, including organization name if available.
        """
        print(f"Ticket ID: {ticket.id}")
        print(f"Subject: {ticket.subject}")
        print(f"Status: {ticket.status}")
        print(f"Created At: {ticket.created_at}")
        if organization:
            print(f"Organization: {organization.name}")
        else:
            print("Organization: Not associated with an organization")
        print("-" * 50)


# Example usage
if __name__ == "__main__":
    # Create a new ticket
    # new_ticket = create_ticket("Test Ticket", "This is a test ticket created via API", "low")
    # print(f"Created ticket with ID: {new_ticket.id}")
    zendesk_client = ZendeskClient()
    for ticket in zendesk_client.get_tickets_last_week():
        ticket, org, comments = zendesk_client.get_ticket_with_organization(ticket.id)
        zendesk_client.print_ticket_details(ticket, org)

#!/usr/bin/env python3
"""
Generate synthetic users for analytics testing.

This script creates a set of realistic user profiles that can be used
to generate activity data for populating DataHub analytics dashboards.
"""

import argparse
import json
import logging
import random
from datetime import datetime, timezone
from typing import Dict, List

from datahub.emitter.mce_builder import make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    CorpUserEditableInfoClass,
    CorpUserInfoClass,
    CorpUserStatusClass,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Sample data for generating realistic user profiles
FIRST_NAMES = [
    "Alice",
    "Bob",
    "Carol",
    "David",
    "Emma",
    "Frank",
    "Grace",
    "Henry",
    "Isabel",
    "Jack",
    "Kate",
    "Liam",
    "Maya",
    "Noah",
    "Olivia",
    "Peter",
    "Quinn",
    "Rachel",
    "Sam",
    "Tara",
    "Uma",
    "Victor",
    "Wendy",
    "Xander",
    "Yara",
    "Zoe",
    "Alex",
    "Blake",
    "Casey",
    "Drew",
]

LAST_NAMES = [
    "Anderson",
    "Brown",
    "Chen",
    "Davis",
    "Evans",
    "Fischer",
    "Garcia",
    "Harris",
    "Ivanov",
    "Johnson",
    "Kim",
    "Lee",
    "Miller",
    "Nelson",
    "O'Brien",
    "Patel",
    "Quinn",
    "Rodriguez",
    "Smith",
    "Taylor",
    "Upton",
    "Vargas",
    "Wilson",
    "Xavier",
    "Yang",
    "Zhang",
]

DEPARTMENTS = [
    "Engineering",
    "Data Science",
    "Product",
    "Marketing",
    "Sales",
    "Finance",
    "Operations",
    "Legal",
    "HR",
    "Customer Success",
]

TITLES = [
    "Data Engineer",
    "Data Scientist",
    "Product Manager",
    "Software Engineer",
    "Analytics Engineer",
    "Data Analyst",
    "ML Engineer",
    "BI Developer",
    "Platform Engineer",
    "Data Architect",
]

TEAMS = [
    "Platform",
    "Growth",
    "Core Product",
    "Data Infrastructure",
    "Analytics",
    "ML Platform",
    "Business Intelligence",
    "Data Governance",
    "Data Quality",
]


def generate_user_profile(index: int, base_email_domain: str = "example.com") -> Dict:
    """Generate a single user profile."""
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    username = f"{first_name.lower()}.{last_name.lower()}{index if index > 0 else ''}"
    email = f"{username}@{base_email_domain}"

    return {
        "username": username,
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "display_name": f"{first_name} {last_name}",
        "title": random.choice(TITLES),
        "department": random.choice(DEPARTMENTS),
        "team": random.choice(TEAMS),
        "slack": f"@{username}",
    }


def create_user_aspects(user_profile: Dict) -> List[MetadataChangeProposalWrapper]:
    """Create DataHub user aspects from profile data."""
    user_urn = make_user_urn(user_profile["username"])

    mcps = []

    # CorpUserInfo aspect
    user_info = CorpUserInfoClass(
        active=True,
        displayName=user_profile["display_name"],
        email=user_profile["email"],
        title=user_profile["title"],
        fullName=f"{user_profile['first_name']} {user_profile['last_name']}",
        firstName=user_profile["first_name"],
        lastName=user_profile["last_name"],
        departmentName=user_profile["department"],
    )

    mcps.append(
        MetadataChangeProposalWrapper(
            entityUrn=user_urn,
            aspect=user_info,
        )
    )

    # CorpUserEditableInfo aspect
    editable_info = CorpUserEditableInfoClass(
        aboutMe=f"{user_profile['title']} in {user_profile['department']}",
        teams=[user_profile["team"]],
        skills=["Data", "Analytics"],
        pictureLink="https://datahubproject.io/img/datahub-logo.png",
    )

    mcps.append(
        MetadataChangeProposalWrapper(
            entityUrn=user_urn,
            aspect=editable_info,
        )
    )

    # CorpUserStatus aspect - mark as active
    status = CorpUserStatusClass(
        status="ACTIVE",
        lastModified=AuditStampClass(
            time=int(datetime.now(timezone.utc).timestamp() * 1000),
            actor="urn:li:corpuser:datahub",
        ),
    )

    mcps.append(
        MetadataChangeProposalWrapper(
            entityUrn=user_urn,
            aspect=status,
        )
    )

    return mcps


def generate_and_emit_users(
    num_users: int,
    gms_url: str,
    token: str,
    email_domain: str = "example.com",
    output_file: str = None,
) -> List[Dict]:
    """Generate users and emit to DataHub."""
    logger.info(f"Generating {num_users} user profiles...")

    users = []
    for i in range(num_users):
        user_profile = generate_user_profile(i, email_domain)
        users.append(user_profile)

    logger.info(f"Generated {len(users)} users")

    # Save to file if requested
    if output_file:
        with open(output_file, "w") as f:
            json.dump(users, f, indent=2)
        logger.info(f"Saved user profiles to {output_file}")

    # Emit to DataHub
    if gms_url and token:
        logger.info(f"Emitting users to DataHub at {gms_url}...")
        emitter = DataHubRestEmitter(gms_server=gms_url, token=token)

        for user_profile in users:
            mcps = create_user_aspects(user_profile)
            for mcp in mcps:
                emitter.emit_mcp(mcp)

        logger.info(f"Successfully emitted {len(users)} users to DataHub")

    return users


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic users for DataHub analytics testing"
    )
    parser.add_argument(
        "--num-users",
        type=int,
        default=20,
        help="Number of users to generate (default: 20)",
    )
    parser.add_argument(
        "--gms-url",
        default="http://localhost:8080",
        help="DataHub GMS URL (default: http://localhost:8080)",
    )
    parser.add_argument(
        "--token",
        required=True,
        help="DataHub authentication token",
    )
    parser.add_argument(
        "--email-domain",
        default="example.com",
        help="Email domain for generated users (default: example.com)",
    )
    parser.add_argument(
        "--output-file",
        help="Optional: Save generated user profiles to JSON file",
    )

    args = parser.parse_args()

    users = generate_and_emit_users(
        num_users=args.num_users,
        gms_url=args.gms_url,
        token=args.token,
        email_domain=args.email_domain,
        output_file=args.output_file,
    )

    logger.info(f"✅ Created {len(users)} users")
    logger.info("Sample usernames:")
    for user in users[:5]:
        logger.info(f"  - {user['username']} ({user['display_name']})")
    if len(users) > 5:
        logger.info(f"  ... and {len(users) - 5} more")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Backfill synthetic activity events for DataHub analytics.

This script generates realistic user activity events with backdated timestamps
to populate analytics dashboards with historical data.

Supports events for:
- Page views (home, search, entity profiles)
- Search queries
- Entity views (datasets, dashboards, charts, etc.)
- Login events
"""

import argparse
import json
import logging
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import requests

from datahub.emitter.mce_builder import (
    make_chart_urn,
    make_dashboard_urn,
    make_dataset_urn,
)
from datahub.utilities.urns.urn import guess_entity_type

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Event type constants matching DataHub backend
EVENT_TYPE_ENTITY_VIEW = "EntityViewEvent"
EVENT_TYPE_ENTITY_ACTION = "EntityActionEvent"
EVENT_TYPE_ENTITY_SECTION_VIEW = "EntitySectionViewEvent"
EVENT_TYPE_SEARCH = "SearchEvent"
EVENT_TYPE_SEARCH_RESULTS_VIEW = "SearchResultsViewEvent"
EVENT_TYPE_HOME_PAGE_VIEW = "HomePageViewEvent"
EVENT_TYPE_LOGIN = "LogInEvent"

# Entity types for analytics - comprehensive list of DataHub entities
ENTITY_TYPES = [
    "dataset",
    "dashboard",
    "chart",
    "dataFlow",
    "dataJob",
    "mlModel",
    "mlFeature",
    "mlFeatureTable",
    "glossaryTerm",
    "glossaryNode",
    "tag",
    "domain",
    "container",
    "notebook",
    "dataProduct",
    "dataProcess",
    "dataPlatformInstance",
    "assertion",
    "testResult",
    "dataHubView",
    "dataHubPolicy",
    "schemaField",
    "corpUser",
    "corpGroup",
]

# Popular search queries
SEARCH_QUERIES = [
    "customer",
    "revenue",
    "user",
    "transaction",
    "product",
    "sales",
    "analytics",
    "pipeline",
    "warehouse",
    "reporting",
    "dashboard",
    "metrics",
    "daily",
    "monthly",
    "summary",
]

# Tab types for tab view events (EntitySectionViewEvent)
# Based on actual tabs used across different entity types in DataHub
TAB_TYPES = [
    "Schema",
    "Columns",
    "Documentation",
    "Lineage",
    "Properties",
    "Queries",
    "Stats",
    "Validation",
    "Quality",
    "Governance",
    "Incidents",
    "Runs",
    "Operations",
    "Access",
    "Preview",
    "View Definition",
    "Relationships",
    "Assets",
    "Members",
    "Groups",
    "Features",
    "Models",
    "Tests",
]

# Action types for entity action events (EntityActionEvent)
ACTION_TYPES = [
    "ClickExternalUrl",
    "DownloadAsCsv",
    "UpdateDescription",
    "UpdateTags",
    "UpdateOwnership",
    "UpdateLinks",
    "UpdateDomain",
    "UpdateDeprecation",
]


class ActivityEventGenerator:
    """Generate realistic activity events with temporal patterns."""

    def __init__(
        self,
        gms_url: str,
        token: str,
        users: List[Dict],
        entity_urns: List[str],
    ):
        self.gms_url = gms_url
        self.token = token
        self.users = users
        self.entity_urns = entity_urns

        # Assign "power users" (20% of users generate 80% of activity)
        num_power_users = max(1, len(users) // 5)
        self.power_users = random.sample(users, num_power_users)

        # Generate a consistent browserId for each user (simulating each user has one main browser)
        self.user_browser_ids = {user["username"]: str(uuid.uuid4()) for user in users}

    def _get_random_user(self, prefer_power_user: bool = True) -> Dict:
        """Get a random user, preferring power users 80% of the time."""
        if prefer_power_user and random.random() < 0.8:
            return random.choice(self.power_users)
        return random.choice(self.users)

    def _get_random_entity_urn(self) -> str:
        """Get a random entity URN, mixing real URNs with synthetic ones for diversity."""
        # 40% chance to use a synthetic URN for more diverse entity types
        if random.random() < 0.4 or not self.entity_urns:
            return self._generate_sample_urn()
        return random.choice(self.entity_urns)

    def _generate_sample_urn(self) -> str:
        """Generate a sample URN if no entities provided."""
        entity_type = random.choice(ENTITY_TYPES)
        if entity_type == "dataset":
            return make_dataset_urn(
                "bigquery", f"project.dataset.table_{random.randint(1, 100)}"
            )
        elif entity_type == "dashboard":
            return make_dashboard_urn("looker", f"dashboard_{random.randint(1, 50)}")
        elif entity_type == "chart":
            return make_chart_urn("looker", f"chart_{random.randint(1, 100)}")
        return f"urn:li:{entity_type}:(platform,{entity_type}_{random.randint(1, 100)},PROD)"

    def generate_entity_view_event(
        self,
        timestamp: datetime,
        user: Optional[Dict] = None,
        entity_urn: Optional[str] = None,
    ) -> Dict:
        """Generate an entity view event."""
        user = user or self._get_random_user()
        entity_urn = entity_urn or self._get_random_entity_urn()
        entity_type = guess_entity_type(entity_urn)

        return {
            "type": EVENT_TYPE_ENTITY_VIEW,
            "timestamp": int(timestamp.timestamp() * 1000),
            "actorUrn": f"urn:li:corpuser:{user['username']}",
            "browserId": self.user_browser_ids[user["username"]],
            "entityUrn": entity_urn,
            "entityType": entity_type.upper(),
            "usageSource": "web",
        }

    def generate_entity_action_event(
        self,
        timestamp: datetime,
        action_type: str,
        user: Optional[Dict] = None,
        entity_urn: Optional[str] = None,
    ) -> Dict:
        """Generate an entity action event."""
        user = user or self._get_random_user()
        entity_urn = entity_urn or self._get_random_entity_urn()
        entity_type = guess_entity_type(entity_urn)

        return {
            "type": EVENT_TYPE_ENTITY_ACTION,
            "timestamp": int(timestamp.timestamp() * 1000),
            "actorUrn": f"urn:li:corpuser:{user['username']}",
            "browserId": self.user_browser_ids[user["username"]],
            "entityUrn": entity_urn,
            "entityType": entity_type.upper(),
            "actionType": action_type,
            "usageSource": "web",
        }

    def generate_entity_section_view_event(
        self,
        timestamp: datetime,
        section: str,
        user: Optional[Dict] = None,
        entity_urn: Optional[str] = None,
    ) -> Dict:
        """Generate an entity section view event (for tab views)."""
        user = user or self._get_random_user()
        entity_urn = entity_urn or self._get_random_entity_urn()
        entity_type = guess_entity_type(entity_urn)

        return {
            "type": EVENT_TYPE_ENTITY_SECTION_VIEW,
            "timestamp": int(timestamp.timestamp() * 1000),
            "actorUrn": f"urn:li:corpuser:{user['username']}",
            "browserId": self.user_browser_ids[user["username"]],
            "entityUrn": entity_urn,
            "entityType": entity_type.upper(),
            "section": section,
            "usageSource": "web",
        }

    def generate_search_event(
        self,
        timestamp: datetime,
        query: Optional[str] = None,
        user: Optional[Dict] = None,
    ) -> Dict:
        """Generate a search event."""
        user = user or self._get_random_user()
        query = query or random.choice(SEARCH_QUERIES)

        return {
            "type": EVENT_TYPE_SEARCH,
            "timestamp": int(timestamp.timestamp() * 1000),
            "actorUrn": f"urn:li:corpuser:{user['username']}",
            "browserId": self.user_browser_ids[user["username"]],
            "query": query,
            "usageSource": "web",
        }

    def generate_search_results_view_event(
        self,
        timestamp: datetime,
        query: Optional[str] = None,
        user: Optional[Dict] = None,
    ) -> Dict:
        """Generate a search results view event."""
        user = user or self._get_random_user()
        query = query or random.choice(SEARCH_QUERIES)

        return {
            "type": EVENT_TYPE_SEARCH_RESULTS_VIEW,
            "timestamp": int(timestamp.timestamp() * 1000),
            "actorUrn": f"urn:li:corpuser:{user['username']}",
            "browserId": self.user_browser_ids[user["username"]],
            "query": query,
            "usageSource": "web",
        }

    def generate_home_page_view_event(
        self,
        timestamp: datetime,
        user: Optional[Dict] = None,
    ) -> Dict:
        """Generate a home page view event."""
        user = user or self._get_random_user()

        return {
            "type": EVENT_TYPE_HOME_PAGE_VIEW,
            "timestamp": int(timestamp.timestamp() * 1000),
            "actorUrn": f"urn:li:corpuser:{user['username']}",
            "browserId": self.user_browser_ids[user["username"]],
            "usageSource": "web",
        }

    def generate_login_event(
        self,
        timestamp: datetime,
        user: Optional[Dict] = None,
    ) -> Dict:
        """Generate a login event."""
        user = user or self._get_random_user()

        return {
            "type": EVENT_TYPE_LOGIN,
            "timestamp": int(timestamp.timestamp() * 1000),
            "actorUrn": f"urn:li:corpuser:{user['username']}",
            "browserId": self.user_browser_ids[user["username"]],
            "loginSource": "web",
        }

    def generate_guaranteed_coverage_events(
        self,
        timestamp: datetime,
        required_entity_types: Optional[List[str]] = None,
    ) -> List[Dict]:
        """
        Generate events that guarantee coverage of required entity types.

        This ensures tests that check for specific entity types will always pass.
        Creates both EntityViewEvent and EntitySectionViewEvent for each type.

        Args:
            timestamp: Timestamp for the events
            required_entity_types: List of entity types to ensure coverage for.
                                   Defaults to ["dataset", "dashboard", "chart"]

        Returns:
            List of events guaranteeing coverage of required entity types
        """
        if required_entity_types is None:
            required_entity_types = ["dataset", "dashboard", "chart"]

        events = []
        user = self._get_random_user()

        for entity_type in required_entity_types:
            # Generate multiple events per entity type for realistic distribution
            for i in range(5):  # 5 events per entity type minimum
                if entity_type == "dataset":
                    entity_urn = make_dataset_urn(
                        "bigquery", f"project.dataset.guaranteed_table_{i}"
                    )
                elif entity_type == "dashboard":
                    entity_urn = make_dashboard_urn(
                        "looker", f"guaranteed_dashboard_{i}"
                    )
                elif entity_type == "chart":
                    entity_urn = make_chart_urn("looker", f"guaranteed_chart_{i}")
                else:
                    entity_urn = f"urn:li:{entity_type}:(platform,guaranteed_{entity_type}_{i},PROD)"

                # Generate view event
                events.append(
                    self.generate_entity_view_event(timestamp, user, entity_urn)
                )

                # Generate tab view event (what the failing test checks)
                tab = random.choice(TAB_TYPES)
                events.append(
                    self.generate_entity_section_view_event(
                        timestamp, tab, user, entity_urn
                    )
                )

        logger.info(
            f"Generated {len(events)} guaranteed coverage events for types: {required_entity_types}"
        )
        return events

    def simulate_user_session(self, session_start: datetime, user: Dict) -> List[Dict]:
        """Simulate a realistic user session with multiple events."""
        events = []
        current_time = session_start

        # Start with login
        events.append(self.generate_login_event(current_time, user))
        current_time += timedelta(seconds=random.randint(1, 5))

        # Home page view
        events.append(self.generate_home_page_view_event(current_time, user))
        current_time += timedelta(seconds=random.randint(5, 20))

        # Simulate 3-10 activities in this session
        num_activities = random.randint(3, 10)
        for _ in range(num_activities):
            activity_type = random.choice(
                ["search", "entity_view", "tab_view", "entity_action"]
            )

            if activity_type == "search":
                query = random.choice(SEARCH_QUERIES)
                events.append(self.generate_search_event(current_time, query, user))
                current_time += timedelta(seconds=random.randint(1, 3))
                events.append(
                    self.generate_search_results_view_event(current_time, query, user)
                )
                current_time += timedelta(seconds=random.randint(5, 30))

            elif activity_type == "entity_view":
                entity_urn = self._get_random_entity_urn()
                events.append(
                    self.generate_entity_view_event(current_time, user, entity_urn)
                )
                current_time += timedelta(seconds=random.randint(10, 60))

                # Sometimes view tabs on the entity
                if random.random() < 0.7:
                    tab = random.choice(TAB_TYPES)
                    events.append(
                        self.generate_entity_section_view_event(
                            current_time, tab, user, entity_urn
                        )
                    )
                    current_time += timedelta(seconds=random.randint(5, 30))

                # Sometimes perform actions on the entity
                if random.random() < 0.3:
                    action = random.choice(ACTION_TYPES)
                    events.append(
                        self.generate_entity_action_event(
                            current_time, action, user, entity_urn
                        )
                    )
                    current_time += timedelta(seconds=random.randint(2, 10))

            elif activity_type == "tab_view":
                entity_urn = self._get_random_entity_urn()
                tab = random.choice(TAB_TYPES)
                events.append(
                    self.generate_entity_section_view_event(
                        current_time, tab, user, entity_urn
                    )
                )
                current_time += timedelta(seconds=random.randint(5, 30))

            else:  # entity_action
                entity_urn = self._get_random_entity_urn()
                action = random.choice(ACTION_TYPES)
                events.append(
                    self.generate_entity_action_event(
                        current_time, action, user, entity_urn
                    )
                )
                current_time += timedelta(seconds=random.randint(2, 10))

        return events

    def generate_events_for_date(
        self, date: datetime, target_events: int, ensure_coverage: bool = False
    ) -> List[Dict]:
        """
        Generate events for a specific date with realistic temporal patterns.

        Args:
            date: The date to generate events for
            target_events: Target number of events to generate
            ensure_coverage: If True, guarantees coverage of common entity types
                             (dataset, dashboard, chart) for deterministic tests

        Returns:
            List of generated events for the date
        """
        events = []

        # Add guaranteed coverage events if requested (for deterministic tests)
        if ensure_coverage:
            coverage_events = self.generate_guaranteed_coverage_events(
                date.replace(hour=10, minute=0, second=0, microsecond=0)
            )
            events.extend(coverage_events)
            logger.info(
                f"Added {len(coverage_events)} guaranteed coverage events for {date.date()}"
            )

        # Working hours are 9 AM to 6 PM
        work_start = date.replace(hour=9, minute=0, second=0, microsecond=0)
        work_end = date.replace(hour=18, minute=0, second=0, microsecond=0)

        # Generate sessions throughout the day
        num_sessions = random.randint(target_events // 10, target_events // 5)

        for _ in range(num_sessions):
            # Random time during work hours
            session_start = work_start + timedelta(
                seconds=random.randint(0, int((work_end - work_start).total_seconds()))
            )

            # Select user (power users more likely)
            user = self._get_random_user(prefer_power_user=True)

            # Generate session events
            session_events = self.simulate_user_session(session_start, user)
            events.extend(session_events)

        logger.info(f"Generated {len(events)} events for {date.date()}")
        return events


def send_events_to_elasticsearch(
    events: List[Dict],
    elasticsearch_url: str,
    index_name: str = "datahub_usage_event",
) -> None:
    """
    Send events directly to Elasticsearch.

    Uses Elasticsearch bulk API for efficient ingestion.
    Each event is sent as an index operation.
    """
    logger.info(
        f"Sending {len(events)} events to Elasticsearch at {elasticsearch_url}..."
    )

    # Prepare bulk request body
    # Bulk API format: action_and_metadata\n + optional_source\n
    bulk_body_lines = []
    for event in events:
        # Index action
        action = {"index": {"_index": index_name}}
        bulk_body_lines.append(json.dumps(action))
        bulk_body_lines.append(json.dumps(event))

    bulk_body = "\n".join(bulk_body_lines) + "\n"

    # Send bulk request
    bulk_url = f"{elasticsearch_url}/_bulk"
    headers = {"Content-Type": "application/x-ndjson"}

    try:
        response = requests.post(bulk_url, data=bulk_body, headers=headers)
        response.raise_for_status()
        result = response.json()

        # Check for errors in bulk response
        if result.get("errors"):
            failed_items = [
                item
                for item in result.get("items", [])
                if "error" in item.get("index", {})
            ]
            logger.error(f"Bulk indexing had {len(failed_items)} errors")
            for item in failed_items[:5]:  # Show first 5 errors
                logger.error(f"  Error: {item}")
            raise Exception(f"Bulk indexing failed for {len(failed_items)} events")

        logger.info(f"✅ Successfully indexed {len(events)} events to Elasticsearch")

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send events to Elasticsearch: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Backfill synthetic activity events for DataHub analytics"
    )
    parser.add_argument(
        "--users-file",
        required=True,
        help="JSON file containing user profiles (from generate_users.py)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to backfill (default: 30)",
    )
    parser.add_argument(
        "--events-per-day",
        type=int,
        default=200,
        help="Target number of events per day (default: 200)",
    )
    parser.add_argument(
        "--gms-url",
        default="http://localhost:8080",
        help="DataHub GMS URL (default: http://localhost:8080)",
    )
    parser.add_argument(
        "--elasticsearch-url",
        default="http://localhost:9200",
        help="Elasticsearch URL (default: http://localhost:9200)",
    )
    parser.add_argument(
        "--token",
        help="DataHub authentication token (optional, for future use)",
    )
    parser.add_argument(
        "--output-file",
        help="Optional: Output file for generated events (JSON). If not provided, events are only sent to Elasticsearch.",
    )
    parser.add_argument(
        "--entity-urns-file",
        help="Optional: JSON file containing list of entity URNs to use",
    )
    parser.add_argument(
        "--load-to-elasticsearch",
        action="store_true",
        help="Load events directly to Elasticsearch after generation",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible event generation (default: 42)",
    )
    parser.add_argument(
        "--ensure-test-coverage",
        action="store_true",
        help="Ensure required entity types (dataset, dashboard, chart) are always present for tests",
    )

    args = parser.parse_args()

    # Set random seed for deterministic generation
    random.seed(args.seed)
    logger.info(f"Using random seed: {args.seed}")

    # Load users
    logger.info(f"Loading users from {args.users_file}...")
    with open(args.users_file, "r") as f:
        users = json.load(f)
    logger.info(f"Loaded {len(users)} users")

    # Load entity URNs if provided
    entity_urns = []
    if args.entity_urns_file:
        logger.info(f"Loading entity URNs from {args.entity_urns_file}...")
        with open(args.entity_urns_file, "r") as f:
            entity_urns = json.load(f)
        logger.info(f"Loaded {len(entity_urns)} entity URNs")

    # Initialize generator
    generator = ActivityEventGenerator(
        gms_url=args.gms_url,
        token=args.token or "",
        users=users,
        entity_urns=entity_urns,
    )

    # Generate events for each day
    all_events = []
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=args.days)

    logger.info(f"Generating events from {start_date.date()} to {end_date.date()}...")

    current_date = start_date
    day_index = 0
    while current_date <= end_date:
        # Weekend activity is reduced
        is_weekend = current_date.weekday() >= 5
        target_events = args.events_per_day // 3 if is_weekend else args.events_per_day

        # Only add guaranteed coverage events on the first weekday for efficiency
        should_ensure_coverage = args.ensure_test_coverage and day_index == 0

        day_events = generator.generate_events_for_date(
            current_date, target_events, ensure_coverage=should_ensure_coverage
        )
        all_events.extend(day_events)

        current_date += timedelta(days=1)
        day_index += 1

    logger.info(f"Generated {len(all_events)} total events")

    # Event statistics
    event_types: Dict[str, int] = {}
    for event in all_events:
        event_type = event.get("type", "Unknown")
        event_types[event_type] = event_types.get(event_type, 0) + 1

    logger.info("Event type breakdown:")
    for event_type, count in sorted(event_types.items()):
        logger.info(f"  {event_type}: {count}")

    # Save to file if requested
    if args.output_file:
        with open(args.output_file, "w") as f:
            json.dump(all_events, f, indent=2)
        logger.info(f"Saved events to {args.output_file}")

    # Load to Elasticsearch if requested
    if args.load_to_elasticsearch:
        send_events_to_elasticsearch(
            events=all_events,
            elasticsearch_url=args.elasticsearch_url,
        )

    logger.info("✅ Event generation complete!")


if __name__ == "__main__":
    main()

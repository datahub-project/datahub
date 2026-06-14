"""Utilities for setting up Metabase test environment via Docker."""

import logging
import time
from typing import Any, Dict, Optional

import requests
import tenacity
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


def build_retry_session(
    total: int = 3,
    backoff_factor: float = 1,
    status_forcelist: Optional[list] = None,
) -> requests.Session:
    """Create a requests.Session pre-configured with retry behavior."""
    if status_forcelist is None:
        status_forcelist = [500, 502, 503, 504, 429]

    session = requests.Session()
    retry_strategy = Retry(
        total=total, backoff_factor=backoff_factor, status_forcelist=status_forcelist
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


class MetabaseTestClient:
    """Client for setting up Metabase test environment."""

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = build_retry_session(
            total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504, 429]
        )
        self.session_token: Optional[str] = None

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(10),
        wait=tenacity.wait_exponential(multiplier=1, min=2, max=30),
        retry=tenacity.retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True,
    )
    def setup_initial_user(
        self, email: str, password: str, first_name: str, last_name: str
    ) -> Dict[str, Any]:
        """Setup the initial admin user in Metabase."""
        setup_payload = {
            "token": self.get_setup_token(),
            "user": {
                "email": email,
                "first_name": first_name,
                "last_name": last_name,
                "password": password,
                "site_name": "DataHub Test",
            },
            "prefs": {"site_name": "DataHub Test", "allow_tracking": False},
        }

        response = self.session.post(
            f"{self.base_url}/api/setup", json=setup_payload, timeout=30
        )
        response.raise_for_status()
        logger.info(f"Successfully setup initial user: {email}")
        return response.json()

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(10),
        wait=tenacity.wait_exponential(multiplier=1, min=2, max=30),
        retry=tenacity.retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True,
    )
    def get_setup_token(self) -> str:
        """Get the setup token required for initial setup."""
        response = self.session.get(
            f"{self.base_url}/api/session/properties", timeout=15
        )
        response.raise_for_status()
        props = response.json()
        return props.get("setup-token", "")

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=30),
        retry=tenacity.retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True,
    )
    def login(self, email: str, password: str) -> str:
        """Login to Metabase and get session token."""
        response = self.session.post(
            f"{self.base_url}/api/session",
            json={"username": email, "password": password},
            timeout=15,
        )
        response.raise_for_status()
        session_data = response.json()
        self.session_token = session_data.get("id")
        logger.info(f"Successfully logged in as {email}")
        return self.session_token

    def _get_headers(self) -> Dict[str, str]:
        """Get headers with session token."""
        if not self.session_token:
            raise ValueError("Not logged in. Call login() first.")
        return {
            "X-Metabase-Session": self.session_token,
            "Content-Type": "application/json",
        }

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=30),
        retry=tenacity.retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True,
    )
    def create_database(
        self, name: str, engine: str, details: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create a database connection in Metabase."""
        payload = {
            "name": name,
            "engine": engine,
            "details": details,
            "is_on_demand": False,
            "is_full_sync": True,
            "auto_run_queries": True,
        }

        response = self.session.post(
            f"{self.base_url}/api/database",
            headers=self._get_headers(),
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        logger.info(f"Successfully created database: {name}")
        return response.json()

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=30),
        retry=tenacity.retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True,
    )
    def sync_database(self, database_id: int) -> None:
        """Trigger database sync and wait for completion."""
        response = self.session.post(
            f"{self.base_url}/api/database/{database_id}/sync_schema",
            headers=self._get_headers(),
            timeout=30,
        )
        response.raise_for_status()
        logger.info(f"Database sync triggered for database_id: {database_id}")

        # Wait for sync to complete
        time.sleep(10)

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=30),
        retry=tenacity.retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True,
    )
    def create_collection(self, name: str, description: str = "") -> Dict[str, Any]:
        """Create a collection in Metabase."""
        payload = {"name": name, "description": description, "color": "#509EE3"}

        response = self.session.post(
            f"{self.base_url}/api/collection",
            headers=self._get_headers(),
            json=payload,
            timeout=15,
        )
        response.raise_for_status()
        logger.info(f"Successfully created collection: {name}")
        return response.json()

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=30),
        retry=tenacity.retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True,
    )
    def create_card(
        self,
        name: str,
        database_id: int,
        query: Dict[str, Any],
        collection_id: Optional[int] = None,
        display: str = "table",
        dataset: bool = False,
    ) -> Dict[str, Any]:
        """Create a card (question/model) in Metabase."""
        payload = {
            "name": name,
            "database_id": database_id,
            "dataset_query": query,
            "display": display,
            "visualization_settings": {},
            "dataset": dataset,
        }

        if collection_id:
            payload["collection_id"] = collection_id

        response = self.session.post(
            f"{self.base_url}/api/card",
            headers=self._get_headers(),
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        logger.info(f"Successfully created {'model' if dataset else 'card'}: {name}")
        return response.json()

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=30),
        retry=tenacity.retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True,
    )
    def create_dashboard(
        self, name: str, collection_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Create a dashboard in Metabase."""
        payload: Dict[str, Any] = {
            "name": name,
            "description": f"Test dashboard: {name}",
        }

        if collection_id:
            payload["collection_id"] = collection_id

        response = self.session.post(
            f"{self.base_url}/api/dashboard",
            headers=self._get_headers(),
            json=payload,
            timeout=15,
        )
        response.raise_for_status()
        logger.info(f"Successfully created dashboard: {name}")
        return response.json()

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=30),
        retry=tenacity.retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True,
    )
    def add_card_to_dashboard(self, dashboard_id: int, card_id: int) -> Dict[str, Any]:
        """Add a card to a dashboard."""
        payload = {
            "cardId": card_id,
            "col": 0,
            "row": 0,
            "size_x": 4,
            "size_y": 4,
        }

        response = self.session.post(
            f"{self.base_url}/api/dashboard/{dashboard_id}/cards",
            headers=self._get_headers(),
            json=payload,
            timeout=15,
        )
        logger.info(f"Add card response status: {response.status_code}")
        if response.status_code >= 400:
            logger.error(f"Failed to add card. Response: {response.text}")
        response.raise_for_status()
        logger.info(f"Successfully added card {card_id} to dashboard {dashboard_id}")
        return response.json()


def verify_metabase_api_ready(base_url: str, timeout: int = 120) -> None:
    """Verify that Metabase API is fully accessible."""
    session = build_retry_session(
        total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504, 429]
    )

    for attempt in tenacity.Retrying(
        stop=tenacity.stop_after_delay(timeout),
        wait=tenacity.wait_fixed(3),
        reraise=True,
    ):
        with attempt:
            health_url = f"{base_url}/api/health"
            resp = session.get(health_url, timeout=15)
            if resp.status_code != 200:
                raise AssertionError(f"Health endpoint not ready: {resp.status_code}")

            # Also check session properties endpoint
            props_url = f"{base_url}/api/session/properties"
            props_resp = session.get(props_url, timeout=15)
            if props_resp.status_code != 200:
                raise AssertionError(
                    f"Properties endpoint not ready: {props_resp.status_code}"
                )

            logger.info("Metabase API endpoints fully ready")
            return


def setup_metabase_test_data(base_url: str, credentials: Dict[str, str]) -> None:
    """Setup test data in Metabase including database, collections, cards, and dashboards."""
    client = MetabaseTestClient(base_url)

    # Check if setup is needed
    try:
        session_props = client.session.get(
            f"{base_url}/api/session/properties", timeout=15
        )
        session_props.raise_for_status()
        props = session_props.json()

        if props.get("has-user-setup"):
            logger.info("Metabase already has users, skipping setup")
            client.login(credentials["email"], credentials["password"])
        else:
            logger.info("Setting up initial Metabase user")
            client.setup_initial_user(
                email=credentials["email"],
                password=credentials["password"],
                first_name=credentials["first_name"],
                last_name=credentials["last_name"],
            )
            client.login(credentials["email"], credentials["password"])
    except Exception as e:
        logger.warning(f"Error checking setup status: {e}")
        client.login(credentials["email"], credentials["password"])

    # Create PostgreSQL database connection
    postgres_db = client.create_database(
        name="Test PostgreSQL",
        engine="postgres",
        details={
            "host": "postgres",
            "port": 5432,
            "dbname": "test_db",
            "user": "test_user",
            "password": "test_password",
            "ssl": False,
        },
    )
    db_id = postgres_db["id"]

    # Sync database to get table metadata
    client.sync_database(db_id)

    # Create a collection
    collection = client.create_collection(
        name="Sales Analytics", description="Collection for sales-related queries"
    )
    collection_id = collection["id"]

    # Create a simple card (question)
    simple_card = client.create_card(
        name="All Products",
        database_id=db_id,
        query={
            "type": "query",
            "database": db_id,
            "query": {
                "source-table": 1  # This will be the products table
            },
        },
        collection_id=collection_id,
        display="table",
    )

    # Create a native SQL card
    sql_card = client.create_card(
        name="Order Summary",
        database_id=db_id,
        query={
            "type": "native",
            "database": db_id,
            "native": {
                "query": "SELECT p.category, COUNT(o.id) as order_count, SUM(o.quantity) as total_quantity FROM products p JOIN orders o ON p.id = o.product_id GROUP BY p.category"
            },
        },
        collection_id=collection_id,
        display="bar",
    )

    # Create a model (dataset)
    model_card = client.create_card(
        name="Product Sales Model",
        database_id=db_id,
        query={
            "type": "native",
            "database": db_id,
            "native": {
                "query": "SELECT p.id, p.name, p.category, p.price, SUM(o.quantity) as total_sold FROM products p LEFT JOIN orders o ON p.id = o.product_id GROUP BY p.id, p.name, p.category, p.price"
            },
        },
        collection_id=collection_id,
        display="table",
        dataset=True,  # Mark as model
    )

    # Create a dashboard
    dashboard = client.create_dashboard(
        name="Sales Dashboard", collection_id=collection_id
    )
    logger.info(f"Created dashboard with response: {dashboard}")

    # Add cards to dashboard
    try:
        client.add_card_to_dashboard(dashboard["id"], simple_card["id"])
        client.add_card_to_dashboard(dashboard["id"], sql_card["id"])
        client.add_card_to_dashboard(dashboard["id"], model_card["id"])
    except Exception as e:
        logger.error(f"Failed to add cards to dashboard {dashboard['id']}: {e}")
        logger.error(f"Dashboard object: {dashboard}")
        logger.error(f"Simple card: {simple_card}")
        # Continue anyway - the dashboard and cards exist even if adding fails
        logger.warning("Continuing despite card addition failure")

    logger.info("Metabase test data setup completed successfully")

#!/usr/bin/env python3
"""
Setup script for DuckDB test database for Snowplow integration tests.

This script creates a local DuckDB database with a simplified atomic.events table
structure mimicking Snowplow's warehouse schema. Used for testing warehouse
lineage and event data extraction.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from random import choice, randint
from uuid import uuid4

import duckdb


def create_database(db_path: str = "snowplow_test.duckdb") -> None:
    """Create DuckDB database with Snowplow atomic.events table."""

    conn = duckdb.connect(db_path)

    # Create schema
    conn.execute("CREATE SCHEMA IF NOT EXISTS snowplow")

    # Create atomic.events table (simplified structure)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snowplow.events (
            -- Base Snowplow columns
            app_id VARCHAR,
            platform VARCHAR,
            etl_tstamp TIMESTAMP,
            collector_tstamp TIMESTAMP,
            dvce_created_tstamp TIMESTAMP,
            event VARCHAR,
            event_id VARCHAR,
            txn_id INTEGER,
            name_tracker VARCHAR,

            -- User identifiers
            user_id VARCHAR,
            user_ipaddress VARCHAR,

            -- Page context
            page_url VARCHAR,
            page_title VARCHAR,
            page_referrer VARCHAR,

            -- Device context
            br_name VARCHAR,
            br_family VARCHAR,
            os_name VARCHAR,
            os_family VARCHAR,

            -- Geo context (from IP Lookup enrichment)
            geo_country VARCHAR,
            geo_region VARCHAR,
            geo_city VARCHAR,
            geo_zipcode VARCHAR,
            geo_latitude DOUBLE,
            geo_longitude DOUBLE,

            -- Custom event contexts (JSON columns)
            contexts_com_acme_checkout_started_1 JSON,
            contexts_com_acme_product_viewed_1 JSON,
            contexts_com_acme_user_context_1 JSON,

            -- Unstruct event (self-describing events)
            unstruct_event_com_acme_checkout_started_1 JSON,
            unstruct_event_com_acme_product_viewed_1 JSON,

            -- Derived timestamp
            derived_tstamp TIMESTAMP,

            -- Load timestamp
            load_tstamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    print(f"✅ Created database: {db_path}")
    print("✅ Created schema: snowplow")
    print("✅ Created table: snowplow.events")

    conn.close()


def generate_sample_events(
    db_path: str = "snowplow_test.duckdb", count: int = 100
) -> None:
    """Generate sample events and insert into DuckDB."""

    conn = duckdb.connect(db_path)

    events = []
    base_time = datetime.now() - timedelta(days=30)

    for i in range(count):
        event_time = base_time + timedelta(seconds=i * 300)  # 5 min intervals

        # Randomly choose event type
        event_type = choice(["checkout_started", "product_viewed"])

        # Base event data
        event = {
            "app_id": "web",
            "platform": "web",
            "etl_tstamp": event_time,
            "collector_tstamp": event_time,
            "dvce_created_tstamp": event_time,
            "event": "unstruct" if event_type == "checkout_started" else "struct",
            "event_id": str(uuid4()),
            "txn_id": i,
            "name_tracker": "sp",
            "user_id": f"user_{randint(1, 100)}",
            "user_ipaddress": f"{randint(1, 255)}.{randint(1, 255)}.{randint(1, 255)}.{randint(1, 255)}",
            "page_url": f"https://example.com/{choice(['checkout', 'product', 'cart'])}",
            "page_title": f"Page {i}",
            "page_referrer": "https://google.com",
            "br_name": choice(["Chrome", "Firefox", "Safari"]),
            "br_family": "Chrome",
            "os_name": choice(["Windows", "macOS", "Linux"]),
            "os_family": "Unix",
            "geo_country": choice(["US", "GB", "CA"]),
            "geo_region": "CA",
            "geo_city": choice(["San Francisco", "London", "Toronto"]),
            "geo_zipcode": "94102",
            "geo_latitude": 37.7749,
            "geo_longitude": -122.4194,
            "derived_tstamp": event_time,
            "load_tstamp": datetime.now(),
        }

        # Add event-specific data
        if event_type == "checkout_started":
            checkout_data = {
                "amount": randint(1000, 100000),
                "currency": choice(["USD", "EUR", "GBP"]),
                "discount_code": choice([None, "SAVE10", "WELCOME20"]),
                "items": [
                    {
                        "product_id": f"prod_{randint(1, 50)}",
                        "quantity": randint(1, 5),
                        "price": randint(1000, 50000),
                    }
                ],
            }
            event["unstruct_event_com_acme_checkout_started_1"] = json.dumps(
                checkout_data
            )
            event["contexts_com_acme_checkout_started_1"] = json.dumps([checkout_data])

        elif event_type == "product_viewed":
            product_data = {
                "product_id": f"prod_{randint(1, 50)}",
                "category": choice(["electronics", "clothing", "books"]),
                "price": randint(1000, 100000),
                "user_id": event["user_id"],
            }
            event["unstruct_event_com_acme_product_viewed_1"] = json.dumps(product_data)
            event["contexts_com_acme_product_viewed_1"] = json.dumps([product_data])

        # Add user context
        user_context = {
            "user_id": event["user_id"],
            "user_type": choice(["free", "premium", "enterprise"]),
            "registration_date": (base_time - timedelta(days=randint(1, 365))).strftime(
                "%Y-%m-%d"
            ),
        }
        event["contexts_com_acme_user_context_1"] = json.dumps([user_context])

        events.append(event)

    # Insert events
    for event in events:
        conn.execute(
            """
            INSERT INTO snowplow.events (
                app_id, platform, etl_tstamp, collector_tstamp, dvce_created_tstamp,
                event, event_id, txn_id, name_tracker,
                user_id, user_ipaddress,
                page_url, page_title, page_referrer,
                br_name, br_family, os_name, os_family,
                geo_country, geo_region, geo_city, geo_zipcode, geo_latitude, geo_longitude,
                contexts_com_acme_checkout_started_1,
                contexts_com_acme_product_viewed_1,
                contexts_com_acme_user_context_1,
                unstruct_event_com_acme_checkout_started_1,
                unstruct_event_com_acme_product_viewed_1,
                derived_tstamp, load_tstamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                event["app_id"],
                event["platform"],
                event["etl_tstamp"],
                event["collector_tstamp"],
                event["dvce_created_tstamp"],
                event["event"],
                event["event_id"],
                event["txn_id"],
                event["name_tracker"],
                event["user_id"],
                event["user_ipaddress"],
                event["page_url"],
                event["page_title"],
                event["page_referrer"],
                event["br_name"],
                event["br_family"],
                event["os_name"],
                event["os_family"],
                event["geo_country"],
                event["geo_region"],
                event["geo_city"],
                event["geo_zipcode"],
                event["geo_latitude"],
                event["geo_longitude"],
                event.get("contexts_com_acme_checkout_started_1"),
                event.get("contexts_com_acme_product_viewed_1"),
                event.get("contexts_com_acme_user_context_1"),
                event.get("unstruct_event_com_acme_checkout_started_1"),
                event.get("unstruct_event_com_acme_product_viewed_1"),
                event["derived_tstamp"],
                event["load_tstamp"],
            ),
        )

    # Verify
    result = conn.execute("SELECT COUNT(*) FROM snowplow.events").fetchone()
    if result:
        print(f"✅ Inserted {result[0]} events into snowplow.events")

    conn.close()


def print_sample_query(db_path: str = "snowplow_test.duckdb") -> None:
    """Print sample queries for testing."""

    print("\n" + "=" * 80)
    print("Sample Queries for Testing")
    print("=" * 80)

    print("\n1. Count events by type:")
    print(
        f'   duckdb {db_path} "SELECT event, COUNT(*) FROM snowplow.events GROUP BY event"'
    )

    print("\n2. View checkout events:")
    print(
        f"   duckdb {db_path} \"SELECT event_id, user_id, unstruct_event_com_acme_checkout_started_1 FROM snowplow.events WHERE event = 'unstruct' LIMIT 5\""
    )

    print("\n3. View all columns:")
    print(f'   duckdb {db_path} "SELECT * FROM snowplow.events LIMIT 1"')

    print("\n4. Python connection:")
    print("""
    import duckdb
    conn = duckdb.connect('snowplow_test.duckdb')
    result = conn.execute("SELECT * FROM snowplow.events LIMIT 5").fetchall()
    conn.close()
    """)

    print("=" * 80 + "\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Setup DuckDB test database for Snowplow"
    )
    parser.add_argument(
        "--db-path", default="snowplow_test.duckdb", help="Path to DuckDB database file"
    )
    parser.add_argument(
        "--event-count",
        type=int,
        default=100,
        help="Number of sample events to generate",
    )
    parser.add_argument(
        "--recreate", action="store_true", help="Recreate database (delete existing)"
    )

    args = parser.parse_args()

    # Recreate if requested
    if args.recreate:
        db_file = Path(args.db_path)
        if db_file.exists():
            db_file.unlink()
            print(f"🗑️  Deleted existing database: {args.db_path}")

    # Create database
    create_database(args.db_path)

    # Generate sample events
    generate_sample_events(args.db_path, args.event_count)

    # Print sample queries
    print_sample_query(args.db_path)

    print("✅ Setup complete!")

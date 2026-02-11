# DataHub Analytics Backfill Tools

Populate DataHub analytics dashboards with synthetic user activity data for testing and demonstration purposes.

## Overview

These scripts generate realistic user profiles and activity events with historical timestamps to populate all DataHub analytics charts including:

- **Weekly Active Users (WAU)**
- **Monthly Active Users (MAU)**
- **New Users (Last 30 Days)**
- **Top Users (Last 30 Days)**
- **Number of Searches**
- **Top Searches (Past Week)**
- **Top Viewed Datasets (Past Week)**
- **Top Viewed Dashboards (Past Week)**
- **Tab Views By Entity Type (Past Week)**
- **Actions By Entity Type (Past Week)**
- **Data Assets by Term**

## Quick Start

### Prerequisites

- DataHub instance running (quickstart or production)
- Python 3.7+ with `acryl-datahub` package installed
- Access to DataHub GMS and Elasticsearch
- DataHub access token (generate from Settings → Access Tokens)
- `jq` and `curl` installed

### One-Command Population

```bash
# Set your DataHub token
export DATAHUB_TOKEN="your-token-here"

# Run the master script with defaults (20 users, 30 days of activity)
./populate_analytics.sh --token $DATAHUB_TOKEN
```

This will:

1. Generate 20 synthetic users
2. Extract existing entity URNs from DataHub
3. Create business glossary terms and attach them to datasets
4. Create ~6,000 activity events over 30 days
5. Load events into Elasticsearch
6. Populate all analytics dashboards

View results at `http://localhost:9002/analytics`

## Scripts

### 1. `populate_analytics.sh` (Master Script)

Orchestrates the entire pipeline.

**Usage:**

```bash
./populate_analytics.sh --token TOKEN [OPTIONS]
```

**Options:**

- `--num-users N` - Number of users to generate (default: 20)
- `--num-days N` - Days of history to generate (default: 30)
- `--events-per-day N` - Target events per day (default: 200)
- `--gms-url URL` - DataHub GMS URL (default: http://localhost:8080)
- `--token TOKEN` - DataHub auth token (required)
- `--elasticsearch-url URL` - Elasticsearch URL (default: http://localhost:9200)
- `--email-domain DOMAIN` - Email domain for users (default: example.com)
- `--skip-users` - Skip user generation
- `--skip-events` - Skip event generation
- `--skip-load` - Skip loading to Elasticsearch

**Examples:**

```bash
# Generate 50 users with 60 days of high activity
./populate_analytics.sh --token $TOKEN --num-users 50 --num-days 60 --events-per-day 500

# Only generate events (users already exist)
./populate_analytics.sh --token $TOKEN --skip-users

# Generate data but don't load yet (for review)
./populate_analytics.sh --token $TOKEN --skip-load
```

### 2. `generate_users.py`

Creates synthetic user profiles and emits them to DataHub.

**Features:**

- Realistic names, emails, titles, departments
- Proper CorpUser aspects (info, editable info, status)
- Configurable email domain
- Saves user profiles to JSON for event generation

**Usage:**

```bash
python generate_users.py \
  --num-users 20 \
  --token $TOKEN \
  --output-file users.json
```

**Output:**

```json
[
  {
    "username": "alice.anderson",
    "first_name": "Alice",
    "last_name": "Anderson",
    "email": "alice.anderson@example.com",
    "display_name": "Alice Anderson",
    "title": "Data Engineer",
    "department": "Engineering",
    "team": "Platform"
  },
  ...
]
```

### 3. `generate_glossary_terms.py`

Creates business glossary terms and attaches them to datasets.

**Features:**

- 20+ predefined business terms across 5 categories (Customer Data, Financial Metrics, Product Metrics, Sales & Marketing, Operations)
- Realistic term definitions and metadata
- Automatic attachment to ~70% of existing datasets
- Each dataset receives 1-3 random terms from different categories
- Populates "Data Assets by Term" analytics chart

**Categories:**

- **Customer Data**: Customer ID, Customer Lifetime Value, Customer Segment, Churn Rate
- **Financial Metrics**: Revenue, Annual Recurring Revenue, Gross Margin, Operating Expenses
- **Product Metrics**: Daily Active Users, Monthly Active Users, User Engagement, Feature Adoption
- **Sales & Marketing**: Lead, Conversion Rate, Customer Acquisition Cost, Marketing Qualified Lead
- **Operations**: Service Level Agreement, Incident, Mean Time To Resolution, Uptime

**Usage:**

```bash
python generate_glossary_terms.py \
  --token $TOKEN \
  --entity-urns-file entity_urns.json \
  --output-file glossary_terms.json
```

**Output:**

```json
{
  "terms": [
    {
      "name": "Customer ID",
      "urn": "urn:li:glossaryTerm:customer_id",
      "category": "Customer Data",
      "definition": "Unique identifier for a customer"
    },
    ...
  ],
  "attachments": [
    {
      "dataset_urn": "urn:li:dataset:...",
      "term_urns": ["urn:li:glossaryTerm:customer_id", ...],
      "num_terms": 2
    },
    ...
  ]
}
```

### 4. `backfill_activity_events.py`

Generates realistic user activity events with backdated timestamps.

**Event Types Generated:**

- `EntityViewEvent` - Dataset, dashboard, chart views
- `EntityActionEvent` - Tab views, actions on entities
- `SearchEvent` - Search queries
- `SearchResultsViewEvent` - Search results viewed
- `HomePageViewEvent` - Home page visits
- `LogInEvent` - User login events

**Features:**

- Realistic temporal patterns (working hours, weekdays vs weekends)
- Power user simulation (20% of users generate 80% of activity)
- Session-based activity (login → browse → search → view entities)
- Varied entity types and search queries

**Usage:**

```bash
# Generate and save to file
python backfill_activity_events.py \
  --users-file users.json \
  --days 30 \
  --events-per-day 200 \
  --output-file activity_events.json

# Generate and load directly to Elasticsearch (for CI/automated tests)
python backfill_activity_events.py \
  --users-file users.json \
  --days 30 \
  --events-per-day 200 \
  --elasticsearch-url http://localhost:9200 \
  --load-to-elasticsearch

# Both save to file AND load to Elasticsearch
python backfill_activity_events.py \
  --users-file users.json \
  --days 30 \
  --events-per-day 200 \
  --output-file activity_events.json \
  --elasticsearch-url http://localhost:9200 \
  --load-to-elasticsearch
```

**Key Feature: Relative Timestamps**

The script generates events with timestamps **relative to execution time**, ensuring "Past Week" and "Past Month" analytics always have fresh data regardless of when tests run. This is critical for CI environments where tests must pass consistently over time.

**Output:**

```json
[
  {
    "type": "EntityViewEvent",
    "timestamp": 1696780800000,
    "actorUrn": "urn:li:corpuser:alice.anderson",
    "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)",
    "entityType": "dataset",
    "usageSource": "web"
  },
  ...
]
```

### 5. `load_events_to_elasticsearch.sh`

Loads events into Elasticsearch using the bulk API.

**Features:**

- Batch processing for large event volumes
- Progress reporting
- Error handling
- Index refresh after load

**Usage:**

```bash
./load_events_to_elasticsearch.sh activity_events.json

# Custom Elasticsearch
./load_events_to_elasticsearch.sh \
  -e http://elasticsearch:9200 \
  -i datahub_usage_event \
  activity_events.json
```

## Configuration

### Environment Variables

```bash
# DataHub configuration
export DATAHUB_GMS_URL="http://localhost:8080"
export DATAHUB_TOKEN="your-token-here"

# Elasticsearch configuration
export ELASTICSEARCH_URL="http://localhost:9200"

# Generation parameters
export NUM_USERS=20
export NUM_DAYS=30
export EVENTS_PER_DAY=200
export EMAIL_DOMAIN="example.com"
```

### Customization

#### Custom Entity URNs

Provide your own entity URNs for more realistic activity:

```bash
# Extract entities from your DataHub instance
datahub get --urn "urn:li:dataset:*" > my_entities.json

# Use in event generation
python backfill_activity_events.py \
  --entity-urns-file my_entities.json \
  ...
```

#### Temporal Patterns

Events are generated with realistic patterns:

- **Weekdays**: Full `EVENTS_PER_DAY` count
- **Weekends**: 33% of `EVENTS_PER_DAY`
- **Working Hours**: 9 AM - 6 PM
- **Power Users**: 20% of users generate 80% of events

## Generated Data Structure

### Users

- **Count**: Configurable (default: 20)
- **Departments**: Engineering, Data Science, Product, Marketing, etc.
- **Titles**: Data Engineer, Data Scientist, Analytics Engineer, etc.
- **Teams**: Platform, Growth, Data Infrastructure, etc.

### Activity Events

- **Volume**: `NUM_DAYS × EVENTS_PER_DAY` (default: 6,000 events)
- **Time Range**: Last N days with backdated timestamps
- **Event Distribution**:
  - Entity Views: ~40%
  - Tab Views: ~25%
  - Searches: ~20%
  - Page Views: ~10%
  - Logins: ~5%

## Analytics Charts Populated

### User Analytics

- **Weekly Active Users** - Distinct users per week
- **Monthly Active Users** - Distinct users per month
- **New Users (Last 30 Days)** - First-time users
- **Top Users (Last 30 Days)** - Most active users

### Search Analytics

- **Number of Searches** - Total search count over time
- **Top Searches (Past Week)** - Most frequent queries

### Entity Analytics

- **Top Viewed Datasets (Past Week)** - Most viewed datasets
- **Top Viewed Dashboards (Past Week)** - Most viewed dashboards
- **Tab Views By Entity Type (Past Week)** - Tab views breakdown
- **Actions By Entity Type (Past Week)** - Actions breakdown

### Metadata Analytics

- **Data Assets by Term** - Dataset distribution across glossary terms

## Troubleshooting

### "Cannot connect to DataHub GMS"

- Verify DataHub is running: `curl http://localhost:8080/health`
- Check `--gms-url` parameter

### "Cannot connect to Elasticsearch"

- Verify Elasticsearch is running: `curl http://localhost:9200/_cluster/health`
- Check `--elasticsearch-url` parameter
- For Docker: `docker ps | grep elasticsearch`

### "datahub Python package not found"

```bash
pip install 'acryl-datahub[datahub-rest]'
```

### "jq command not found"

```bash
# macOS
brew install jq

# Ubuntu/Debian
sudo apt-get install jq

# RHEL/CentOS
sudo yum install jq
```

### Analytics charts not showing data

1. Wait a few minutes for Elasticsearch indexing
2. Refresh the analytics page
3. Check index exists: `curl http://localhost:9200/datahub_usage_event/_count`
4. Verify events loaded: Check the count is > 0

### Events loaded but charts still empty

- DataHub filters out backend-generated events
- Ensure `usageSource` is set to "web" (scripts handle this)
- Check DataHub logs for errors

## Advanced Usage

### Integration with Smoke Tests

Analytics tests now use pytest fixtures for automatic data loading with relative timestamps:

```python
# In smoke-test/tests/analytics/test_analytics.py
def test_weekly_active_users_chart(auth_session, analytics_events_loaded):
    """Test Weekly Active Users chart - fixture ensures fresh data."""
    # Query analytics charts...
    # Assertions will pass because data is generated relative to execution time
```

The `analytics_events_loaded` fixture (defined in `tests/analytics/conftest.py`):

- Generates events with timestamps relative to current execution time
- Loads directly to Elasticsearch via bulk API
- Ensures "Past Week" and "Past Month" charts always have data
- Runs once per test session for efficiency

**Benefits:**

- ✅ Tests pass consistently regardless of when they run
- ✅ No stale pre-generated JSON files
- ✅ Fresh analytics data for every CI run
- ✅ Automatic cleanup between test sessions

### Continuous Data Generation

For ongoing testing:

```bash
# Cron job to add daily activity
0 0 * * * cd /path/to/analytics_backfill && ./populate_analytics.sh --token $TOKEN --num-days 1 --skip-users
```

### Custom Activity Patterns

Modify `backfill_activity_events.py` to add custom patterns:

```python
# Add specific events for your use case
def generate_dashboard_migration_events(self, date):
    """Simulate a dashboard migration project."""
    events = []
    for dashboard_urn in self.dashboard_urns:
        # Heavy activity on specific dashboards
        for _ in range(50):
            events.append(self.generate_entity_view_event(date, entity_urn=dashboard_urn))
    return events
```

## Best Practices

1. **Start Small**: Test with 10 users and 7 days first
2. **Review Before Loading**: Use `--skip-load` to review generated events
3. **Backup**: Backup Elasticsearch before loading large datasets
4. **Realistic Numbers**: 20-50 users, 30-90 days is realistic for most demos
5. **Clean Up**: Document how to remove test data if needed

## Cleanup

To remove generated data:

```bash
# Delete test users
datahub delete --urn "urn:li:corpuser:alice.anderson"

# Delete usage events index
curl -X DELETE http://localhost:9200/datahub_usage_event

# Recreate empty index (will be auto-created on next event)
```

## Support

For issues or questions:

1. Check DataHub logs: `docker logs datahub-gms`
2. Check Elasticsearch logs: `docker logs elasticsearch`
3. Review generated JSON files for correctness
4. Open an issue in the DataHub repository

## License

These tools are part of the DataHub project and follow the same Apache 2.0 license.

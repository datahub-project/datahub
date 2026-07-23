### Overview

The `snowplow` module ingests metadata from Snowplow into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

The Snowplow source extracts metadata from Snowplow's behavioral data platform, including:

- **Event schemas** - Self-describing event definitions with properties and validation rules
- **Entity schemas** - Context and entity schemas attached to events
- **Event specifications** - Tracking requirements and specifications (BDP only)
- **Tracking scenarios** - Groupings of related events (BDP only)
- **Organizations** - Top-level containers for all schemas

Snowplow is an open-source behavioral data platform that collects, validates, and models event-level data. This connector supports both:

- **Snowplow BDP** (Behavioral Data Platform) - Managed Snowplow with Console API
- **Open-source Snowplow** - Self-hosted with Iglu schema registry

#### References

- [Snowplow Documentation](https://docs.snowplow.io/)
- [Snowplow BDP Console API](https://console.snowplowanalytics.com/api/msc/v1/docs/)
- [Iglu Schema Registry](https://docs.snowplow.io/docs/api-reference/iglu/)
- [SchemaVer Specification](https://docs.snowplow.io/docs/api-reference/iglu/common-architecture/schemaver/)
- [Snowplow GitHub](https://github.com/snowplow/snowplow)

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

#### For Snowplow BDP (Managed)

1. **Snowplow BDP account** with Console access
2. **Organization ID** - Found in Console URL: `https://console.snowplowanalytics.com/{org-id}/...`
3. **API credentials** - Generated from Console → Settings → API Credentials:
   - API Key ID
   - API Key Secret

#### For Open-Source Snowplow

1. **Iglu Schema Registry** - URL of your Iglu server
2. **API Key** (optional) - Required for private Iglu registries

#### Python Requirements

- Python 3.8 or newer
- DataHub CLI installed

#### Snowplow BDP API Permissions

The connector requires **read-only access** to the following BDP Console API endpoints:

##### Minimum Required Permissions

To extract basic schema metadata:

- **`read:data-structures`** - Read access to data structures (event and entity schemas)
- **`read:organizations`** - Access to organization information

##### Permissions by Capability

| Capability               | Required Permissions      | Configuration                        |
| ------------------------ | ------------------------- | ------------------------------------ |
| **Schema Metadata**      | `read:data-structures`    | Enabled by default                   |
| **Event Specifications** | `read:event-specs`        | `extract_event_specifications: true` |
| **Tracking Scenarios**   | `read:tracking-scenarios` | `extract_tracking_scenarios: true`   |
| **Tracking Plans**       | `read:data-products`      | `extract_tracking_plans: true`       |

##### Permission Testing

Test your API credentials and permissions:

```bash
# Get JWT token
curl -X POST \
  -H "X-API-Key-ID: <API_KEY_ID>" \
  -H "X-API-Key: <API_KEY>" \
  https://console.snowplowanalytics.com/api/msc/v1/organizations/<ORG_ID>/credentials/v3/token

# List data structures
curl -H "Authorization: Bearer <JWT>" \
  https://console.snowplowanalytics.com/api/msc/v1/organizations/<ORG_ID>/data-structures/v1
```

#### Iglu Registry Permissions

For **open-source Snowplow** with Iglu:

- **Public registries**: No authentication required (e.g., Iglu Central)
- **Private registries**: API key with read access to schemas

#### Configuration

See the recipe files for complete configuration examples:

- [snowplow_recipe.yml](snowplow_recipe.yml) - Comprehensive configuration with all options
- [snowplow_bdp_basic.yml](snowplow_bdp_basic.yml) - Minimal BDP configuration
- [snowplow_iglu.yml](snowplow_iglu.yml) - Open-source Iglu configuration
- [snowplow_with_filtering.yml](snowplow_with_filtering.yml) - Schema filtering examples

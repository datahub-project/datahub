# SQL Execution for DataHub Chatbot - Technical Specification

**Version:** 1.0  
**Date:** October 31, 2025  
**Status:** Draft for Review

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Background and Motivation](#background-and-motivation)
3. [Architecture Overview](#architecture-overview)
4. [Authentication and Authorization](#authentication-and-authorization)
5. [API Design](#api-design)
6. [SQL Execution Service](#sql-execution-service)
7. [Access Control Strategies](#access-control-strategies)
8. [Security Considerations](#security-considerations)
9. [Chatbot Integration](#chatbot-integration)
10. [Open Questions and Decisions](#open-questions-and-decisions)

---

## Executive Summary

This specification outlines the design for adding SQL query execution capabilities to the DataHub chatbot, with initial support for Snowflake. The chatbot can already generate SQL queries; this enhancement enables it to execute those queries and return results to users.

### Key Design Decisions

1. **Execute SQL from GMS, not Integrations Service** - Credentials and token refresh stay near storage
2. **Per-User Credentials First** - Primary focus on individual user authentication (OAuth2 + direct credentials)
3. **REST + SSE API** - Use Server-Sent Events for streaming (not GraphQL) to handle long-running queries
4. **Always Require Approval** - SQL execution requires explicit user approval before running
5. **System-Level Connection Optional** - Opt-in feature for organizations, requires customer-managed access control
6. **OAuth2 Recommended** - Better security and UX, though direct credentials also supported

---

## Background and Motivation

### Current State

The DataHub chatbot can:

- Generate SQL queries based on user requests
- Access metadata about datasets, schemas, and columns
- Understand data lineage and relationships

### Gap

The chatbot **cannot execute** the SQL queries it generates, requiring users to:

1. Copy the generated SQL
2. Open their SQL client
3. Connect to Snowflake
4. Execute manually
5. Return to DataHub with results

### Proposed Solution

Enable the chatbot to execute SQL queries directly on Snowflake, with:

- **Per-user authentication**: Each user provides their own Snowflake credentials
- **OAuth2 support**: Seamless, secure authentication without sharing passwords
- **Query approval workflow**: User must approve before execution
- **Streaming results**: Handle long-running queries with progress updates
- **Audit logging**: Track all executions for compliance

---

## Architecture Overview

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                          User Workflow                           │
├─────────────────────────────────────────────────────────────────┤
│  1. User: "Show me revenue from customers table"                │
│  2. Chatbot: Generates SQL: SELECT * FROM revenue.customers     │
│  3. Chatbot: Shows preview + "Execute this query?"              │
│  4. User: Clicks "Execute"                                       │
│  5. Chatbot: Executes on Snowflake, streams results             │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                   Complete System Architecture                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Frontend (React Chat UI)                                        │
│       │                                                          │
│       │ 1. User: "Show me top 10 customers"                     │
│       │                                                          │
│       ▼                                                          │
│  Integrations Service (Python - Chatbot)                         │
│       │                                                          │
│       │ 2. LLM generates SQL query                              │
│       │ 3. Gets approval from user                              │
│       │    [See Approval Mechanism spec]                        │
│       │ 4. Calls SQL execution via MCP tools                    │
│       │                                                          │
│       ▼                                                          │
│  GMS Coordinator (DataHub Cloud)                                 │
│   ┌─────────────────────────────────────┐                       │
│   │ SQL Execution Coordinator           │                       │
│   │  • Enforce approval mechanism       │                       │
│   │  • Authorize user                   │                       │
│   │  • Create ExecutionRequest          │                       │
│   └────────────┬────────────────────────┘                       │
│                │                                                 │
│                │ Dispatch via SQS/Celery                         │
│                ▼                                                 │
│ ┌──────────────────────────────────────────────────┐            │
│ │ Remote Executor (Customer VPC)                   │            │
│ │  ┌─────────────────────────────────────┐         │            │
│ │  │ SQL Query Task Handler              │         │            │
│ │  │  • Get connection from secrets      │         │            │
│ │  │  • Refresh OAuth tokens if needed   │         │            │
│ │  │  • Execute query on Snowflake       │         │            │
│ │  │  • Return results to coordinator    │         │            │
│ │  └────────────┬────────────────────────┘         │            │
│ │               │                                   │            │
│ │               │ Credentials stay in VPC           │            │
│ │               ▼                                   │            │
│ │        Snowflake (customer network)               │            │
│ └──────────────────────────────────────────────────┘            │
│                │                                                 │
│                │ Results                                         │
│                │                                                 │
└────────────────┼─────────────────────────────────────────────────┘
                 │
                 │ Stream back via coordinator
                 ▼
         GMS → Integrations Service → Frontend

Note: Initial implementation may use embedded executor in coordinator,
then migrate to remote executor for production deployments.


┌─────────────────────────────────────────────────────────────────┐
│                   Chatbot Integration Flow                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Integrations Service (Python)                                   │
│       │                                                          │
│       │ MCP Tool: execute_snowflake_query()                     │
│       │                                                          │
│       ├──► Call GMS REST API                                    │
│       │    POST /api/v3/sql/snowflake/execute                   │
│       │                                                          │
│       └──► Return formatted results to LLM                       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Where SQL Execution Happens: Remote Executor Architecture

**Long-term Goal: Execute in Remote Executor (Customer VPC)**

#### Remote Executor Overview

DataHub's Remote Executor is deployed in the customer's VPC/private network:

- **Credentials stay in customer environment** (never leave VPC)
- **Network access controlled by customer** (firewall, security groups)
- **Secure communication** with DataHub Cloud/GMS (outbound only)
- **Already used for**: Ingestion, assertions/monitors

**Architecture:**

```
GMS/Coordinator (DataHub Cloud)
    │
    │ Creates ExecutionRequest
    │ Sends via SQS/Celery
    │
    ▼
Remote Executor (Customer VPC)
    │
    ├─► Has access to Snowflake credentials
    ├─► Executes SQL query on Snowflake
    ├─► Streams results back to GMS
    └─► All credentials stay in VPC
```

**Why Remote Executor for SQL?**

1. **Credential Security**

   - Snowflake credentials stay in customer VPC (never transmitted to cloud)
   - Customer controls credential storage (secrets manager, vault, etc.)
   - Reduces DataHub's security liability

2. **Network Topology**

   - Snowflake may be in private network (VPC peering, private endpoints)
   - Remote executor has network access, GMS doesn't
   - No need to open firewall rules for GMS

3. **Customer Control**

   - Customer manages executor deployment and scaling
   - Customer audits all SQL execution in their environment
   - Customer controls data residency

4. **Consistent Architecture**
   - Follows same pattern as ingestion and assertions
   - Reuses existing executor infrastructure
   - Single deployment model for all data access

#### Implementation Strategy: Start with Coordinator

**Decision: Initial version executes SQL from Coordinator, add Remote Executor support later**

**Why Start with Coordinator?**

1. **Faster Time to Value**

   - No dependency on remote executor deployment
   - Can ship and iterate quickly
   - Validate SQL execution patterns before broader rollout

2. **Remote Executor Update Challenges**

   - Remote executors run in customer deployments
   - **No reliable way to update software** across all customer environments
   - Customers control update cadence
   - New features can take months to reach all executors

3. **Reasonable Intermediate Milestone**
   - Many customers run DataHub in their own VPC (not cloud)
   - Coordinator is in same network as Snowflake for these deployments
   - Solves the chatbot SQL execution problem immediately
   - Can add remote executor support when infrastructure matures

**Deployment Modes:**

| Mode                | Execution Location | Credential Location | Use Case                                   | Timeline   |
| ------------------- | ------------------ | ------------------- | ------------------------------------------ | ---------- |
| **Coordinator**     | GMS process        | GMS (encrypted)     | Initial release, self-hosted DataHub       | Phase 1 ✅ |
| **Remote Executor** | Customer VPC       | Customer secrets    | Cloud deployments, compliance requirements | Future     |

**Migration Path:**

```
Phase 1 (Initial - 4-6 weeks):
  - Implement SQL execution in Coordinator
  - REST API + SSE streaming
  - Per-user credentials stored in GMS
  - Approval mechanism
  - Works for self-hosted and cloud deployments

Phase 2 (Future - When Remote Executor Ecosystem Matures):
  - Implement RUN_SQL_QUERY task type for remote executor
  - Credentials stay in customer VPC
  - Same API, different execution location
  - Gradual migration as customers update executors
```

**Important Notes:**

- Coordinator execution is **not just for dev/testing** - it's a valid deployment mode
- For self-hosted DataHub (on-prem or customer VPC), coordinator is in same network as Snowflake
- Remote executor becomes critical for **DataHub Cloud** where coordinator is outside customer network

#### Task Type: SQL Query Execution

Following existing patterns (`RUN_INGEST_TASK_NAME`, `RUN_ASSERTION_TASK_NAME`):

```python
RUN_SQL_QUERY_TASK_NAME = "RUN_SQL_QUERY"

# ExecutionRequest structure
{
  "executor_id": "customer-vpc-executor",
  "exec_id": "sql_exec_abc123",
  "name": "RUN_SQL_QUERY",
  "args": {
    "query": "SELECT ...",
    "connection_urn": "urn:li:dataHubConnection:snowflake-query-alice",
    "dataset_urn": "urn:li:dataset:(...)",
    "limit": 100,
    "user_urn": "urn:li:corpuser:alice",
    "approval_correlation_id": "corr_abc123"
  }
}
```

#### Why Not Integrations Service?

**Rejected: Execute SQL directly from Integrations Service (Python chatbot)**

**Reasons:**

- Would bypass remote executor architecture
- Credentials would need to be in Integrations Service
- Doesn't align with DataHub's security model
- Can't leverage customer VPC deployment
- Would be different from ingestion/assertions pattern

---

## Authentication and Authorization

### Primary Approach: Per-User Credentials

**Decision: Focus on per-user authentication first, with system-level as optional enhancement**

Each user provides their own Snowflake credentials to DataHub. DataHub executes queries using the user's identity.

#### Authentication Methods Supported

### Method 1: Direct Credentials (Username + Password or Private Key)

**How it works:**

1. User navigates to Settings → Connections → Snowflake
2. Enters Snowflake username and password (or private key)
3. DataHub encrypts credentials using `SecretService` (AES-256-GCM)
4. Stores in connection entity: `urn:li:dataHubConnection:snowflake-query-{userUrn}`
5. When executing SQL, DataHub decrypts and uses these credentials

**Connection Entity Structure:**

```json
{
  "urn": "urn:li:dataHubConnection:snowflake-query-urn:li:corpuser:alice",
  "platform": "urn:li:dataPlatform:snowflake",
  "details": {
    "type": "JSON",
    "name": "Alice's Snowflake Query Connection",
    "json": {
      "blob": "{\"username\":\"alice\",\"password\":\"encrypted...\",\"account_id\":\"abc123\",\"warehouse\":\"COMPUTE_WH\",\"role\":\"analyst_role\",\"authentication_type\":\"DEFAULT_AUTHENTICATOR\"}"
    }
  }
}
```

**Pros:**

- Simple to implement
- Works with any Snowflake authentication setup
- User has full control
- No OAuth2 app configuration needed

**Cons:**

- User shares password with DataHub (security concern for some orgs)
- Manual credential updates when password changes
- Credentials can expire without notification

**When to use:**

- Organizations without Snowflake OAuth2 setup
- Users who prefer direct credential management
- Testing and development environments

---

### Method 2: OAuth2 Delegated Access (Recommended)

**How it works:**

1. User clicks "Connect Snowflake Account" in DataHub Settings
2. DataHub redirects to Snowflake OAuth2 authorization page
3. User logs in to Snowflake (DataHub never sees password)
4. User authorizes DataHub to access Snowflake on their behalf
5. Snowflake redirects back to DataHub with authorization code
6. DataHub exchanges code for refresh token and access token
7. Tokens stored encrypted in connection entity
8. When executing SQL:
   - DataHub checks if access token is expired
   - If expired, uses refresh token to get new access token
   - Executes query with valid access token

**OAuth2 Flow Diagram:**

```
User                DataHub                Snowflake OAuth2
  │                    │                           │
  │ 1. Click Connect   │                           │
  ├───────────────────►│                           │
  │                    │ 2. Redirect to OAuth2     │
  │                    ├──────────────────────────►│
  │                    │                           │
  │                    │       3. User logs in     │
  │                    │       4. Authorizes app   │
  │◄───────────────────┤────────────────────────────┤
  │                    │                           │
  │ 5. Callback with code                         │
  ├───────────────────►│                           │
  │                    │ 6. Exchange code for tokens
  │                    ├──────────────────────────►│
  │                    │◄──────────────────────────┤
  │                    │  refresh_token, access_token
  │                    │                           │
  │                    │ 7. Store tokens (encrypted)
  │◄───────────────────┤                           │
  │  Connected!        │                           │
```

**Token Refresh Flow:**

```
Chatbot requests SQL execution
    │
    ├─► Get user's connection
    │
    ├─► Check if access_token expired?
    │       │
    │       ├─► NO: Use existing access_token
    │       │
    │       └─► YES: Refresh tokens
    │               │
    │               ├─► POST to Snowflake token endpoint
    │               │   with refresh_token
    │               │
    │               ├─► Receive new access_token & refresh_token
    │               │
    │               ├─► Update connection entity
    │               │
    │               └─► Use new access_token
    │
    └─► Execute SQL with access_token
```

**Connection Entity Structure:**

```json
{
  "urn": "urn:li:dataHubConnection:snowflake-query-urn:li:corpuser:alice",
  "platform": "urn:li:dataPlatform:snowflake",
  "details": {
    "type": "JSON",
    "name": "Alice's Snowflake Query Connection (OAuth2)",
    "json": {
      "blob": "{\"username\":\"alice\",\"refresh_token\":\"encrypted...\",\"access_token\":\"encrypted...\",\"token_expires_at\":\"2025-11-01T12:00:00Z\",\"account_id\":\"abc123\",\"warehouse\":\"COMPUTE_WH\",\"role\":\"analyst_role\",\"authentication_type\":\"OAUTH_AUTHENTICATOR\"}"
    }
  }
}
```

**OAuth2 Token Refresh - Architectural Considerations:**

Reference implementations in codebase:

- `datahub-integrations-service/src/datahub_integrations/slack/app_manifest.py:160-189` (Slack)
- `datahub-integrations-service/src/datahub_integrations/teams/graph_api.py:46-92` (Teams)

**1. Concurrency Control: Race Condition Handling**

**Problem:** Multiple concurrent SQL execution requests from same user may all detect expired token and attempt refresh simultaneously.

**Options:**

| Approach                                  | Pros                                                               | Cons                                                                     | Recommendation                            |
| ----------------------------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------------ | ----------------------------------------- |
| **Distributed Lock** (Redis/ZooKeeper)    | Guarantees single refresh, no wasted API calls                     | Requires external infrastructure, adds latency (50-100ms), failure point | ❌ Too complex for initial implementation |
| **Optimistic Locking** (version field)    | Simple, works in distributed environment, no external dependencies | May waste refresh calls if collision happens                             | ✅ **Recommended** - Simple, reliable     |
| **In-Memory Lock** (per-user JVM lock)    | Fast, no external calls                                            | Doesn't work across multiple GMS instances                               | ❌ Doesn't scale                          |
| **Database Row Lock** (SELECT FOR UPDATE) | Standard SQL pattern                                               | Blocks other transactions, potential deadlocks                           | ⚠️ Possible alternative                   |

**Recommended Implementation: Optimistic Locking**

```
Connection Entity Schema:
  - refreshToken (encrypted)
  - accessToken (encrypted)
  - tokenExpiresAt (timestamp)
  - tokenVersion (integer) ← Used for optimistic locking

Token Refresh Flow:
  1. Read connection (get tokenVersion = v1)
  2. Check if token expired → yes
  3. Call Snowflake OAuth endpoint → get new tokens
  4. Update connection WHERE tokenVersion = v1 SET tokenVersion = v2
  5. If update affected 0 rows → Another thread already refreshed → Re-read connection
  6. If update succeeded → Use new tokens
```

**Cost Analysis:**

- If 5 concurrent requests hit expired token:
  - All 5 call Snowflake OAuth endpoint
  - Only 1 update succeeds
  - 4 wasted API calls (acceptable cost for simplicity)
- Snowflake rate limits OAuth calls, so this is safe

**2. Token Refresh Failure Handling**

**Failure Scenarios:**

- Network timeout to Snowflake
- Snowflake OAuth service temporarily down
- Refresh token expired/revoked (user must re-authenticate)
- Invalid OAuth credentials (config issue)

**Strategy:**

| Failure Type           | Retry Strategy                                  | User Impact                      | Monitoring                 |
| ---------------------- | ----------------------------------------------- | -------------------------------- | -------------------------- |
| Network timeout        | 3 retries with exponential backoff (1s, 2s, 4s) | Delayed query execution (max 7s) | Alert if timeout rate > 5% |
| Snowflake service down | No retry, fail fast                             | Error message, try again later   | Alert immediately          |
| Refresh token expired  | No retry, mark connection invalid               | User must re-authenticate        | Track expiration rate      |
| Invalid OAuth config   | No retry, fail fast                             | Admin must fix configuration     | Alert immediately          |

**Connection Invalidation:**

```
When refresh token is expired/revoked:
  1. Mark connection as invalid (new field: status = INVALID)
  2. Log event for admin monitoring
  3. Return user-friendly error: "Your Snowflake connection has expired.
     Please reconnect your account in Settings."
  4. Clear cached connection from memory
```

**3. Token Storage Consistency**

**Challenge:** Token refresh is a two-phase operation:

- Phase 1: Call Snowflake OAuth endpoint (external, can't rollback)
- Phase 2: Update DataHub connection entity (internal, can fail)

**What if Phase 2 fails?**

- New tokens obtained from Snowflake but not persisted
- Next request sees old expired token → triggers another refresh
- Result: Multiple unnecessary refreshes (inefficient but safe)

**Mitigation:**

- Retry Phase 2 update (3 attempts)
- If all fail, log error but don't block query
- Next request will re-refresh (minor inefficiency acceptable)
- Monitor "token refresh but not saved" metric

**4. Connection Caching Strategy**

**Questions:**

- Should we cache decrypted connections in memory?
- How long to cache?
- Per-user or global cache?

**Options:**

| Strategy                      | Performance                     | Security                     | Memory Usage                    |
| ----------------------------- | ------------------------------- | ---------------------------- | ------------------------------- |
| **No caching**                | Decrypt on every query (slow)   | Most secure                  | Minimal                         |
| **Short-lived cache** (5 min) | Good (decrypt once per 5 min)   | Acceptable                   | Moderate (~1KB per active user) |
| **Session-lived cache**       | Best (decrypt once per session) | Credentials in memory longer | Higher                          |

**Recommendation:** **Short-lived cache with LRU eviction**

```
Cache Configuration:
  - TTL: 5 minutes (balance performance vs security)
  - Max size: 1000 entries (protects memory)
  - Eviction: LRU (least recently used)
  - Encryption: Credentials still encrypted in cache, decrypt on access

Benefits:
  - Reduces decryption overhead (~10ms per query)
  - Limits exposure window (5 minutes)
  - Bounded memory usage
```

**5. Token Refresh Timing Strategy**

**Question:** When should we refresh tokens?

**Options:**

- **On-demand** (refresh when expired): Simple, but adds latency to first query after expiration
- **Proactive** (refresh 5 min before expiry): Complex, requires background job
- **Hybrid** (check on each query, refresh if < 5 min remaining): Best of both

**Recommendation:** **Hybrid approach**

```
On each SQL execution request:
  if (tokenExpiresAt - now < 5 minutes):
    refreshToken()  // Proactive refresh
  else:
    use existing token
```

**Benefits:**

- No latency spike when token expires
- No background jobs needed
- Natural refresh triggered by user activity

**6. Connection Pooling Considerations**

**Question:** Should we pool Snowflake connections per user?

**Trade-offs:**

| Approach                                  | Pros                            | Cons                                         |
| ----------------------------------------- | ------------------------------- | -------------------------------------------- |
| **No pooling** (new connection per query) | Simple, no state                | Slow (connection handshake ~1-2s)            |
| **Per-user pool**                         | Fast queries (reuse connection) | Complex lifecycle management, memory         |
| **Global pool with session context**      | Efficient resource use          | Requires session variables for user identity |

**Recommendation:** **Start without pooling, add if performance demands**

- Initial: Create new connection per query (simple)
- Monitor connection time metrics
- If p95 connection time > 2s AND users complain → implement pooling
- Premature optimization avoided

**Pros:**

- **More secure**: DataHub never sees user's password
- **Better UX**: Automatic token refresh, no manual updates
- **Standard pattern**: OAuth2 is industry standard
- **Revocable**: User can revoke access in Snowflake settings
- **Audit trail**: Snowflake logs show OAuth2 app access

**Cons:**

- More complex to implement
- Requires Snowflake OAuth2 app setup by customer
- Initial OAuth flow adds setup step
- Requires callback URL configuration

**When to use:**

- Production environments (recommended)
- Organizations with OAuth2 infrastructure
- Security-conscious customers
- Large user bases

**Setup Requirements:**

1. Customer creates Snowflake OAuth2 application
2. Customer provides DataHub with:
   - OAuth2 Client ID
   - OAuth2 Client Secret
   - Redirect URI: `https://{datahub-domain}/api/v3/oauth/snowflake/callback`
3. Customer configures DataHub with these values
4. Users can then connect their accounts

---

### Secondary Option: System-Level Connection

**Decision: Optional, opt-in feature with customer-managed access control**

**Important: This feature requires customers to configure Snowflake-side security. DataHub is NOT responsible for data leaks when using system-level connections.**

#### Overview

System-level connection uses a single Snowflake account (e.g., service account) that all DataHub users share. This enables users to query Snowflake without providing individual credentials.

**Use Cases:**

- Organizations want to enable SQL execution for users who don't have Snowflake accounts
- Simplified onboarding (no credential setup per user)
- Read-only analytics use cases
- Data exploration for business users

**Configuration:**

```yaml
# application.yaml
datahub:
  sql:
    snowflake:
      systemConnection:
        enabled: false # DISABLED by default, must opt-in
        connectionUrn: urn:li:dataHubConnection:snowflake-system
        accessControl:
          strategy: SESSION_CONTEXT # Recommended
```

#### Critical Security Considerations

⚠️ **Warning: With system-level connections, ALL DataHub users execute SQL with the same Snowflake identity. Customers MUST configure Snowflake-side access control to prevent unauthorized data access.**

**Customer Responsibilities:**

1. Configure Snowflake Row-Level Security (RLS) policies
2. Set up Snowflake roles with appropriate access
3. Monitor and audit query execution
4. Understand data access implications
5. Accept responsibility for data security

**DataHub provides mechanisms, customers provide security policy.**

---

## Access Control Strategies for System-Level Connections

When using a system-level connection, customers can choose from three strategies to control data access:

### Strategy 1: SESSION_CONTEXT (Recommended)

**How it works:**

- DataHub passes the user's identity to Snowflake via query tags
- Customer configures Snowflake Row-Level Security (RLS) policies
- RLS policies read from query tags to filter data per user

**Implementation:**

```java
// Before executing user's SQL query
String queryTag = String.format(
  "{\"datahub_user\": \"%s\", \"source\": \"chatbot\", \"timestamp\": \"%s\"}",
  userUrn.toString(),
  Instant.now()
);

// Set session context
connection.execute(
  String.format("ALTER SESSION SET QUERY_TAG = '%s'", queryTag)
);

// Now execute user's query
// Snowflake RLS policies can read CURRENT_QUERY_TAG() to filter rows
ResultSet results = connection.execute(userQuery);
```

**Snowflake RLS Policy Example:**

```sql
-- Create a mapping table: DataHub User URN → Allowed Regions
CREATE TABLE datahub_user_access_control (
  datahub_user_urn VARCHAR,
  allowed_region VARCHAR
);

INSERT INTO datahub_user_access_control VALUES
  ('urn:li:corpuser:alice', 'US-WEST'),
  ('urn:li:corpuser:bob', 'US-EAST');

-- Create row access policy that reads from query tag
CREATE OR REPLACE ROW ACCESS POLICY datahub_user_region_policy
  AS (region VARCHAR) RETURNS BOOLEAN ->
    EXISTS (
      SELECT 1 FROM datahub_user_access_control
      WHERE datahub_user_urn = PARSE_JSON(CURRENT_QUERY_TAG()):datahub_user::"VARCHAR"
        AND allowed_region = region
    );

-- Apply policy to sensitive table
ALTER TABLE customers
  ADD ROW ACCESS POLICY datahub_user_region_policy ON (region);
```

**Result:**

- Alice queries `SELECT * FROM customers` → Only sees US-WEST customers
- Bob queries `SELECT * FROM customers` → Only sees US-EAST customers
- Snowflake audit logs show which DataHub user ran each query

**Pros:**

- Fine-grained control (row-level)
- Snowflake enforces security (not DataHub)
- Flexible policies (customer-defined)
- Audit trail in Snowflake
- Single connection, multiple access patterns

**Cons:**

- Requires RLS policy setup (customer effort)
- Policies must be maintained
- Performance impact (RLS evaluation on every row)
- Requires mapping DataHub users to Snowflake access rules

**When to use:**

- Production environments with system connection
- When users need different data access levels
- Compliance requirements (audit trail)
- Organizations with existing RLS infrastructure

---

### Strategy 2: ROLE_MAPPING

**How it works:**

- Map DataHub user groups to Snowflake roles
- Switch to user's mapped role before executing query
- Snowflake's role-based access control (RBAC) enforces permissions

**Implementation:**

```java
// Configuration: DataHub Group → Snowflake Role mapping
Map<String, String> groupToRoleMapping = Map.of(
  "urn:li:corpGroup:analysts", "ANALYST_ROLE",
  "urn:li:corpGroup:engineers", "ENGINEER_ROLE",
  "urn:li:corpGroup:executives", "EXECUTIVE_ROLE"
);

// Get user's groups
Set<Urn> userGroups = opContext.getActorContext().getGroupMembership();

// Find matching Snowflake role
String snowflakeRole = null;
for (Urn group : userGroups) {
  if (groupToRoleMapping.containsKey(group.toString())) {
    snowflakeRole = groupToRoleMapping.get(group.toString());
    break;  // Use first matching role
  }
}

if (snowflakeRole == null) {
  throw new UnauthorizedException(
    "User does not belong to any group with Snowflake access"
  );
}

// Switch role before executing query
connection.execute(String.format("USE ROLE %s", snowflakeRole));

// Execute user's query with this role's permissions
ResultSet results = connection.execute(userQuery);
```

**Configuration:**

```yaml
# application.yaml
datahub:
  sql:
    snowflake:
      systemConnection:
        accessControl:
          strategy: ROLE_MAPPING
          roleMapping:
            - datahubGroup: "urn:li:corpGroup:analysts"
              snowflakeRole: "ANALYST_ROLE"
            - datahubGroup: "urn:li:corpGroup:engineers"
              snowflakeRole: "ENGINEER_ROLE"
```

**Snowflake Role Setup Example:**

```sql
-- Create roles with different access levels
CREATE ROLE ANALYST_ROLE;
CREATE ROLE ENGINEER_ROLE;

-- Analysts can read customer data, not financial
GRANT SELECT ON TABLE customers TO ROLE ANALYST_ROLE;
GRANT SELECT ON TABLE orders TO ROLE ANALYST_ROLE;

-- Engineers can read all tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ROLE ENGINEER_ROLE;

-- Grant roles to the system connection user
GRANT ROLE ANALYST_ROLE TO USER datahub_service_account;
GRANT ROLE ENGINEER_ROLE TO USER datahub_service_account;
```

**Pros:**

- Leverages Snowflake's native RBAC
- Clear role-to-permission mapping
- Good for organization-wide policies
- Easier to understand than RLS
- Better performance than RLS

**Cons:**

- Coarse-grained (role-level, not row-level)
- Requires maintaining group→role mapping
- Users in multiple groups: which role to use?
- Changes require configuration updates
- No per-user customization

**When to use:**

- Organizations with clear role hierarchies
- When data access aligns with user groups
- Simpler access patterns (team-based)
- When RLS is too complex

---

### Strategy 3: DATAHUB_ONLY

**How it works:**

- Check user's DataHub permissions before executing
- No Snowflake-side enforcement
- Relies on DataHub metadata being accurate

**Implementation:**

```java
// Check if user can view the dataset in DataHub
if (datasetUrn != null) {
  boolean authorized = authorizationService.authorize(
    opContext,
    datasetUrn,
    PoliciesConfig.VIEW_DATASET_USAGE_PRIVILEGE
  );

  if (!authorized) {
    throw new UnauthorizedException(
      "User does not have permission to query this dataset"
    );
  }
}

// If authorized, execute query
// No Snowflake-side access control
ResultSet results = connection.execute(userQuery);
```

**Pros:**

- Simple to implement
- No Snowflake configuration needed
- Fast (no RLS overhead)
- Centralized in DataHub

**Cons:**

- ⚠️ **Least secure option**
- Relies on DataHub metadata completeness
- If user queries unknown dataset → no check possible
- Snowflake permissions not respected
- DataHub could be out of sync with reality
- Users might access data not in DataHub metadata

**When to use:**

- **Not recommended for production**
- Development/testing environments only
- When Snowflake contains only non-sensitive data
- All datasets fully ingested into DataHub

---

### Access Control Strategy Comparison

| Strategy            | Security Level      | Setup Complexity | Performance | Use Case               |
| ------------------- | ------------------- | ---------------- | ----------- | ---------------------- |
| **SESSION_CONTEXT** | High (row-level)    | Medium           | Medium      | Production, compliance |
| **ROLE_MAPPING**    | Medium (role-level) | Low              | High        | Clear role hierarchies |
| **DATAHUB_ONLY**    | Low                 | Very Low         | High        | Development only       |

**Recommendation Hierarchy:**

1. **Per-User Credentials** (OAuth2 or direct) - Most secure, recommended
2. **SESSION_CONTEXT** - If system connection needed, use this
3. **ROLE_MAPPING** - Simpler alternative for team-based access
4. **DATAHUB_ONLY** - Development/testing only

---

## API Design

### Decision: REST + SSE (Not GraphQL)

**Chosen: REST API with Server-Sent Events (SSE) for streaming**

#### Why Not GraphQL?

After researching DataHub's GraphQL implementation:

1. **GraphQL is synchronous in DataHub**

   - Documentation states: "GraphQL API operations are synchronous"
   - No WebSocket subscriptions support
   - Standard HTTP timeouts apply (typically 60 seconds)

2. **Long-running queries**

   - SQL queries can take minutes to execute
   - Need progress updates during execution
   - HTTP timeouts would kill the connection

3. **No built-in streaming support**

   - DataHub's GraphQL doesn't support streaming responses
   - Would require implementing custom streaming layer
   - Adds complexity without benefit

4. **Existing SSE pattern works well**
   - Chat system successfully uses REST + SSE
   - Infrastructure already in place
   - Frontend and backend support SSE

#### REST + SSE Pattern (Following Chat System)

**Architecture:**

```
Frontend / Chatbot
    │
    │ POST /api/v3/sql/snowflake/execute
    │ Accept: text/event-stream
    │
    ▼
GMS OpenAPI Controller
    │
    │ Returns SseEmitter
    │
    ├─► Start background thread
    │   │
    │   ├─► Execute SQL query
    │   │
    │   └─► Stream progress events:
    │       ├─► event: progress
    │       │   data: {"status": "validating", "message": "Validating SQL..."}
    │       │
    │       ├─► event: progress
    │       │   data: {"status": "executing", "message": "Running query..."}
    │       │
    │       ├─► event: result
    │       │   data: {"columns": [...], "rows": [...]}
    │       │
    │       └─► event: complete
    │           data: {"executionTimeMs": 2500}
    │
    └─► Return SseEmitter to client
```

**Benefits of SSE:**

- Handles long-running operations (no timeout)
- Progress updates during execution
- Browser native support (EventSource API)
- Simple server implementation (Spring SseEmitter)
- Works through proxies and load balancers
- Graceful error handling

**Alternative Async Pattern Considered:**

Could use async GraphQL with polling:

```graphql
mutation {
  startQueryExecution(input: {...}) {
    executionId
  }
}

query {
  getQueryExecutionStatus(executionId: "...") {
    status
    results
  }
}
```

**Rejected because:**

- More complex (client must poll)
- Higher latency (polling interval)
- More server load (repeated status checks)
- Worse UX (no real-time progress)
- SSE is simpler and better

---

### Approval Mechanism

⚠️ **CRITICAL**: SQL execution requires **system-enforced approval**. We cannot rely on the LLM to consistently ask for user approval.

**📄 Complete Specification:**  
There will be a separate "SQL Execution Approval Mechanism" spec for the detailed design of how user approval is enforced at the system level.

---

### REST API Specification

#### Endpoint: Execute SQL Query

**Request:**

```http
POST /api/v3/sql/snowflake/execute HTTP/1.1
Host: {datahub-gms}
Authorization: Bearer {session-token}
Content-Type: application/json
Accept: text/event-stream

{
  "query": "SELECT customer_id, revenue FROM customers WHERE region = 'US' LIMIT 100",
  "connectionUrn": "urn:li:dataHubConnection:snowflake-query-urn:li:corpuser:alice",
  "datasetUrn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.customers,PROD)",
  "limit": 100,
  "requireApproval": true
}
```

**Request Schema:**

```typescript
interface ExecuteSnowflakeQueryRequest {
  // SQL query to execute (SELECT only)
  query: string;

  // Optional: User's connection URN
  // If not provided, uses system connection (if enabled)
  connectionUrn?: string;

  // Optional: Dataset URN for authorization context
  // Used to check if user has permission to access this dataset
  datasetUrn?: string;

  // Maximum rows to return (default: 100, max: 1000)
  limit?: number;

  // Whether approval was explicitly given
  // Frontend should set this to true after user clicks "Execute"
  requireApproval: boolean;
}
```

**Response (SSE Stream):**

```http
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive

event: progress
data: {"status":"validating","message":"Validating SQL query...","timestamp":1698765432000}

event: progress
data: {"status":"authorizing","message":"Checking permissions...","timestamp":1698765433000}

event: progress
data: {"status":"connecting","message":"Connecting to Snowflake...","timestamp":1698765434000}

event: progress
data: {"status":"executing","message":"Executing query...","timestamp":1698765435000}

event: result
data: {"executionId":"exec_abc123","columns":[{"name":"customer_id","type":"NUMBER"},{"name":"revenue","type":"FLOAT"}],"rows":[["1001","15000.50"],["1002","23500.75"]],"rowCount":2,"totalRows":2,"executionTimeMs":1250}

event: complete
data: {"executionId":"exec_abc123","status":"SUCCESS"}
```

**SSE Event Types:**

1. **progress** - Execution progress update

```json
{
  "status": "validating" | "authorizing" | "connecting" | "executing",
  "message": "Human-readable status message",
  "timestamp": 1698765432000
}
```

2. **result** - Query results (can be sent in chunks for large results)

```json
{
  "executionId": "exec_abc123",
  "columns": [
    { "name": "customer_id", "type": "NUMBER" },
    { "name": "revenue", "type": "FLOAT" }
  ],
  "rows": [
    ["1001", "15000.50"],
    ["1002", "23500.75"]
  ],
  "rowCount": 2,
  "totalRows": 2,
  "executionTimeMs": 1250,
  "hasMore": false
}
```

3. **error** - Execution error

```json
{
  "executionId": "exec_abc123",
  "error": "SQL execution failed",
  "errorType": "SYNTAX_ERROR" | "PERMISSION_DENIED" | "TIMEOUT" | "CONNECTION_ERROR",
  "message": "Detailed error message",
  "timestamp": 1698765436000
}
```

4. **complete** - Execution completed

```json
{
  "executionId": "exec_abc123",
  "status": "SUCCESS" | "FAILED" | "TIMEOUT"
}
```

**Error Responses (Non-streaming):**

```http
HTTP/1.1 400 Bad Request
Content-Type: application/json

{
  "error": "INVALID_SQL",
  "message": "Only SELECT statements are allowed",
  "details": "Query contains forbidden keyword: INSERT"
}
```

**Error Codes:**

- `400 Bad Request` - Invalid request (malformed SQL, forbidden operations)
- `401 Unauthorized` - Missing or invalid authentication
- `403 Forbidden` - User lacks permission to execute query or access dataset
- `404 Not Found` - Connection URN not found
- `429 Too Many Requests` - Rate limit exceeded
- `500 Internal Server Error` - Server-side execution error
- `504 Gateway Timeout` - Query execution timeout

---

## SQL Execution Service

### SnowflakeQueryExecutor Implementation

**Service Architecture:**

```java
package com.linkedin.metadata.sql;

@Slf4j
@Service
public class SnowflakeQueryExecutor {

  private final ConnectionService connectionService;
  private final SnowflakeOAuthRefreshService oauthRefreshService;
  private final SecretService secretService;
  private final AuthorizationService authorizationService;
  private final AuditLogger auditLogger;
  private final ExecutorService executorService;

  // Configuration
  private static final int QUERY_TIMEOUT_SECONDS = 30;
  private static final int MAX_RESULT_ROWS = 1000;
  private static final Pattern SELECT_PATTERN =
    Pattern.compile("^\\s*SELECT\\s+", Pattern.CASE_INSENSITIVE);

  @Value("${datahub.sql.snowflake.queryTimeout:30}")
  private int queryTimeoutSeconds;

  @Value("${datahub.sql.snowflake.maxResultRows:1000}")
  private int maxResultRows;

  /**
   * Execute a SQL query on Snowflake with streaming progress updates.
   *
   * @param opContext Operation context with user authentication
   * @param request Query execution request
   * @param emitter SSE emitter for streaming progress
   * @return CompletableFuture that completes when execution finishes
   */
  public CompletableFuture<Void> executeQuery(
      OperationContext opContext,
      ExecuteSnowflakeQueryRequest request,
      SseEmitter emitter
  ) {
    String executionId = UUID.randomUUID().toString();

    return CompletableFuture.runAsync(() -> {
      try {
        // 1. Validate SQL
        sendProgress(emitter, "validating", "Validating SQL query...");
        validateSQL(request.getQuery());

        // 2. Authorize user
        sendProgress(emitter, "authorizing", "Checking permissions...");
        authorizeExecution(opContext, request);

        // 3. Get connection
        sendProgress(emitter, "connecting", "Connecting to Snowflake...");
        SnowflakeConnection connection = getConnection(
          opContext,
          request.getConnectionUrn()
        );

        // 4. Refresh OAuth tokens if needed
        connection = oauthRefreshService.refreshTokenIfNeeded(
          connection,
          opContext.getActorContext().getActorUrn(),
          opContext
        );

        // 5. Execute query
        sendProgress(emitter, "executing", "Executing query...");
        QueryResult result = executeWithTimeout(
          connection,
          request.getQuery(),
          Math.min(request.getLimit(), maxResultRows),
          queryTimeoutSeconds
        );

        // 6. Send results
        sendResult(emitter, executionId, result);

        // 7. Audit log
        auditLogger.log(buildAuditEvent(
          opContext,
          request,
          result,
          executionId,
          "SUCCESS"
        ));

        // 8. Complete
        sendComplete(emitter, executionId, "SUCCESS");
        emitter.complete();

      } catch (Exception e) {
        handleError(emitter, executionId, e, opContext, request);
      }
    }, executorService);
  }

  /**
   * Validate that SQL query is safe to execute.
   * Only SELECT statements allowed.
   */
  private void validateSQL(String query) {
    if (query == null || query.trim().isEmpty()) {
      throw new IllegalArgumentException("Query cannot be empty");
    }

    String normalizedQuery = query.trim();

    // Must start with SELECT
    if (!SELECT_PATTERN.matcher(normalizedQuery).find()) {
      throw new InvalidSQLException(
        "Only SELECT statements are allowed",
        "INVALID_SQL"
      );
    }

    // Block forbidden keywords (DDL/DML)
    List<String> forbiddenKeywords = Arrays.asList(
      "INSERT", "UPDATE", "DELETE", "MERGE",
      "CREATE", "ALTER", "DROP", "TRUNCATE",
      "GRANT", "REVOKE"
    );

    String upperQuery = normalizedQuery.toUpperCase();
    for (String keyword : forbiddenKeywords) {
      if (upperQuery.contains(keyword)) {
        throw new InvalidSQLException(
          String.format("Query contains forbidden keyword: %s", keyword),
          "FORBIDDEN_KEYWORD"
        );
      }
    }

    // Additional validations
    // - Check for SQL injection patterns
    // - Validate query length
    // - Check for nested queries depth
    // ...
  }

  /**
   * Authorize user to execute query.
   */
  private void authorizeExecution(
      OperationContext opContext,
      ExecuteSnowflakeQueryRequest request
  ) {
    // If dataset URN provided, check user can view it
    if (request.getDatasetUrn() != null) {
      boolean authorized = authorizationService.authorize(
        opContext,
        UrnUtils.getUrn(request.getDatasetUrn()),
        PoliciesConfig.VIEW_DATASET_USAGE_PRIVILEGE
      );

      if (!authorized) {
        throw new UnauthorizedException(
          "User does not have permission to query this dataset"
        );
      }
    }

    // If connection URN provided, verify user owns it
    if (request.getConnectionUrn() != null) {
      verifyConnectionOwnership(
        opContext.getActorContext().getActorUrn(),
        UrnUtils.getUrn(request.getConnectionUrn())
      );
    }
  }

  /**
   * Get Snowflake connection (user or system).
   */
  private SnowflakeConnection getConnection(
      OperationContext opContext,
      String connectionUrnString
  ) {
    Urn connectionUrn;

    if (connectionUrnString != null) {
      // Use specified connection
      connectionUrn = UrnUtils.getUrn(connectionUrnString);
    } else {
      // Use system connection (if enabled)
      if (!config.isSystemConnectionEnabled()) {
        throw new IllegalArgumentException(
          "No connection specified and system connection is disabled"
        );
      }
      connectionUrn = config.getSystemConnectionUrn();
    }

    // Retrieve connection entity
    EntityResponse response = connectionService.getConnectionEntityResponse(
      opContext,
      connectionUrn
    );

    if (response == null) {
      throw new NotFoundException("Connection not found: " + connectionUrn);
    }

    // Parse connection details
    DataHubConnectionDetails details =
      parseConnectionDetails(response);

    return SnowflakeConnection.fromDetails(details, secretService);
  }

  /**
   * Execute query with timeout.
   */
  private QueryResult executeWithTimeout(
      SnowflakeConnection connection,
      String query,
      int limit,
      int timeoutSeconds
  ) throws SQLException, TimeoutException {

    long startTime = System.currentTimeMillis();

    // Create native Snowflake connection
    try (Connection nativeConn = connection.createNativeConnection()) {

      // Set query timeout
      try (Statement stmt = nativeConn.createStatement()) {
        stmt.setQueryTimeout(timeoutSeconds);
        stmt.setMaxRows(limit);

        // Execute query
        try (ResultSet rs = stmt.executeQuery(query)) {

          // Extract column metadata
          ResultSetMetaData metadata = rs.getMetaData();
          int columnCount = metadata.getColumnCount();

          List<QueryColumn> columns = new ArrayList<>();
          for (int i = 1; i <= columnCount; i++) {
            columns.add(new QueryColumn(
              metadata.getColumnName(i),
              metadata.getColumnTypeName(i)
            ));
          }

          // Extract rows
          List<List<String>> rows = new ArrayList<>();
          int rowCount = 0;

          while (rs.next() && rowCount < limit) {
            List<String> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
              String value = rs.getString(i);
              row.add(value != null ? value : "NULL");
            }
            rows.add(row);
            rowCount++;
          }

          long executionTime = System.currentTimeMillis() - startTime;

          return QueryResult.builder()
            .columns(columns)
            .rows(rows)
            .rowCount(rowCount)
            .totalRows(rowCount)  // TODO: Get actual total if different
            .executionTimeMs(executionTime)
            .hasMore(rs.next())  // Check if there are more rows
            .build();
        }
      }
    }
  }

  // SSE helper methods
  private void sendProgress(SseEmitter emitter, String status, String message) {
    try {
      emitter.send(SseEmitter.event()
        .name("progress")
        .data(Map.of(
          "status", status,
          "message", message,
          "timestamp", System.currentTimeMillis()
        )));
    } catch (IOException e) {
      log.error("Failed to send progress event", e);
      emitter.completeWithError(e);
    }
  }

  private void sendResult(SseEmitter emitter, String executionId, QueryResult result) {
    try {
      emitter.send(SseEmitter.event()
        .name("result")
        .data(Map.of(
          "executionId", executionId,
          "columns", result.getColumns(),
          "rows", result.getRows(),
          "rowCount", result.getRowCount(),
          "totalRows", result.getTotalRows(),
          "executionTimeMs", result.getExecutionTimeMs(),
          "hasMore", result.isHasMore()
        )));
    } catch (IOException e) {
      log.error("Failed to send result event", e);
      emitter.completeWithError(e);
    }
  }

  private void sendComplete(SseEmitter emitter, String executionId, String status) {
    try {
      emitter.send(SseEmitter.event()
        .name("complete")
        .data(Map.of(
          "executionId", executionId,
          "status", status
        )));
    } catch (IOException e) {
      log.error("Failed to send complete event", e);
    }
  }

  private void handleError(
      SseEmitter emitter,
      String executionId,
      Exception e,
      OperationContext opContext,
      ExecuteSnowflakeQueryRequest request
  ) {
    log.error("SQL execution failed", e);

    String errorType = determineErrorType(e);
    String message = e.getMessage();

    try {
      emitter.send(SseEmitter.event()
        .name("error")
        .data(Map.of(
          "executionId", executionId,
          "error", "SQL execution failed",
          "errorType", errorType,
          "message", message,
          "timestamp", System.currentTimeMillis()
        )));

      emitter.send(SseEmitter.event()
        .name("complete")
        .data(Map.of(
          "executionId", executionId,
          "status", "FAILED"
        )));
    } catch (IOException ioEx) {
      log.error("Failed to send error event", ioEx);
    }

    emitter.completeWithError(e);

    // Audit log the failure
    auditLogger.log(buildAuditEvent(
      opContext,
      request,
      null,
      executionId,
      "FAILED"
    ));
  }
}
```

---

## Security Considerations

### SQL Validation

**Approach: Multi-layered validation**

1. **Syntactic Validation**

   - Must start with `SELECT`
   - Regex check for forbidden keywords
   - Character whitelist (prevent injection)

2. **Semantic Validation** (Optional enhancement)

   - Parse SQL with proper parser (e.g., JSQLParser)
   - Verify no subqueries modify data
   - Check for suspicious patterns

3. **Runtime Limits**
   - Query timeout: 30 seconds default
   - Result row limit: 1000 max
   - Query length limit: 10KB
   - Connection timeout: 10 seconds

**Supported Operations (Decision Required):**

⚠️ **Important**: Initial spec assumes SELECT-only, but mutators (INSERT, UPDATE, DELETE, MERGE) are under consideration.

**Three Options:**

| Option                      | Allowed Operations                    | Use Cases                              | Risk Level | Approval Requirements            |
| --------------------------- | ------------------------------------- | -------------------------------------- | ---------- | -------------------------------- |
| **Option A: READ-ONLY**     | SELECT only                           | Data exploration, analytics, reporting | Low        | Standard approval flow           |
| **Option B: LIMITED WRITE** | SELECT, INSERT                        | Data entry, ETL, backfills             | Medium     | Enhanced approval + confirmation |
| **Option C: FULL DML**      | SELECT, INSERT, UPDATE, DELETE, MERGE | Complete data manipulation             | High       | Strict approval + dry-run        |

**Recommendation:** Start with **Option A (READ-ONLY)**, add Option B in Phase 2 based on user demand.

**Always Blocked (All Options):**

- DDL operations: `CREATE`, `ALTER`, `DROP`, `TRUNCATE`
- Permission changes: `GRANT`, `REVOKE`
- Stored procedures: `CALL`, `EXECUTE`
- System operations: `COPY`, `SET` (session variables)

**Additional Safety Measures if Mutators Supported:**

1. **Dry-Run Capability**

   ```sql
   -- Preview what would be affected:
   EXPLAIN UPDATE customers SET region='US' WHERE country='USA';
   -- Shows: 5,234 rows would be affected
   ```

2. **Row Limits for Mutators**

   - INSERT: Max 1,000 rows per statement
   - UPDATE/DELETE: Max 1,000 rows per statement
   - Can be configured per deployment
   - Error if exceeds: "This would affect 5,234 rows, max is 1,000"

3. **Enhanced Audit Logging**

   ```json
   {
     "event": "EXECUTE_SQL_MUTATOR",
     "queryType": "UPDATE",
     "affectedRows": 42,
     "affectedTables": ["prod.sales.customers"],
     "approvalCorrelationId": "corr_abc123",
     "canRollback": false
   }
   ```

4. **Transaction Support** (Future Enhancement)
   - BEGIN/COMMIT/ROLLBACK support
   - Auto-rollback on error
   - User confirmation before COMMIT

### Credential Security

**Encryption:**

- All credentials encrypted with `SecretService` (AES-256-GCM)
- Encryption key from environment: `SECRET_SERVICE_ENCRYPTION_KEY`
- Tokens stored encrypted in connection entity
- Credentials never logged or exposed in errors

**Access Control:**

- User can only use their own connections
- System connection requires explicit privilege
- Connection ownership verified before use

**Token Lifecycle:**

- Access tokens cached in connection entity
- Refresh tokens never exposed to client
- Token expiration checked before each use
- Failed refresh invalidates connection

### Audit Logging

**All SQL executions logged with:**

```json
{
  "event": "EXECUTE_SQL",
  "actor": "urn:li:corpuser:alice",
  "resource": "urn:li:dataset:(...)",
  "action": "QUERY",
  "timestamp": "2025-10-31T12:34:56Z",
  "metadata": {
    "executionId": "exec_abc123",
    "query": "SELECT customer_id, revenue FROM customers LIMIT 100",
    "connectionUrn": "urn:li:dataHubConnection:snowflake-query-urn:li:corpuser:alice",
    "datasetUrn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.customers,PROD)",
    "rowCount": 42,
    "executionTimeMs": 1250,
    "status": "SUCCESS",
    "clientIp": "10.0.1.5",
    "userAgent": "Mozilla/5.0..."
  }
}
```

**Audit log used for:**

- Security monitoring
- Compliance reporting
- Usage analytics
- Debugging
- Cost attribution

### Rate Limiting

**Per-user limits:**

- 100 queries per hour
- 10 concurrent queries
- Enforced at GMS level
- Returns HTTP 429 when exceeded

**Implementation:**

```java
@Component
public class QueryRateLimiter {

  private final Cache<String, AtomicInteger> hourlyQueries =
    Caffeine.newBuilder()
      .expireAfterWrite(1, TimeUnit.HOURS)
      .build();

  private final Cache<String, AtomicInteger> concurrentQueries =
    Caffeine.newBuilder()
      .build();

  public void checkRateLimit(Urn userUrn) {
    String userId = userUrn.toString();

    // Check hourly limit
    AtomicInteger hourlyCount = hourlyQueries.get(userId,
      k -> new AtomicInteger(0));
    if (hourlyCount.incrementAndGet() > 100) {
      throw new RateLimitExceededException(
        "Hourly query limit exceeded (100 queries/hour)"
      );
    }

    // Check concurrent limit
    AtomicInteger concurrentCount = concurrentQueries.get(userId,
      k -> new AtomicInteger(0));
    if (concurrentCount.get() >= 10) {
      throw new RateLimitExceededException(
        "Concurrent query limit exceeded (10 concurrent queries)"
      );
    }
  }

  public void acquireConcurrentSlot(Urn userUrn) {
    concurrentQueries.get(userUrn.toString(),
      k -> new AtomicInteger(0)).incrementAndGet();
  }

  public void releaseConcurrentSlot(Urn userUrn) {
    AtomicInteger count = concurrentQueries.getIfPresent(userUrn.toString());
    if (count != null) {
      count.decrementAndGet();
    }
  }
}
```

---

## Chatbot Integration

### MCP Tool Implementation

**New tool for SQL execution:**

```python
# datahub-integrations-service/src/datahub_integrations/mcp/tools/execute_sql.py

from typing import Optional, Dict, Any
import httpx
from loguru import logger

from datahub_integrations.mcp.mcp_server import mcp, get_datahub_client


@mcp.tool()
def execute_snowflake_query(
    query: str,
    dataset_urn: Optional[str] = None,
    limit: int = 100
) -> Dict[str, Any]:
    """
    Execute a SQL query on Snowflake using the current user's credentials.

    This tool executes SELECT queries only. The query must be approved by the
    user before execution. Results are limited to preserve performance.

    Args:
        query: SQL SELECT statement to execute. Must be SELECT only, no DDL/DML.
        dataset_urn: Optional dataset URN for authorization context. If provided,
                    verifies user has permission to query this dataset.
        limit: Maximum number of rows to return (default: 100, max: 1000).

    Returns:
        Dictionary containing:
        - columns: List of column names and types
        - rows: List of row data
        - rowCount: Number of rows returned
        - executionTimeMs: Query execution time
        - error: Error message if execution failed

    Examples:
        >>> execute_snowflake_query(
        ...     query="SELECT customer_id, revenue FROM customers LIMIT 10",
        ...     dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.customers,PROD)"
        ... )
        {
            "columns": [
                {"name": "customer_id", "type": "NUMBER"},
                {"name": "revenue", "type": "FLOAT"}
            ],
            "rows": [
                ["1001", "15000.50"],
                ["1002", "23500.75"]
            ],
            "rowCount": 2,
            "executionTimeMs": 850
        }
    """
    client = get_datahub_client()

    # Get user's connection URN
    # Format: urn:li:dataHubConnection:snowflake-query-{userUrn}
    user_urn = client._graph.get_current_user()
    connection_urn = f"urn:li:dataHubConnection:snowflake-query-{user_urn}"

    # Build request
    request = {
        "query": query,
        "connectionUrn": connection_urn,
        "datasetUrn": dataset_urn,
        "limit": min(limit, 1000),  # Cap at 1000
        "requireApproval": True
    }

    try:
        # Call GMS API
        # Note: This is a simplified version. In practice, we'd need to:
        # 1. Stream SSE events
        # 2. Show progress to user
        # 3. Handle errors gracefully

        base_url = client._graph.server
        url = f"{base_url}/api/v3/sql/snowflake/execute"

        # For now, use simplified non-streaming call
        # TODO: Implement SSE streaming for progress updates
        response = httpx.post(
            url,
            json=request,
            headers={
                "Authorization": f"Bearer {client._graph.get_session_token()}",
                "Content-Type": "application/json"
            },
            timeout=60.0  # 60 second timeout
        )

        response.raise_for_status()
        result = response.json()

        logger.info(
            f"Successfully executed SQL query, returned {result.get('rowCount', 0)} rows"
        )

        return result

    except httpx.HTTPStatusError as e:
        error_detail = e.response.json() if e.response else {}
        error_msg = error_detail.get("message", str(e))

        logger.error(f"SQL execution failed: {error_msg}")

        return {
            "error": error_msg,
            "errorType": error_detail.get("error", "EXECUTION_ERROR"),
            "columns": [],
            "rows": [],
            "rowCount": 0
        }

    except Exception as e:
        logger.exception("Unexpected error executing SQL")
        return {
            "error": str(e),
            "errorType": "UNEXPECTED_ERROR",
            "columns": [],
            "rows": [],
            "rowCount": 0
        }


@mcp.tool()
def check_snowflake_connection() -> Dict[str, Any]:
    """
    Check if the current user has a Snowflake connection configured.

    Returns:
        Dictionary containing:
        - hasConnection: Whether user has a connection
        - connectionUrn: The connection URN (if exists)
        - authenticationType: Type of authentication (if exists)
    """
    client = get_datahub_client()

    user_urn = client._graph.get_current_user()
    connection_urn = f"urn:li:dataHubConnection:snowflake-query-{user_urn}"

    # Check if connection exists
    query = """
    query GetConnection($urn: String!) {
      connection(urn: $urn) {
        urn
        details {
          json {
            blob
          }
        }
      }
    }
    """

    try:
        result = client._graph.execute_graphql(
            query=query,
            variables={"urn": connection_urn}
        )

        connection = result.get("connection")

        if not connection:
            return {
                "hasConnection": False,
                "message": "No Snowflake connection configured. Please set up your connection in Settings."
            }

        # Parse connection details to get auth type
        import json
        blob = connection["details"]["json"]["blob"]
        details = json.loads(blob)

        return {
            "hasConnection": True,
            "connectionUrn": connection_urn,
            "authenticationType": details.get("authentication_type", "UNKNOWN")
        }

    except Exception as e:
        logger.exception("Failed to check connection")
        return {
            "hasConnection": False,
            "error": str(e)
        }
```

### Chat Flow with SQL Execution

**Typical conversation flow:**

````
User: "Show me the top 10 customers by revenue"

Bot: [Calls search tool to find customers table]
     [Generates SQL based on schema]

     I can query the customers table for you. Here's the SQL I'll run:

     ```sql
     SELECT customer_id, customer_name, total_revenue
     FROM prod.sales.customers
     ORDER BY total_revenue DESC
     LIMIT 10
     ```

     Would you like me to execute this query?
     [Execute Button]

User: [Clicks Execute]

Bot: [Calls execute_snowflake_query tool]
     [Streams progress updates]

     Executing query... ✓

     Here are the top 10 customers:

     | Customer ID | Customer Name | Revenue |
     |-------------|---------------|---------|
     | 1001 | Acme Corp | $250,000 |
     | 1002 | Globex Inc | $220,000 |
     ...

     (10 rows, executed in 1.2s)
````

**Error handling:**

```
User: "Execute the query"

Bot: [Calls execute_snowflake_query]
     [Tool returns error: no connection]

     I can't execute this query because you haven't connected your
     Snowflake account yet.

     To set up your connection:
     1. Go to Settings → Connections → Snowflake
     2. Enter your Snowflake credentials
     3. Click "Connect"

     Would you like me to generate the SQL for now so you can run
     it manually?
```

---

## Implementation Components

### Remote Executor Components (Customer VPC)

**New Task Handler:**

- `datahub-executor/src/datahub_executor/worker/sql_query_task.py`
  - Handles `RUN_SQL_QUERY` task type
  - Gets connection from secrets (AWS Secrets Manager, etc.)
  - Executes SQL on Snowflake
  - Returns results to coordinator

**Existing Reusable Components:**

- `datahub-executor/src/datahub_executor/common/source/snowflake/snowflake.py` - Snowflake connection and execution
- `datahub-executor/src/datahub_executor/common/connection/snowflake/snowflake_connection.py` - Connection management

### Coordinator Components (GMS)

**New API Layer:**

- `metadata-service/openapi-servlet/.../SqlExecutionController.java` - REST endpoints for preview and execute
- `metadata-io/src/main/java/com/linkedin/metadata/sql/SqlExecutionCoordinator.java` - Creates ExecutionRequests and dispatches to executors

**Approval & Security:**

- See [SQL Execution Approval Mechanism](sql-execution-approval-mechanism.md) for approval token implementation

**Existing Reusable:**

- `metadata-io/src/main/java/com/linkedin/metadata/connection/ConnectionService.java` - Connection entity management
- `datahub-executor/src/datahub_executor/coordinator/scheduler.py` - ExecutionRequest dispatch pattern

### Integrations Service Components

**New MCP Tools:**

- `datahub-integrations-service/src/datahub_integrations/mcp/tools/sql_execution.py`
  - See [Approval Mechanism spec](sql-execution-approval-mechanism.md) for tool design

### Deployment Modes

**Mode 1: Embedded Executor (Initial/Development)**

- Executes in coordinator process
- No remote deployment needed
- Credentials in GMS environment
- Good for: Development, testing

**Mode 2: Remote Executor (Production)**

- Executes in customer VPC
- Credentials in customer environment
- Full security benefits
- Good for: Production, compliance-sensitive deployments

---

**Document Version History:**

| Version | Date       | Author       | Changes               |
| ------- | ---------- | ------------ | --------------------- |
| 1.0     | 2025-10-31 | AI Assistant | Initial specification |

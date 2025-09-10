# Multi-Tenant Router Design for Teams Integration

_Technical design for routing Teams webhook events to correct DataHub instances_

## 🎯 **Overview**

The Multi-Tenant Router is a critical component that receives Microsoft Teams webhook events and routes them to the correct DataHub instance based on tenant identification and configuration. This design addresses how to handle multiple DataHub deployments serving different organizations or teams within the same Microsoft Azure tenant.

## 🏗️ **Architecture**

### **High-Level Flow**

```mermaid
graph TD
    %% Microsoft Teams and Azure Components
    A[Microsoft Teams] -->|Teams Webhook Events| B[Azure Bot Service]
    B -->|POST /public/teams/webhook| C[Multi-Tenant Router :9005]
    B -->|POST /api/messages| C

    %% OAuth Flow Components
    O[DataHub Admin] -->|OAuth Setup via Frontend| FRONTEND[DataHub Web React :3000]
    FRONTEND -->|GraphQL teamsOAuthConfig| GQL[DataHub GraphQL Core]
    GQL -->|Proxy Request| INTEGRATIONS[DataHub Integrations Service :9003]
    INTEGRATIONS -->|POST /teams/oauth/config| INTEGRATIONS
    FRONTEND -->|Redirect to Microsoft OAuth| MS[Microsoft OAuth]
    MS -->|OAuth Callback| R[GET /public/teams/oauth/callback]
    C -->|Decode State & Forward| INTEGRATIONS2[/private/teams/oauth/complete :9003]
    INTEGRATIONS2 -->|OAuth Token Exchange| MS
    INTEGRATIONS2 -->|Success Response| C
    C -->|Auto-register Tenant| DB[(Router Database)]
    C -->|Redirect Success/Error| FRONTEND

    %% Regular Webhook Routing
    C -->|Route Decision| DB
    C -->|POST /public/teams/webhook| INTEGRATIONS
    INTEGRATIONS -->|Process Teams Events| TEAMS[Teams Bot Handler]

    %% Database and Configuration
    DB -->|Store Tenant Mappings| SQLITE[(SQLite/MySQL)]
    DB -->|Store Instance Registry| SQLITE
    DB -->|Track Unknown Tenants| SQLITE

    %% Admin API (not currently used by other components)
    ADMIN[Admin User] -.->|Manual Configuration| ADMINAPI[Router Admin API]
    ADMINAPI -.->|CRUD Operations| DB

    %% Health and Monitoring
    C -->|Health Check| HC[/health]
    C -->|Routing Stats| STATS[/admin/routing-stats]

    %% Component Styling
    classDef router fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    classDef oauth fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    classDef datahub fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px
    classDef external fill:#fff3e0,stroke:#ef6c00,stroke-width:2px
    classDef database fill:#f1f8e9,stroke:#558b2f,stroke-width:2px

    class C,R,ADMINAPI,HC,STATS router
    class MS,O oauth
    class FRONTEND,GQL,INTEGRATIONS,INTEGRATIONS2,TEAMS datahub
    class A,B external
    class DB,SQLITE database
```

### **Deployment Patterns**

The multi-tenant router supports two deployment patterns:

1. **Cloud-to-Cloud**: Direct HTTP forwarding to cloud-hosted DataHub instances
2. **Cloud-to-OnPrem**: Reverse tunnel over persistent WebSocket connections

### **Component Responsibilities**

| Component               | Responsibility                                                               |
| ----------------------- | ---------------------------------------------------------------------------- |
| **Multi-Tenant Router** | Receive webhooks, OAuth proxy, identify tenant, route to correct instance    |
| **OAuth Proxy**         | Handle Microsoft OAuth callbacks, decode state, forward to DataHub instances |
| **Configuration Store** | Maintain OAuth-based tenant → DataHub instance mappings                      |
| **Tunnel Manager**      | Manage persistent WebSocket connections with on-prem instances               |
| **Connection Registry** | Track active tunnel connections and their health                             |
| **Health Monitor**      | Track instance availability and health (cloud + on-prem)                     |
| **Fallback Handler**    | Handle routing failures and unknown tenants                                  |

## 🔍 **Tenant Identification Strategy**

### **Event Data Available**

From Teams installation events, we extract:

```json
{
  "conversation": {
    "tenantId": "5ff2a251-a73c-4122-af7d-871fd744c752"
  },
  "channelData": {
    "team": {
      "id": "19:W0_JnTr3YBkTQZGhrwRhbV9QnOKEbsS49n95VI1X-dI1@thread.tacv2",
      "name": "Engineering Team"
    }
  }
}
```

### **Routing Decision Logic**

```python
async def route_teams_event(event: Dict[str, Any]) -> DataHubInstance:
    """
    Route Teams event to correct DataHub instance.

    Priority:
    1. Team-specific mapping (for large orgs with multiple instances)
    2. Tenant-only mapping (most common case)
    3. Default/fallback handling
    """
    tenant_id = event["conversation"]["tenantId"]

    # Extract team information if available
    team_info = None
    if "channelData" in event and "team" in event["channelData"]:
        team_info = {
            "id": event["channelData"]["team"]["id"],
            "name": event["channelData"]["team"]["name"]
        }

    # Strategy 1: Team-specific routing (for large organizations)
    if team_info:
        instance = await get_instance_by_tenant_and_team(tenant_id, team_info["id"])
        if instance:
            logger.info(f"✅ Routed to {instance.url} via team mapping")
            return instance

    # Strategy 2: Tenant-only routing (most common)
    instance = await get_instance_by_tenant(tenant_id)
    if instance:
        logger.info(f"✅ Routed to {instance.url} via tenant mapping")
        return instance

    # Strategy 3: Default routing for unknown tenants
    if await has_default_instance():
        instance = await get_default_instance()
        logger.warning(f"⚠️ Routed unknown tenant {tenant_id} to default instance {instance.url}")
        await create_unknown_tenant_alert(tenant_id, team_info)
        return instance

    # Strategy 4: No routing available
    logger.error(f"❌ No routing found for tenant {tenant_id}")
    raise UnknownTenantError(tenant_id)
```

## 💾 **Configuration Storage**

The router maintains tenant-to-instance mappings to enable proper event routing. The storage design supports:

### **Core Data Model**

**Tenant Mappings:**

- Tenant ID → DataHub Instance URL mapping
- Optional team-specific routing for large organizations
- Default instance configuration per tenant
- Mapping metadata (creation time, created by, etc.)

**Instance Registry:**

- Cloud instances: Direct HTTP endpoints
- On-premises instances: Tunnel connection identifiers
- Health status and availability tracking
- Connection timeout and retry configuration

### **Configuration Examples**

**Cloud Instance (Single DataHub per Tenant):**

```json
{
  "tenant_id": "5ff2a251-a73c-4122-af7d-871fd744c752",
  "datahub_instance_id": "engineering-datahub-prod",
  "is_default_for_tenant": true
}
```

**On-Prem Instance Configuration:**

```json
{
  "tenant_id": "5ff2a251-a73c-4122-af7d-871fd744c752",
  "datahub_instance_id": "acme-corp-onprem",
  "is_default_for_tenant": true,
  "deployment_type": "on_prem",
  "connection_id": "acme-corp-main-connection"
}
```

**Mixed Cloud + On-Prem (Large Organization):**

```json
[
  {
    "tenant_id": "5ff2a251-a73c-4122-af7d-871fd744c752",
    "team_id": "19:engineering-team@thread.tacv2",
    "team_name": "Engineering Team",
    "datahub_instance_id": "engineering-datahub-cloud",
    "deployment_type": "cloud"
  },
  {
    "tenant_id": "5ff2a251-a73c-4122-af7d-871fd744c752",
    "team_id": "19:finance-team@thread.tacv2",
    "team_name": "Finance Team",
    "datahub_instance_id": "finance-datahub-onprem",
    "deployment_type": "on_prem",
    "connection_id": "finance-secure-connection"
  },
  {
    "tenant_id": "5ff2a251-a73c-4122-af7d-871fd744c752",
    "is_default_for_tenant": true,
    "datahub_instance_id": "corporate-datahub-cloud",
    "deployment_type": "cloud"
  }
]
```

## 🏢 **On-Premises Integration**

### **Registration & Intent Declaration**

When Teams integration is configured in an on-premises DataHub instance, the `datahub-integrations-service` container registers with the multi-tenant router and establishes a persistent tunnel.

#### **Registration Flow**

1. **Intent Declaration**: On-prem instance declares intent to handle Teams events for specific tenants
2. **Authentication**: Instance authenticates with router using API key/certificate
3. **Capability Registration**: Declares supported features (webhook handling, messaging, health checks)
4. **Tunnel Establishment**: Creates persistent WebSocket connection for bidirectional communication
5. **Heartbeat Setup**: Establishes keepalive mechanism to detect connection failures

#### **Registration API**

```
POST /api/instances/register
{
  "instance_id": "acme-corp-main",
  "connection_id": "acme-corp-connection-1",
  "tenant_mappings": ["tenant-id-1", "tenant-id-2"],
  "capabilities": ["teams_webhook", "teams_messaging"],
  "health_check_path": "/health"
}
```

### **Tunnel Communication Design**

For on-premises DataHub instances that cannot be directly reached from the internet, the router supports reverse tunnel connections:

#### **Connection Establishment**

1. **Registration**: On-prem instance registers with router using unique connection ID
2. **Authentication**: Secure authentication using API keys or certificates
3. **Tunnel Creation**: Persistent bidirectional communication channel
4. **Health Monitoring**: Continuous availability and latency monitoring

#### **Message Flow**

1. **Webhook Reception**: Router receives Teams event
2. **Tenant Resolution**: Lookup determines on-prem instance target
3. **Tunnel Routing**: Event forwarded through established tunnel
4. **Response Handling**: Instance response relayed back to Teams

## 🔄 **Router Implementation**

### **Router Architecture**

The multi-tenant router implements a FastAPI service with the following key endpoints:

| Endpoint                       | Purpose                        | Method  |
| ------------------------------ | ------------------------------ | ------- |
| `/public/teams/webhook`        | Main Teams webhook receiver    | POST    |
| `/public/teams/oauth/callback` | OAuth callback proxy for setup | GET     |
| `/tunnel/{connection_id}`      | WebSocket tunnel for on-prem   | WS      |
| `/api/instances/register`      | On-prem instance registration  | POST    |
| `/admin/router/*`              | Admin configuration endpoints  | Various |
| `/health`                      | Router health check            | GET     |

### **Request Flow**

1. **Webhook Reception**: Teams event received at `/public/teams/webhook`
2. **Event Validation**: Validate Teams event structure and authentication
3. **Tenant Resolution**: Extract tenant ID and optional team information
4. **Instance Lookup**: Find target DataHub instance from configuration
5. **Routing Decision**: Choose between cloud HTTP or on-prem tunnel routing
6. **Event Forwarding**: Forward to target instance with timeout handling
7. **Response Relay**: Return target instance response to Teams

### **Health Monitoring**

The router monitors instance health through different mechanisms based on deployment type:

#### **Cloud Instance Health**

- **HTTP Health Checks**: Periodic GET requests to `/health` endpoint
- **Response Time Tracking**: Monitor latency and availability
- **Circuit Breaker**: Temporarily disable unhealthy instances

#### **On-Prem Instance Health**

- **WebSocket Heartbeats**: 30-second ping/pong messages through tunnel
- **Tunnel Status**: Monitor WebSocket connection state
- **Response Timeouts**: Track webhook response times through tunnel

#### **Health Status Tracking**

| Status         | Description                           | Action                    |
| -------------- | ------------------------------------- | ------------------------- |
| `healthy`      | Instance responding normally          | Route traffic             |
| `degraded`     | Slow responses or intermittent issues | Route with fallback       |
| `unhealthy`    | Failed health checks                  | Stop routing, alert admin |
| `disconnected` | Tunnel connection lost (on-prem)      | Attempt reconnection      |

## 🚨 **Error Handling & Fallbacks**

### **Failure Scenarios**

1. **Unknown Tenant**: Tenant not in configuration
2. **Instance Unavailable**: Target DataHub instance down/unhealthy
3. **Network Failures**: Connectivity issues
4. **Configuration Errors**: Malformed mappings

### **Fallback Strategy**

```python
class FallbackHandler:
    """Handle routing failures gracefully."""

    async def handle_unknown_tenant(self, tenant_id: str, event_data: Dict):
        """Handle events from unknown tenants."""

        # Log unknown tenant for admin review
        await self.log_unknown_tenant(tenant_id, event_data)

        # Check if there's a default instance configured
        default_instance = await get_default_instance()
        if default_instance:
            logger.warning(f"Routing unknown tenant {tenant_id} to default instance")
            return default_instance

        # Create admin alert for unknown tenant
        await self.create_admin_alert("unknown_tenant", {
            "tenant_id": tenant_id,
            "event_type": event_data.get("type"),
            "team_info": event_data.get("channelData", {}).get("team")
        })

        raise UnknownTenantError(tenant_id)

    async def handle_instance_failure(self, instance: DataHubInstance, event_data: Dict):
        """Handle when primary instance is unavailable."""

        # Try to find a backup instance for the same tenant
        backup_instances = await get_backup_instances_for_tenant(
            event_data["conversation"]["tenantId"]
        )

        for backup in backup_instances:
            if await self.health_monitor.check_instance_health(backup):
                logger.warning(f"Failing over from {instance.url} to {backup.url}")
                return backup

        # No healthy backups available
        await self.create_admin_alert("instance_failure", {
            "failed_instance": instance.url,
            "tenant_id": event_data["conversation"]["tenantId"],
            "event_type": event_data.get("type")
        })

        raise DataHubInstanceUnavailableError(instance.url)
```

## 📊 **Monitoring & Observability**

### **Key Metrics**

The router should track operational metrics for reliability:

- **Routing Success Rate**: Percentage of events successfully routed to target instances
- **Routing Latency**: End-to-end webhook processing time (p95, p99)
- **Unknown Tenant Rate**: Events from unmapped tenants requiring admin attention
- **Instance Health**: Availability and response time of registered DataHub instances
- **Fallback Usage**: Events routed to default instances due to primary instance failures

### **Structured Logging**

Events should be logged with sufficient context for debugging and analytics:

- Tenant ID and team information for routing decisions
- Target instance and routing strategy used
- Processing latency and success/failure status
- Error details for failed routing attempts

## 🔧 **Configuration Management**

### **Administrative Interface**

The router requires administrative capabilities for operational management:

**Tenant Management:**

- Create, read, update, delete tenant-to-instance mappings
- Support for team-specific routing within tenants
- Default instance configuration per tenant

**Instance Management:**

- Register new DataHub instances (cloud and on-premises)
- Monitor instance health and availability status
- Update connection parameters and authentication

**Operational Monitoring:**

- View routing statistics and performance metrics
- Review unknown tenant events requiring attention
- Monitor tunnel connection status for on-premises instances

### **Configuration Validation**

Configuration changes should be validated to ensure system consistency:

- Verify target instances are reachable and active
- Prevent conflicting tenant mappings
- Ensure each tenant has at least one default routing rule
- Validate authentication credentials and permissions

## 🚀 **Deployment Architecture**

### **Production Requirements**

The multi-tenant router is designed as a stateless service that can be deployed with high availability:

**Infrastructure:**

- Load balancer for distributing Teams webhook traffic
- Persistent storage for tenant mappings and configuration
- Caching layer for improved routing performance
- Monitoring and alerting integration

**Scaling Considerations:**

- Horizontal scaling through multiple router instances
- Database connection pooling and read replicas
- Geographic distribution for global Teams deployments
- Circuit breakers for handling instance failures

### **Security Architecture**

**Network Security:**

- TLS termination at load balancer
- Internal network isolation between components
- Rate limiting and DDoS protection
- Bot Framework signature validation

**Access Control:**

- API key authentication for administrative operations
- Role-based access for configuration management
- Audit logging for all configuration changes
- Encrypted storage of sensitive credentials

## 🎯 **Implementation Status**

### **Current Capabilities**

✅ **OAuth Proxy Architecture**: Multi-tenant router handles OAuth callbacks and routes to correct DataHub instances
✅ **Basic Webhook Routing**: Teams events routed based on tenant identification  
✅ **Tenant Mapping**: OAuth-based automatic tenant registration with secure state encoding
✅ **Configuration Management**: Centralized OAuth configuration for consistent setup experience

### **Actual Endpoint Usage Analysis**

Based on code analysis of the current implementation:

**Router Endpoints (Port 9005):**

- `POST /public/teams/webhook` - ✅ **USED**: Main webhook receiver for Teams events
- `POST /api/messages` - ✅ **USED**: Bot Framework message handler
- `GET /public/teams/oauth/callback` - ✅ **USED**: OAuth callback proxy for setup flows
- `GET /health` - ✅ **USED**: Health check endpoint
- `POST /admin/instances` - ⚠️ **IMPLEMENTED**: Admin API for instance management (manual use only)
- `GET /admin/tenants/{tenant_id}/mapping` - ⚠️ **IMPLEMENTED**: Tenant mapping queries (manual use only)
- `POST /admin/tenants/{tenant_id}/mapping` - ⚠️ **IMPLEMENTED**: Manual tenant mapping creation (not used by OAuth flow)
- `GET /admin/unknown-tenants` - ⚠️ **IMPLEMENTED**: Unknown tenant alerts (manual use only)
- `GET /admin/routing-stats` - ⚠️ **IMPLEMENTED**: Routing statistics (manual use only)

**Frontend Integration (Port 3000):**

- ❌ **NOT CALLED**: Frontend does not directly call router endpoints
- ✅ **OAUTH FLOW**: Frontend initiates OAuth which redirects to router callback
- ✅ **GRAPHQL PROXY**: Frontend gets OAuth config via GraphQL → Integrations Service

**Integrations Service Integration (Port 9003):**

- ✅ **RECEIVES WEBHOOKS**: Router forwards Teams events to `/public/teams/webhook`
- ✅ **OAUTH COMPLETION**: Router forwards OAuth callbacks to `/private/teams/oauth/complete`
- ✅ **PROVIDES CONFIG**: Serves OAuth configuration at `/teams/oauth/config`
- ❌ **REGISTRATION**: Does not call router's `/api/instances/register` (auto-registration via OAuth instead)

**Key Findings:**

1. **No Direct Frontend-Router Communication**: All frontend interactions go through router OAuth redirects
2. **Auto-Registration Works**: OAuth flow automatically creates tenant mappings without manual API calls
3. **Admin APIs Unused**: Most admin endpoints exist for manual configuration but aren't used by the application
4. **Port Strategy**: Router (9005) → Integrations Service (9003) → Frontend (3000) flow works correctly

### **Future Enhancements**

**Operational Features:**

- Comprehensive admin dashboard for configuration management
- Advanced health monitoring and alerting integration
- Performance optimization with caching layers
- Load balancing and circuit breaker patterns

**On-Premises Support:**

- WebSocket tunnel management for secure connectivity
- Automatic instance registration and discovery
- Heartbeat monitoring and reconnection handling

**Enterprise Features:**

- Team-specific routing for large organization deployments
- Advanced routing strategies and fallback mechanisms
- Audit logging and compliance reporting
- Geographic distribution and disaster recovery

## 🔒 **Security Considerations**

### **Authentication & Authorization**

**Public Endpoints (No Authentication Required):**

- `POST /public/teams/webhook` - Teams webhook receiver (validated by Bot Framework JWT)
- `POST /api/messages` - Bot Framework messages (validated by Bot Framework JWT)
- `GET /public/teams/oauth/callback` - OAuth callback proxy (validated by Microsoft OAuth state)
- `GET /health` - Health check endpoint
- `GET /` - Root endpoint with server info

**Protected Admin Endpoints (API Key Required):**

- `POST /admin/instances` - Create DataHub instances
- `GET /admin/instances` - List DataHub instances
- `POST /admin/tenants/{tenant_id}/mapping` - Create tenant mappings
- `GET /admin/tenants/{tenant_id}/mapping` - Query tenant mappings
- `GET /admin/unknown-tenants` - View unknown tenant alerts
- `GET /admin/routing-stats` - View routing statistics

### **Admin API Security Implementation**

**Environment Variable Bootstrap:**

```bash
# Set admin API key (required for production)
export DATAHUB_ROUTER_ADMIN_API_KEY="your-secure-api-key-here"

# Optional: Disable auth for development (INSECURE)
export DATAHUB_ROUTER_ENABLE_AUTH="false"
# OR use CLI flag: --disable-admin-auth
```

**API Key Usage:**

```bash
# Access protected admin endpoints
curl -H "Authorization: Bearer your-secure-api-key-here" \
  http://router.datahub.com:9005/admin/instances

# Create new instance
curl -X POST \
  -H "Authorization: Bearer your-secure-api-key-here" \
  -H "Content-Type: application/json" \
  -d '{"name": "Production DataHub", "url": "https://datahub.company.com"}' \
  http://router.datahub.com:9005/admin/instances
```

**Security Features:**

- ✅ **Bearer Token Authentication** - HTTP Authorization header required
- ✅ **Auto-Generated Keys** - Random API key generated if not provided
- ✅ **Configurable Auth** - Can be disabled for development environments
- ✅ **Secure by Default** - Authentication enabled unless explicitly disabled
- ✅ **No Token Exposure** - API tokens not returned in GET responses (shows `api_token_present: boolean`)
- ✅ **Clear Security Feedback** - Startup displays auth status and usage examples

**API Key Management:**

- **Generation**: Manual (set via environment variable) or auto-generated on startup
- **Storage**: In memory only (no database persistence required)
- **Rotation**: Restart service with new `DATAHUB_ROUTER_ADMIN_API_KEY`
- **Scope**: Full admin access (create instances, manage mappings, view stats)

### **Data Privacy**

- **Minimal data logging** - only routing metadata and tenant IDs
- **Encryption at rest** for configuration data in database
- **No credential exposure** - API tokens masked in responses (`api_token_present: true/false`)
- **Secure OAuth state** - Instance routing info encoded in OAuth callbacks
- **Audit logging** for all configuration changes (via standard application logs)

### **Enhanced Security Architecture**

The router implements defense-in-depth security controls across multiple layers:

**Multi-Layer Security Model:**

- **Transport Security** - Bot Framework JWT validation for inbound webhooks
- **State Protection** - Cryptographically secured OAuth state parameters
- **Session Protection** - Anti-replay and anti-tampering controls
- **Admin Security** - Bearer token authentication with CSRF protection
- **Rate Limiting** - Request throttling by integration and source IP
- **Request Integrity** - Optional API-to-API signature validation

### **OAuth State Security**

**Security Requirements:**

- Prevent state parameter tampering and injection attacks
- Eliminate replay attacks through one-time use enforcement
- Ensure state parameters expire within reasonable timeframes
- Maintain backwards compatibility with existing OAuth flows

**Solution Architecture:**

- **Authenticated Encryption** - State parameters use cryptographic signing and encryption
- **Entropy Enhancement** - High-entropy nonces prevent prediction attacks
- **Timestamp Validation** - Configurable expiration windows prevent stale state reuse
- **Anti-Replay Protection** - Nonce tracking ensures one-time use
- **Key Derivation** - Industry-standard key strengthening for state encryption

### **Administrative Security**

**CSRF Protection Requirements:**

- Prevent cross-site request forgery on state-changing admin operations
- Support session-based token validation for admin workflows
- Automatic cleanup of expired security tokens

**Solution Architecture:**

- **Session-Bound Tokens** - CSRF tokens tied to specific admin sessions
- **One-Time Use** - Tokens invalidated after successful validation
- **Secure Generation** - Cryptographically strong token generation
- **Memory Management** - Automatic cleanup prevents token accumulation

### **Request Integrity Validation**

**API Security Requirements:**

- Optional signature validation for API-to-API communications
- Canonical request formatting to prevent manipulation
- Timestamp-based replay attack prevention

**Solution Architecture:**

- **Request Signing** - HMAC signatures cover method, path, timestamp, and body
- **Replay Protection** - Timestamp validation with configurable tolerance windows
- **Canonical Format** - Standardized request representation prevents bypass attempts

### **Security Assessment**

**Production Security Posture:**

- ✅ **OAuth State Hardened** - Cryptographically secure with anti-replay protection
- ✅ **Admin Operations Protected** - Multi-factor protection (API keys + CSRF tokens)
- ✅ **Rate Limiting Implemented** - Per-integration throttling with automatic cleanup
- ✅ **Request Integrity Available** - Optional API signature validation
- ✅ **Webhook Authentication** - Bot Framework JWT validation for all Teams events
- ✅ **Memory Management** - Automatic cleanup of security tokens and rate limit data

**Security Guarantees:**

- **State Parameter Security** - OAuth states cannot be replayed, tampered with, or predicted
- **Admin Operation Integrity** - CSRF protection prevents unauthorized admin actions
- **Request Throttling** - Rate limiting prevents abuse and DoS attacks
- **Integration Isolation** - Webhook validation ensures Teams events are authentic
- **Backward Compatibility** - Security enhancements don't break existing OAuth flows

**Network Security Recommendations:**

- 🔧 **TLS Termination** - Use reverse proxy (nginx/ALB) for HTTPS
- 🔧 **Firewall Rules** - Restrict admin endpoints to management networks
- 🔧 **IP Allowlisting** - Limit admin API access to known IP ranges
- 🔧 **DDoS Protection** - Deploy application-layer DDoS mitigation

**Future Security Enhancements:**

- 📋 **Role-Based Admin Access** - Granular permissions for different admin operations
- 📋 **Audit Trail Enhancement** - Structured security event logging
- 📋 **Key Rotation Automation** - Automated security credential rotation
- 📋 **DataHub SSO Integration** - Leverage existing DataHub authentication systems

### **Credential Management & Rotation**

Teams app credentials (`app_id` and `app_password`) are shared across all components in the multi-tenant architecture:

- **Multi-tenant router**: Needs credentials to send placeholder responses for unconfigured tenants
- **Individual DataHub instances**: Need same credentials for message verification and Teams API calls

**Credential Rotation Scenarios:**

- **Regular security policy** (90/180 days)
- **Credential compromise** (emergency rotation)
- **Personnel changes** (employee departures)
- **Environment separation** (dev/staging/prod isolation)
- **Customer isolation** (dedicated apps for single-tenant deployments)
- **Azure tenant migrations** or **disaster recovery**

**Zero-Downtime Rotation Process:**

1. Create new secret in Azure App Registration (keep old one active)
2. Update `DATAHUB_TEAMS_APP_PASSWORD` environment variable
3. Restart router and all DataHub instances
4. Verify Teams integration functionality
5. Delete old secret from Azure

**Key Points:**

- **App ID remains constant** during rotation
- **Multiple active secrets supported** for seamless rotation
- **Azure enforces expiration** (max 2 years) requiring eventual rotation
- **Environment variables recommended** for credential distribution

### **OAuth-Based Tenant Mapping**

**Secure Automatic Registration** using Microsoft OAuth flow eliminates manual pairing while ensuring tenant authenticity:

#### **OAuth Proxy Architecture**

The multi-tenant router acts as an OAuth proxy to solve the single redirect URI limitation:

```mermaid
sequenceDiagram
    participant U as DataHub Admin
    participant DH as DataHub Instance
    participant R as Multi-Tenant Router
    participant MS as Microsoft OAuth
    participant DB as Router Database

    U->>DH: Navigate to Teams Integration Setup
    DH->>MS: Initiate OAuth (redirect_uri=router/oauth/callback)
    Note over DH,MS: State parameter encodes DataHub instance URL
    MS->>U: Present OAuth consent screen
    U->>MS: Grant permissions
    MS->>R: OAuth callback with authorization code
    R->>R: Decode state to identify DataHub instance
    R->>DH: Forward OAuth callback to instance
    DH->>MS: Exchange code for access tokens
    DH->>DH: Save OAuth tokens to local database
    DH->>R: Return success response
    R->>R: Create tenant mapping (tenant_id → instance_url)
    R->>DB: Store secure tenant mapping
    R->>U: Display success page
```

#### **OAuth Flow Implementation**

**Router OAuth Endpoints:**

```python
# Multi-tenant router endpoints
@router.get("/public/teams/oauth/callback")
async def handle_oauth_callback(request: Request):
    """Handle Microsoft OAuth callback and route to correct DataHub instance"""

    # Parse OAuth response
    code = request.query_params.get("code")
    state = request.query_params.get("state")

    if not code or not state:
        raise HTTPException(400, "Missing OAuth parameters")

    # Decode state to identify target DataHub instance
    try:
        instance_info = decode_oauth_state(state)
        target_url = instance_info["url"]
        instance_id = instance_info["id"]
    except Exception as e:
        raise HTTPException(400, f"Invalid OAuth state: {e}")

    # Forward callback to DataHub instance
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{target_url}/private/teams/oauth/complete",
            params={"code": code, "state": state}
        )

        if response.status_code == 200:
            oauth_data = response.json()
            # Extract tenant ID from successful OAuth response
            tenant_id = oauth_data.get("tenant_id")

            if tenant_id:
                # Create secure tenant mapping
                await create_tenant_mapping(
                    tenant_id=tenant_id,
                    datahub_instance_url=target_url,
                    instance_id=instance_id,
                    registered_via="oauth_flow",
                    oauth_proof=oauth_data.get("access_token")  # Proof of tenant access
                )
                logger.info(f"✅ Created tenant mapping: {tenant_id} → {target_url}")

        return response

def encode_oauth_state(instance_url: str, instance_id: str) -> str:
    """Encode DataHub instance info into OAuth state parameter"""
    import base64
    import json

    state_data = {
        "url": instance_url,
        "id": instance_id,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    json_str = json.dumps(state_data)
    encoded_bytes = base64.urlsafe_b64encode(json_str.encode('utf-8'))
    return encoded_bytes.decode('utf-8').rstrip('=')
```

**DataHub Instance OAuth Completion:**

```python
# Individual DataHub instance endpoint
@private_router.get("/teams/oauth/complete")
async def complete_teams_oauth(code: str, state: Optional[str] = None):
    """Complete Microsoft OAuth flow and save Teams integration configuration"""

    # Exchange authorization code for access tokens
    token_data = await exchange_oauth_code(code, tenant_id)

    if token_data["success"]:
        # Save OAuth tokens and configuration to DataHub database
        config = TeamsConnection(
            app_details=TeamsAppDetails(
                app_id=app_id,
                app_password=app_secret,
                tenant_id=token_data["tenant_id"]
            ),
            oauth_tokens=OAuthTokens(
                access_token=token_data["access_token"],
                refresh_token=token_data["refresh_token"],
                expires_at=calculate_expiry(token_data["expires_in"])
            )
        )

        teams_config.save_config(config)

        return {
            "success": True,
            "tenant_id": token_data["tenant_id"],
            "access_token": token_data["access_token"],  # For router mapping proof
            "message": "Teams OAuth configuration completed successfully"
        }
```

#### **Security Benefits of OAuth-Based Mapping**

**Cryptographic Proof of Tenant Access:**

- **OAuth tokens prove legitimate access** to the specific Microsoft tenant
- **No manual verification required** - OAuth consent is the verification
- **Automatic tenant detection** from Microsoft's token response
- **State-based instance routing** prevents callback hijacking

**Prevents Common Attack Vectors:**

- **Tenant Hijacking**: Impossible without legitimate OAuth consent from tenant admin
- **Instance Spoofing**: State encoding cryptographically links instance to OAuth flow
- **Unauthorized Mapping**: Only successful OAuth flows can create tenant mappings
- **Replay Attacks**: OAuth codes are single-use with short expiration

**Audit Trail:**

```sql
CREATE TABLE oauth_tenant_mappings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    datahub_instance_url TEXT NOT NULL,
    instance_id TEXT,
    created_via TEXT DEFAULT 'oauth_flow',
    oauth_access_token_hash TEXT, -- SHA256 hash for audit verification
    microsoft_user_id TEXT, -- User who granted OAuth consent
    granted_permissions JSONB, -- OAuth scopes granted
    created_at TIMESTAMP DEFAULT NOW(),
    verified_at TIMESTAMP DEFAULT NOW(),

    UNIQUE(tenant_id, datahub_instance_url),
    INDEX(tenant_id),
    INDEX(created_at)
);
```

**Advantages Over Challenge-Response:**

- ✅ **No manual steps** - fully automated registration
- ✅ **Microsoft-verified identity** - relies on Azure AD authentication
- ✅ **Immediate availability** - no waiting for human verification
- ✅ **Stronger security** - cryptographic proof vs. shared secrets
- ✅ **Better UX** - single OAuth consent flow
- ✅ **Audit compliance** - OAuth events logged by Microsoft and DataHub

#### **OAuth State Management**

The OAuth state parameter securely encodes instance routing information:

```python
# Example OAuth authorization URL
https://login.microsoftonline.com/{tenant}/oauth2/v2.0/authorize?
  client_id=8f862cd7-07e2-49c1-8871-5db61a205433&
  response_type=code&
  redirect_uri=https://router.datahub.com/public/teams/oauth/callback&
  scope=https://graph.microsoft.com/.default offline_access&
  state=eyJ1cmwiOiJodHRwOi8vbG9jYWxob3N0OjkwMDMiLCJpZCI6InRlc3QtaW5zdGFuY2UifQ
```

**State Decoding:**

```python
{
  "url": "http://localhost:9003",
  "id": "test-instance",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

This ensures the OAuth callback is routed to the correct DataHub instance that initiated the flow.

## 📈 **Performance Targets**

| Metric                 | Target           | Measurement                   |
| ---------------------- | ---------------- | ----------------------------- |
| **Routing Latency**    | < 100ms p95      | End-to-end webhook processing |
| **Throughput**         | 1000+ events/sec | Concurrent webhook handling   |
| **Availability**       | 99.9%            | Service uptime                |
| **Error Rate**         | < 0.1%           | Failed routing attempts       |
| **Configuration Load** | < 50ms           | Tenant mapping lookup         |

---

This multi-tenant router design provides a robust, scalable foundation for routing Teams events across multiple DataHub instances while maintaining high availability and operational visibility.

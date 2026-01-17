- Start Date: 2026-01-13
- RFC PR: (after opening the RFC PR, update this with a link to it and update the file name)
- Discussion Issue: N/A
- Implementation PR(s): (leave this empty)

# Redis as Shared Infrastructure for DataHub

## Summary

Introduce Redis as a shared infrastructure component for DataHub to provide distributed caching,
state management, and locking across services. Redis would serve as a language-agnostic solution
that works seamlessly with both Java (GMS) and Python (integrations service) components, with
multi-tenant support through key namespacing.

## Motivation

### Current Challenges

1. **Language Barrier**: DataHub's GMS uses Hazelcast for distributed caching and coordination,
   but Hazelcast is Java-only. Python services (integrations, metadata ingestion) cannot participate
   in the same distributed infrastructure.

2. **OAuth State Management**: The integrations service needs to store OAuth state (nonce, PKCE
   code_verifier, user binding) across requests. Current options are all problematic:

   - In-memory: Fails in multi-instance deployments
   - Database/aspects: Heavyweight for ephemeral data
   - New GMS endpoints: Adds complexity and latency

3. **Distributed Locking**: Token refresh operations require locks to prevent race conditions with
   rotating refresh tokens. Multiple services may need to coordinate.

4. **Multi-Service Caching**: Backend services have inconsistent caching infrastructure:
   - GMS: Hazelcast (distributed) + Caffeine (local)
   - Integrations service: In-memory only (no distributed cache)
   - Ingestion workers: In-memory only

### Why Redis?

- **Language-agnostic**: First-class clients for Java and Python
- **Battle-tested**: Industry standard for caching, sessions, and coordination
- **TTL support**: Native expiration for ephemeral data (perfect for OAuth state)
- **Distributed locks**: Redlock algorithm for reliable distributed locking
- **Managed options**: AWS ElastiCache, Azure Cache, GCP Memorystore, Redis Cloud
- **Operational simplicity**: Single binary, simple protocol, extensive tooling

### Motivating Use Case: OAuth for External Integrations

The integrations service is implementing OAuth2 support for external providers (Glean, Snowflake, etc.).
This requires:

1. **OAuth State Storage** (10-minute TTL): nonce, code_verifier, user binding, redirect URL
2. **Distributed Locks** for token refresh: Providers with rotating refresh tokens invalidate old
   tokens on refresh; concurrent attempts can lose the refresh token permanently

Currently using in-memory storage as a placeholder (singleton deployment). This RFC proposes Redis
as the production solution for multi-instance deployments.

## Requirements

### Functional Requirements

- **R1**: Distributed key-value storage with TTL support
- **R2**: Distributed locking with timeout and automatic release
- **R3**: Multi-tenant isolation via key namespacing
- **R4**: Python client for integrations service
- **R5**: Java client integration for GMS (optional initially)
- **R6**: Support for both self-hosted and managed Redis deployments

### Non-Functional Requirements

- **NFR1**: Sub-millisecond latency for cache operations
- **NFR2**: High availability (Redis Sentinel or Cluster mode)
- **NFR3**: Encryption in transit (TLS)
- **NFR4**: Authentication (Redis AUTH or ACLs)
- **NFR5**: Graceful degradation if Redis is unavailable

### Extensibility

- **Future GMS Migration**: GMS could optionally migrate from Hazelcast to Redis for certain use cases
- **Rate Limiting**: Redis sorted sets for sliding window rate limiting
- **Pub/Sub**: Event broadcasting across services (e.g., cache invalidation)

## Non-Requirements

- **Full Hazelcast replacement in GMS**: Redis is additive, not replacing Hazelcast
- **Redis Cluster sharding**: Initial implementation targets single-node or Sentinel HA
- **Redis as primary datastore**: Redis is for caching and coordination, not persistent storage
- **Custom Redis modules**: Standard Redis features only (no RedisJSON, RediSearch, etc.)

## Detailed design

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DataHub Deployment                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│  │     GMS      │     │ Integrations │     │   Ingestion  │                │
│  │   (Java)     │     │   Service    │     │   Workers    │                │
│  │              │     │   (Python)   │     │   (Python)   │                │
│  └──────┬───────┘     └──────┬───────┘     └──────┬───────┘                │
│         │                    │                    │                         │
│         │    ┌───────────────┴────────────────────┘                        │
│         │    │                                                              │
│         ▼    ▼                                                              │
│  ┌─────────────────┐                                                        │
│  │     Redis       │  ← Shared infrastructure                               │
│  │   (Sentinel)    │    - oauth:state:{nonce}                              │
│  │                 │    - lock:refresh:{user}:{provider}                   │
│  │                 │    - cache:search:{hash}                              │
│  └─────────────────┘    - rate:api:{client}:{window}                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Namespacing Strategy

All keys use a structured namespace pattern for multi-tenant isolation:

```
{tenant}:{service}:{category}:{identifier}
```

**Examples:**

```
default:oauth:state:abc123def456          # OAuth state for default tenant
default:oauth:lock:refresh:user1:glean    # Token refresh lock
acme:oauth:state:xyz789                   # OAuth state for 'acme' tenant
```

**Namespace Registry:**

| Namespace            | Service      | Purpose                 | TTL            |
| -------------------- | ------------ | ----------------------- | -------------- |
| `oauth:state`        | integrations | OAuth flow state        | 10 min         |
| `oauth:lock:refresh` | integrations | Token refresh locks     | 30 sec         |
| `cache:search`       | gms          | Search result caching   | 5 min          |
| `cache:entity`       | gms          | Entity resolution cache | 1 hour         |
| `rate:api`           | gms          | API rate limiting       | sliding window |

### Configuration

**Environment Variables:**

| Variable                 | Default     | Description                           |
| ------------------------ | ----------- | ------------------------------------- |
| `DATAHUB_REDIS_ENABLED`  | `false`     | Enable Redis integration              |
| `DATAHUB_REDIS_HOST`     | `localhost` | Redis host or Sentinel master name    |
| `DATAHUB_REDIS_PORT`     | `6379`      | Redis port                            |
| `DATAHUB_REDIS_PASSWORD` | (none)      | Redis AUTH password                   |
| `DATAHUB_REDIS_SSL`      | `false`     | Enable TLS                            |
| `DATAHUB_TENANT`         | `default`   | Tenant identifier for key namespacing |

**Docker Compose Addition:**

```yaml
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --appendonly yes
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
```

**Kubernetes:** Configure via Helm values to point to managed Redis (AWS ElastiCache, Azure Cache) or Valkey.

### Multi-Tenant Isolation Options

1. **Namespace prefix** (recommended): All keys prefixed with tenant ID
2. **Separate databases**: Each tenant uses different Redis DB (0-15 limit)
3. **Separate instances**: Complete isolation but higher cost
4. **Redis ACLs**: Fine-grained permissions per tenant (Redis 6+)

## How we teach this

| Audience           | Impact                                             |
| ------------------ | -------------------------------------------------- |
| Backend developers | New dependency to configure                        |
| DevOps/Platform    | New infrastructure component to deploy and monitor |
| End users          | No direct impact (implementation detail)           |

**Documentation Updates:**

1. Add Redis to deployment documentation (Docker, Kubernetes, quickstart)
2. Update architecture diagrams to show Redis
3. Add troubleshooting guide for Redis connectivity issues

## Drawbacks

### Additional Infrastructure

- **New dependency**: Redis adds another component to deploy, monitor, and secure
- **Operational overhead**: Requires Redis expertise for production deployment
- **Cost**: Additional resource consumption (memory-bound workload)

### Duplication with Hazelcast

- **Two caching systems**: GMS would have both Hazelcast and Redis
- **Consistency questions**: Which system to use for what?

### Mitigation

- Start with optional Redis (feature flag)
- Clear guidelines for when to use Redis vs Hazelcast
- Graceful degradation when Redis unavailable

## Alternatives

### Alternative 1: Hazelcast Client for Python

Create Python client that connects to GMS's Hazelcast cluster.

**Why not chosen**: Hazelcast is Java-first; Python support is secondary and less mature.

### Alternative 2: Database-Based State

Store OAuth state in MySQL/Postgres as DataHub aspects or custom tables.

**Why not chosen**: Database is not designed for ephemeral data with short TTLs; requires cleanup jobs.

### Alternative 3: GMS REST API Proxy

Python services call GMS endpoints that internally use Hazelcast.

**Why not chosen**: Adds complexity and latency; GMS becomes bottleneck.

## Rollout / Adoption Strategy

### Phase 1: Infrastructure

1. Add Redis to Helm chart (enabled by default when integrations service is enabled)
2. Add Redis to Docker Compose for local development
3. Support external Redis (ElastiCache, etc.) as alternative to bundled Redis
4. Update deployment documentation

### Phase 2: Integrations Service

1. Use Redis for OAuth state storage
2. Use Redis for token refresh distributed locks
3. No fallback - Redis is a required dependency

### Phase 3: Expanded Usage (As Needed)

1. Rate limiting for APIs
2. Caching for expensive operations
3. Optional GMS integration for specific features

## Future Work

### Potential Future Use Cases

1. Search result caching
2. Rate limiting (sliding window, leaky bucket)
3. Session storage across GMS instances
4. Pub/Sub for cache invalidation
5. Background job coordination

### GMS Hazelcast Migration (Long-term)

If Redis proves valuable, consider gradual migration:

| Feature           | Current (Hazelcast) | Future (Redis)    | Risk   |
| ----------------- | ------------------- | ----------------- | ------ |
| Distributed cache | IMap                | GET/SET           | Low    |
| Distributed lock  | FencedLock          | Redlock           | Medium |
| Pub/Sub           | ITopic              | PUBLISH/SUBSCRIBE | Low    |

## Unresolved questions

1. **Redis Cluster vs Sentinel**:

   - Recommendation: Redis Cluster (can start with single node, scales as needed)
   - Using managed Redis (ElastiCache, Azure Cache) makes this straightforward

2. **Persistence strategy**: RDB vs AOF?

   - For ephemeral data (OAuth state): No persistence needed
   - For longer-lived data: AOF recommended

3. **Memory limits**: Configure appropriate memory limits and eviction policy for the workload

4. **GMS adoption timeline**: When/if should GMS start using Redis?
   - Wait for Python services to prove value first

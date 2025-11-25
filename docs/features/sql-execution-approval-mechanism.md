# SQL Execution Approval Mechanism - Technical Specification

**Version:** 1.0  
**Date:** October 31, 2025  
**Status:** Draft for Review  
**Parent Spec:** [SQL Execution for DataHub Chatbot](sql-execution-chatbot-spec.md)

## Table of Contents

1. [Overview](#overview)
2. [Why System-Enforced Approval](#why-system-enforced-approval)
3. [Architecture: Two-Phase Execution](#architecture-two-phase-execution)
4. [API Specification](#api-specification)
5. [Security Guarantees](#security-guarantees)
6. [Mutator Safety](#mutator-safety)
7. [Implementation Details](#implementation-details)
8. [Error Handling](#error-handling)
9. [Testing Strategy](#testing-strategy)

---

## Overview

This document specifies the approval mechanism for SQL query execution in the DataHub chatbot. The core principle is **system-enforced approval** - we cannot rely on the LLM to consistently ask for user approval.

### Key Requirements

1. **No LLM Trust**: System must prevent execution without approval, regardless of LLM behavior
2. **Preview Before Execute**: All queries must be validated and shown to user before execution
3. **Token-Based Security**: Short-lived, single-use tokens enforce the two-phase flow
4. **Enhanced Safety for Mutators**: If INSERT/UPDATE/DELETE supported, extra safeguards required
5. **Audit Trail**: Both preview and execute operations must be logged

---

## Why System-Enforced Approval

### The Problem

LLMs are powerful but unpredictable. We **cannot rely** on prompts to guarantee the LLM will always ask for approval:

**Scenarios where LLM might skip approval:**

1. User says "just do it" and LLM interprets as implicit approval
2. Confusing prompt engineering or context leads to misunderstanding
3. LLM optimization leads it to take shortcuts
4. Adversarial prompts attempt to bypass safety checks
5. Context window limitations cause LLM to forget approval requirement

**Risk Analysis:**

| Risk                    | Without System Enforcement     | With System Enforcement |
| ----------------------- | ------------------------------ | ----------------------- |
| Unapproved SELECT query | Medium (data exposure)         | ✅ Prevented            |
| Unapproved UPDATE query | **Critical** (data corruption) | ✅ Prevented            |
| Replay attack           | High (multiple executions)     | ✅ Prevented            |
| Token stealing          | High (unauthorized execution)  | ✅ Mitigated            |

### The Solution

**System-enforced two-phase execution** using approval tokens:

- Phase 1 (Preview): Safe, validates query, generates token
- Phase 2 (Execute): Requires token from Phase 1

Even if LLM misbehaves, system prevents execution without approval.

---

## Architecture: Two-Phase Execution

### Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     Two-Phase Flow                           │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  User Request                                                │
│       ↓                                                      │
│  LLM generates SQL                                           │
│       ↓                                                      │
│  ┌──────────────────────────────────────────────┐           │
│  │ PHASE 1: PREVIEW (Safe, No Execution)       │           │
│  │                                              │           │
│  │  • Validate SQL syntax                      │           │
│  │  • Check allowed operations                 │           │
│  │  • Authorize user                           │           │
│  │  • Estimate affected rows                   │           │
│  │  • Generate approval token                  │           │
│  │  • Store token in cache                     │           │
│  │                                              │           │
│  │  Returns: {                                 │           │
│  │    query, isValid, approvalToken,           │           │
│  │    warning (if mutator)                     │           │
│  │  }                                          │           │
│  └──────────────────────────────────────────────┘           │
│       ↓                                                      │
│  LLM shows preview to user                                   │
│       ↓                                                      │
│  User approves                                               │
│       ↓                                                      │
│  ┌──────────────────────────────────────────────┐           │
│  │ PHASE 2: EXECUTE (Requires Token)           │           │
│  │                                              │           │
│  │  • Validate token exists                    │           │
│  │  • Check not expired (5 min TTL)            │           │
│  │  • Verify user ownership                    │           │
│  │  • Consume token (single use)               │           │
│  │  • Execute query                            │           │
│  │  • Stream results via SSE                   │           │
│  │                                              │           │
│  │  Returns: SSE stream with results           │           │
│  └──────────────────────────────────────────────┘           │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Approval Token Structure

```java
public class ApprovalToken {
  private Urn userUrn;              // User who created token
  private String query;              // Exact query to execute
  private QueryType queryType;       // SELECT, INSERT, UPDATE, etc.
  private String connectionUrn;      // Connection to use
  private String datasetUrn;         // Optional dataset context
  private Instant createdAt;         // When token was created
  private Instant expiresAt;         // When token expires (5 min)
  private String correlationId;      // For audit trail
}
```

**Token Storage:**

- In-memory cache (Caffeine) or Redis for distributed deployments
- TTL: 5 minutes
- Key: UUID (approval token ID)
- Value: ApprovalToken object

---

## API Specification

### Phase 1: Preview Query (POST /api/v3/sql/snowflake/preview)

#### Request

```json
{
  "query": "SELECT customer_id, revenue FROM customers ORDER BY revenue DESC LIMIT 10",
  "connectionUrn": "urn:li:dataHubConnection:snowflake-query-urn:li:corpuser:alice",
  "datasetUrn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.customers,PROD)"
}
```

**Fields:**

- `query` (required): SQL query to preview
- `connectionUrn` (optional): User's connection URN. If omitted, uses default for user.
- `datasetUrn` (optional): Dataset URN for authorization context

#### Response: SELECT Query

```json
{
  "query": "SELECT customer_id, revenue FROM customers ORDER BY revenue DESC LIMIT 10",
  "isValid": true,
  "queryType": "SELECT",
  "affectedTables": ["prod.sales.customers"],
  "estimatedRows": "< 100",
  "approvalToken": "exec_token_abc123xyz789",
  "expiresAt": "2025-10-31T12:35:00Z",
  "expiresInSeconds": 300,
  "correlationId": "corr_abc123"
}
```

#### Response: Mutator Query (if supported)

```json
{
  "query": "UPDATE customers SET region='US' WHERE country='USA'",
  "isValid": true,
  "queryType": "UPDATE",
  "affectedTables": ["prod.sales.customers"],
  "estimatedRowsAffected": 5234,
  "warning": "⚠️ This will MODIFY 5,234 rows in Snowflake. This action cannot be automatically undone.",
  "requiresEnhancedApproval": true,
  "approvalToken": "exec_mutator_xyz789abc",
  "expiresAt": "2025-10-31T12:35:00Z",
  "expiresInSeconds": 300,
  "correlationId": "corr_xyz789"
}
```

#### Error Responses

**400 Bad Request - Invalid SQL:**

```json
{
  "error": "INVALID_SQL",
  "message": "Only SELECT statements are allowed",
  "details": "Query contains forbidden keyword: DROP"
}
```

**403 Forbidden - Unauthorized:**

```json
{
  "error": "FORBIDDEN",
  "message": "User does not have permission to query this dataset",
  "datasetUrn": "urn:li:dataset:(...)"
}
```

**404 Not Found - Connection Missing:**

```json
{
  "error": "CONNECTION_NOT_FOUND",
  "message": "No Snowflake connection found for user. Please configure your connection in Settings.",
  "setupUrl": "/settings/connections/snowflake"
}
```

### Phase 2: Execute Query (POST /api/v3/sql/snowflake/execute)

#### Request

```json
{
  "approvalToken": "exec_token_abc123xyz789"
}
```

**Fields:**

- `approvalToken` (required): Token from preview response

**Headers:**

```http
Accept: text/event-stream
Authorization: Bearer {session-token}
```

#### Response: SSE Stream

```http
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive

event: progress
data: {"status":"validating","message":"Validating approval token..."}

event: progress
data: {"status":"connecting","message":"Connecting to Snowflake..."}

event: progress
data: {"status":"executing","message":"Executing query..."}

event: result
data: {"columns":[{"name":"customer_id","type":"NUMBER"}],"rows":[["1001"],["1002"]],"rowCount":2}

event: complete
data: {"executionId":"exec_abc123","status":"SUCCESS","correlationId":"corr_abc123"}
```

#### Error Responses

**401 Unauthorized - Invalid Token:**

```json
{
  "error": "INVALID_APPROVAL_TOKEN",
  "message": "Invalid or expired approval token. Please preview the query again."
}
```

**401 Unauthorized - Token Expired:**

```json
{
  "error": "APPROVAL_TOKEN_EXPIRED",
  "message": "Approval token expired after 5 minutes. Please preview the query again.",
  "expiresAt": "2025-10-31T12:35:00Z"
}
```

**401 Unauthorized - Token Already Used:**

```json
{
  "error": "APPROVAL_TOKEN_CONSUMED",
  "message": "This approval token has already been used. Please preview the query again if you want to re-execute.",
  "previousExecutionId": "exec_abc123"
}
```

**401 Unauthorized - Wrong User:**

```json
{
  "error": "APPROVAL_TOKEN_OWNERSHIP",
  "message": "This approval token belongs to a different user"
}
```

---

## Security Guarantees

### 1. Cannot Execute Without Preview

**Guarantee:** No query can be executed without first being previewed and generating a valid approval token.

**Enforcement:**

- Execute endpoint requires `approvalToken` parameter
- Token can only be obtained from preview endpoint
- Execute endpoint validates token exists before any execution

**Attack Prevention:**

- LLM cannot guess valid tokens (UUIDs)
- Cannot bypass preview by calling execute directly
- Cannot reuse old tokens

### 2. Single-Use Tokens (Prevents Replay)

**Guarantee:** Each approval token can only be used once.

**Enforcement:**

- Token removed from cache immediately upon use
- Subsequent attempts with same token fail with `APPROVAL_TOKEN_CONSUMED`

**Attack Prevention:**

- Cannot replay the same query multiple times
- Cannot share token with others for duplicate execution
- Limits damage if token is leaked

### 3. Time-Limited Tokens (5 Minutes)

**Guarantee:** Tokens expire 5 minutes after creation.

**Enforcement:**

- `expiresAt` timestamp stored in token
- Execute endpoint checks `token.expiresAt < now()`
- Expired tokens automatically removed from cache (TTL)

**Attack Prevention:**

- Limits window for stolen token use
- Forces fresh approval for delayed execution
- Reduces attack surface over time

**Rationale for 5 minutes:**

- Long enough for normal user workflow (preview → read → approve)
- Short enough to limit exposure if token leaked
- Can be configured per deployment if needed

### 4. User-Bound Tokens

**Guarantee:** Token only works for the user who created it.

**Enforcement:**

- Token contains `userUrn` field
- Execute endpoint verifies `token.userUrn == currentUser.urn`

**Attack Prevention:**

- Cannot steal another user's approval token
- Cannot impersonate other users
- Multi-tenant safe

### 5. Immutable Query

**Guarantee:** Query cannot be modified after preview.

**Enforcement:**

- Token contains exact query text
- Execute uses query from token, not from request
- Query cannot be altered client-side

**Attack Prevention:**

- Cannot preview safe query, then execute dangerous one
- Cannot inject SQL after approval
- What user sees is what executes

### 6. Audit Trail

**Guarantee:** Both preview and execute operations are fully logged.

**Enforcement:**

- Preview logs: query, user, timestamp, token generated
- Execute logs: token used, results, duration, correlation ID
- Correlation ID links preview and execute events

**Benefits:**

- Security monitoring
- Compliance auditing
- Incident investigation
- Usage analytics

---

## Mutator Safety

If INSERT/UPDATE/DELETE/MERGE operations are supported, additional safety measures are required.

### Enhanced Preview for Mutators

**Requirements:**

1. Must estimate affected row count
2. Must show clear warning to user
3. Must set `requiresEnhancedApproval` flag
4. Must identify all affected tables

**Example Preview Response:**

```json
{
  "queryType": "UPDATE",
  "estimatedRowsAffected": 5234,
  "affectedTables": ["prod.sales.customers"],
  "warning": "⚠️ This will MODIFY 5,234 rows in Snowflake. This action cannot be automatically undone.",
  "requiresEnhancedApproval": true,
  "rollbackGuidance": "To undo this change, you would need to:\n1. Have a backup of the data\n2. Run: UPDATE customers SET region=<original_value> WHERE ...",
  "approvalToken": "exec_mutator_xyz"
}
```

### Row Count Estimation

**Challenge:** How to estimate rows affected before execution?

**Approaches:**

| Approach                      | Accuracy | Performance       | Recommendation           |
| ----------------------------- | -------- | ----------------- | ------------------------ |
| **EXPLAIN plan**              | Medium   | Fast              | ✅ Recommended           |
| **COUNT(\*) with same WHERE** | High     | Slow (runs query) | Use for small tables     |
| **Table statistics**          | Low      | Very fast         | Fallback if above fail   |
| **No estimation**             | N/A      | N/A               | ❌ Not safe for mutators |

**Recommended Implementation:**

```sql
-- For: UPDATE customers SET region='US' WHERE country='USA'
EXPLAIN UPDATE customers SET region='US' WHERE country='USA';

-- Snowflake returns execution plan with row estimates
-- Parse plan to extract estimated rows
```

### Row Limits for Mutators

**Configuration:**

```yaml
datahub:
  sql:
    snowflake:
      mutators:
        enabled: true
        maxRowsPerStatement: 1000
        requireExplicitRowCount: true # Fail if can't estimate
```

**Enforcement:**

```java
if (queryType.isMutator()) {
  Integer estimatedRows = estimateAffectedRows(query, connection);

  if (estimatedRows == null && config.requireExplicitRowCount()) {
    throw new ValidationException(
      "Cannot estimate affected rows for this mutator query. " +
      "Please add a more specific WHERE clause."
    );
  }

  if (estimatedRows != null && estimatedRows > config.getMaxRowsPerStatement()) {
    throw new ValidationException(
      String.format(
        "This query would affect %d rows, but the maximum is %d. " +
        "Please refine your WHERE clause to affect fewer rows.",
        estimatedRows,
        config.getMaxRowsPerStatement()
      )
    );
  }
}
```

### User Confirmation Flow for Mutators

**Frontend should show enhanced confirmation:**

```
┌────────────────────────────────────────────────┐
│  ⚠️  WARNING: This Will Modify Data            │
├────────────────────────────────────────────────┤
│                                                 │
│  Query Type: UPDATE                             │
│  Affected Tables: prod.sales.customers          │
│  Estimated Rows: 5,234                          │
│                                                 │
│  SQL:                                           │
│  UPDATE customers                               │
│  SET region='US'                                │
│  WHERE country='USA'                            │
│                                                 │
│  ⚠️  This action cannot be automatically undone.│
│                                                 │
│  [ ] I understand this will modify 5,234 rows  │
│                                                 │
│  [Cancel]  [Yes, Execute This Update]          │
└────────────────────────────────────────────────┘
```

Checkbox must be checked to enable execute button.

---

## Implementation Details

### GMS: Preview Endpoint

```java
@RestController
@RequestMapping("/api/v3/sql/snowflake")
public class SnowflakeSqlController {

  private final SqlValidator sqlValidator;
  private final AuthorizationService authorizationService;
  private final ApprovalTokenService approvalTokenService;
  private final ConnectionService connectionService;

  @PostMapping("/preview")
  public ResponseEntity<PreviewResponse> previewQuery(
      @RequestBody PreviewRequest request,
      @Authenticated OperationContext opContext
  ) {
    // 1. Validate SQL
    QueryValidationResult validation = sqlValidator.validate(request.getQuery());

    if (!validation.isValid()) {
      throw new BadRequestException(validation.getErrorMessage());
    }

    // 2. Authorize user
    if (request.getDatasetUrn() != null) {
      boolean authorized = authorizationService.authorize(
        opContext,
        UrnUtils.getUrn(request.getDatasetUrn()),
        PoliciesConfig.VIEW_DATASET_USAGE_PRIVILEGE
      );

      if (!authorized) {
        throw new ForbiddenException(
          "User does not have permission to query this dataset"
        );
      }
    }

    // 3. Verify connection exists
    Urn connectionUrn = resolveConnectionUrn(request, opContext);
    EntityResponse connection = connectionService.getConnectionEntityResponse(
      opContext,
      connectionUrn
    );

    if (connection == null) {
      throw new NotFoundException(
        "No Snowflake connection found. Please configure in Settings."
      );
    }

    // 4. Estimate affected rows (for mutators)
    Integer estimatedRows = null;
    String warning = null;

    if (validation.getQueryType().isMutator()) {
      estimatedRows = estimateAffectedRows(
        request.getQuery(),
        connectionUrn,
        opContext
      );

      warning = String.format(
        "⚠️ This will MODIFY %s rows in Snowflake. " +
        "This action cannot be automatically undone.",
        estimatedRows != null ? estimatedRows.toString() : "an unknown number of"
      );
    }

    // 5. Generate approval token
    String correlationId = UUID.randomUUID().toString();

    ApprovalToken token = ApprovalToken.builder()
      .userUrn(opContext.getActorContext().getActorUrn())
      .query(request.getQuery())
      .queryType(validation.getQueryType())
      .connectionUrn(connectionUrn)
      .datasetUrn(request.getDatasetUrn())
      .createdAt(Instant.now())
      .expiresAt(Instant.now().plus(5, ChronoUnit.MINUTES))
      .correlationId(correlationId)
      .build();

    String tokenId = approvalTokenService.storeToken(token);

    // 6. Audit log
    auditLogger.log(AuditEvent.builder()
      .event("SQL_PREVIEW")
      .actor(token.getUserUrn())
      .action("PREVIEW")
      .metadata(Map.of(
        "query", request.getQuery(),
        "queryType", validation.getQueryType(),
        "approvalToken", tokenId,
        "correlationId", correlationId
      ))
      .build()
    );

    // 7. Return response
    return ResponseEntity.ok(PreviewResponse.builder()
      .query(request.getQuery())
      .isValid(true)
      .queryType(validation.getQueryType())
      .affectedTables(validation.getAffectedTables())
      .estimatedRows(validation.getQueryType() == QueryType.SELECT ? "< 1000" : null)
      .estimatedRowsAffected(estimatedRows)
      .warning(warning)
      .requiresEnhancedApproval(validation.getQueryType().isMutator())
      .approvalToken(tokenId)
      .expiresAt(token.getExpiresAt())
      .expiresInSeconds(300)
      .correlationId(correlationId)
      .build()
    );
  }
}
```

### GMS: Execute Endpoint

```java
@PostMapping("/execute")
public SseEmitter executeQuery(
    @RequestBody ExecuteRequest request,
    @Authenticated OperationContext opContext
) {
  String tokenId = request.getApprovalToken();

  // 1. Retrieve and validate token
  ApprovalToken token = approvalTokenService.getToken(tokenId);

  if (token == null) {
    throw new UnauthorizedException(
      "INVALID_APPROVAL_TOKEN",
      "Invalid or expired approval token. Please preview the query again."
    );
  }

  if (token.isExpired()) {
    approvalTokenService.removeToken(tokenId);
    throw new UnauthorizedException(
      "APPROVAL_TOKEN_EXPIRED",
      "Approval token expired. Please preview the query again."
    );
  }

  // 2. Verify ownership
  Urn currentUserUrn = opContext.getActorContext().getActorUrn();
  if (!token.getUserUrn().equals(currentUserUrn)) {
    throw new UnauthorizedException(
      "APPROVAL_TOKEN_OWNERSHIP",
      "This approval token belongs to a different user"
    );
  }

  // 3. Consume token (single use)
  boolean removed = approvalTokenService.removeToken(tokenId);

  if (!removed) {
    throw new UnauthorizedException(
      "APPROVAL_TOKEN_CONSUMED",
      "This approval token has already been used"
    );
  }

  log.info("Executing approved query for user {} (correlation: {})",
    currentUserUrn, token.getCorrelationId());

  // 4. Audit log
  String executionId = UUID.randomUUID().toString();

  auditLogger.log(AuditEvent.builder()
    .event("SQL_EXECUTE")
    .actor(currentUserUrn)
    .action("EXECUTE")
    .metadata(Map.of(
      "executionId", executionId,
      "query", token.getQuery(),
      "queryType", token.getQueryType(),
      "approvalToken", tokenId,
      "correlationId", token.getCorrelationId()
    ))
    .build()
  );

  // 5. Execute query with SSE streaming
  SseEmitter emitter = new SseEmitter(600_000L); // 10 min

  queryExecutor.executeQueryAsync(
    token.getQuery(),
    token.getConnectionUrn(),
    token.getDatasetUrn(),
    token.getQueryType(),
    executionId,
    token.getCorrelationId(),
    opContext,
    emitter
  );

  return emitter;
}
```

### Approval Token Service

```java
@Service
public class ApprovalTokenService {

  private final Cache<String, ApprovalToken> tokenCache;

  public ApprovalTokenService() {
    this.tokenCache = Caffeine.newBuilder()
      .expireAfterWrite(5, TimeUnit.MINUTES)
      .maximumSize(10_000)
      .build();
  }

  public String storeToken(ApprovalToken token) {
    String tokenId = UUID.randomUUID().toString();
    tokenCache.put(tokenId, token);
    return tokenId;
  }

  @Nullable
  public ApprovalToken getToken(String tokenId) {
    return tokenCache.getIfPresent(tokenId);
  }

  public boolean removeToken(String tokenId) {
    ApprovalToken token = tokenCache.getIfPresent(tokenId);
    if (token != null) {
      tokenCache.invalidate(tokenId);
      return true;
    }
    return false;
  }
}
```

### MCP Tools (Python)

```python
@mcp.tool()
def preview_snowflake_query(
    query: str,
    dataset_urn: Optional[str] = None
) -> dict:
    """
    Preview a SQL query WITHOUT executing it.

    This validates the query, checks permissions, and returns an approval token.
    You MUST call this before execute_snowflake_query.

    Args:
        query: SQL query to preview
        dataset_urn: Optional dataset URN for authorization context

    Returns:
        Preview result with approval token:
        - query: The validated query
        - isValid: Whether query is valid
        - queryType: Type of query (SELECT, UPDATE, etc.)
        - approvalToken: Token for execution (expires in 5 min)
        - warning: Warning message (if mutator)
    """
    client = get_datahub_client()
    user_urn = get_current_user_urn()
    connection_urn = f"urn:li:dataHubConnection:snowflake-query-{user_urn}"

    try:
        response = client.post("/api/v3/sql/snowflake/preview", json={
            "query": query,
            "connectionUrn": connection_urn,
            "datasetUrn": dataset_urn
        })

        result = response.json()

        logger.info(
            f"Preview successful: {result['queryType']} query, "
            f"token expires in {result['expiresInSeconds']}s"
        )

        return result

    except httpx.HTTPStatusError as e:
        error_data = e.response.json()
        logger.error(f"Preview failed: {error_data.get('message')}")

        return {
            "isValid": False,
            "error": error_data.get("error"),
            "message": error_data.get("message")
        }


@mcp.tool()
def execute_snowflake_query(approval_token: str) -> dict:
    """
    Execute a previously previewed SQL query.

    CRITICAL: This requires an approval token from preview_snowflake_query.
    You CANNOT execute without first previewing.

    Args:
        approval_token: Token from preview_snowflake_query (expires in 5 min)

    Returns:
        Query execution results:
        - columns: List of column definitions
        - rows: List of row data
        - rowCount: Number of rows returned
        - executionTimeMs: Query execution time
    """
    client = get_datahub_client()

    try:
        # Note: In real implementation, this would handle SSE streaming
        response = client.post("/api/v3/sql/snowflake/execute", json={
            "approvalToken": approval_token
        })

        result = response.json()

        logger.info(
            f"Execution successful: {result['rowCount']} rows in "
            f"{result['executionTimeMs']}ms"
        )

        return result

    except httpx.HTTPStatusError as e:
        error_data = e.response.json()
        logger.error(f"Execution failed: {error_data.get('message')}")

        return {
            "error": error_data.get("error"),
            "message": error_data.get("message"),
            "columns": [],
            "rows": [],
            "rowCount": 0
        }
```

---

## Error Handling

### Preview Errors

| Error Code             | HTTP Status | Cause                                                | User Action                      |
| ---------------------- | ----------- | ---------------------------------------------------- | -------------------------------- |
| `INVALID_SQL`          | 400         | Query contains syntax errors or forbidden operations | Fix SQL syntax                   |
| `FORBIDDEN`            | 403         | User lacks dataset permissions                       | Request access to dataset        |
| `CONNECTION_NOT_FOUND` | 404         | No Snowflake connection configured                   | Configure connection in Settings |
| `VALIDATION_ERROR`     | 400         | Query would affect too many rows                     | Refine WHERE clause              |

### Execute Errors

| Error Code                 | HTTP Status | Cause                            | User Action                        |
| -------------------------- | ----------- | -------------------------------- | ---------------------------------- |
| `INVALID_APPROVAL_TOKEN`   | 401         | Token doesn't exist              | Preview query again                |
| `APPROVAL_TOKEN_EXPIRED`   | 401         | Token older than 5 minutes       | Preview query again                |
| `APPROVAL_TOKEN_CONSUMED`  | 401         | Token already used               | Preview query again to re-execute  |
| `APPROVAL_TOKEN_OWNERSHIP` | 401         | Token belongs to different user  | Use your own token                 |
| `QUERY_TIMEOUT`            | 504         | Query exceeded 30 second timeout | Optimize query or increase timeout |
| `CONNECTION_ERROR`         | 500         | Failed to connect to Snowflake   | Check connection credentials       |

---

## Testing Strategy

### Unit Tests

**Token Lifecycle:**

- Token creation and storage
- Token retrieval
- Token expiration
- Token removal

**Validation:**

- SQL syntax validation
- Operation type detection (SELECT vs mutator)
- Row count estimation

### Integration Tests

**Preview Flow:**

- Valid SELECT query → token generated
- Invalid SQL → validation error
- Unauthorized user → 403 error
- Missing connection → 404 error
- Mutator query → enhanced response with warning

**Execute Flow:**

- Valid token → query executes
- Invalid token → 401 error
- Expired token → 401 error
- Used token → 401 error (replay protection)
- Wrong user's token → 401 error

**End-to-End:**

- Preview → Execute → Success
- Preview → Wait 6 min → Execute → Expired error
- Preview → Execute twice → Second fails (single-use)

### Security Tests

**Attack Scenarios:**

- Token guessing → Should fail (UUID)
- Token stealing → Should fail (user-bound)
- Token replay → Should fail (single-use)
- Query modification → Should fail (immutable)
- Expired token reuse → Should fail (TTL)

### LLM Behavior Tests

**LLM Misbehavior:**

- LLM skips preview → Execute fails with token error
- LLM uses fake token → Execute fails with invalid token
- LLM loses token → Must preview again
- LLM tries to modify query → Executes original from token

---

## Appendix: Token Flow Diagram

```
┌────────────────────────────────────────────────────────────────┐
│                      Token Lifecycle                            │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  [PREVIEW REQUEST]                                              │
│         │                                                       │
│         ├──► Validate SQL                                      │
│         ├──► Authorize user                                    │
│         ├──► Generate UUID token                               │
│         │                                                       │
│         ▼                                                       │
│  ┌──────────────────┐                                          │
│  │ Token Cache      │                                          │
│  ├──────────────────┤                                          │
│  │ tokenId: UUID    │ ◄─── Store with 5 min TTL              │
│  │ user: alice      │                                          │
│  │ query: SELECT... │                                          │
│  │ expires: T+5min  │                                          │
│  └──────────────────┘                                          │
│         │                                                       │
│         └──► Return token to client                            │
│                                                                 │
│  [USER APPROVES]                                                │
│         │                                                       │
│  [EXECUTE REQUEST with tokenId]                                │
│         │                                                       │
│         ├──► Lookup token in cache                             │
│         ├──► Check not expired                                 │
│         ├──► Verify user = alice                               │
│         ├──► Remove from cache (consume)                       │
│         │                                                       │
│         ▼                                                       │
│  Execute query from token.query                                │
│         │                                                       │
│         └──► Stream results                                    │
│                                                                 │
│  [SUBSEQUENT ATTEMPT with same tokenId]                        │
│         │                                                       │
│         ├──► Lookup token in cache                             │
│         └──► Not found → Error (already consumed)              │
│                                                                 │
└────────────────────────────────────────────────────────────────┘
```

---

## Summary

The approval mechanism provides defense-in-depth for SQL execution:

1. **System-Enforced**: Architecture prevents execution without approval
2. **Two-Phase**: Preview (safe) → Execute (requires token)
3. **Secure Tokens**: Short-lived, single-use, user-bound
4. **Enhanced for Mutators**: Extra warnings and confirmations
5. **Fully Audited**: Complete trail for compliance
6. **LLM-Proof**: Works even if LLM misbehaves

This design ensures that user approval is **mandatory** regardless of LLM behavior or adversarial prompts.

# Micrometer Best Practices Guide

This guide provides practical recommendations for using the Micrometer library with Prometheus in production environments, addressing common pitfalls and unclear documentation areas.

## Metric Types and When to Use Them

### Counters

**Use for:** Values that only increase (requests processed, errors occurred, bytes sent)

```java
// ✅ Good - Initialize once, reuse everywhere
private final Counter requestCounter = Counter.builder("http_requests_total")
    .description("Total HTTP requests")
    .register(meterRegistry);

// Usage
requestCounter.increment();
requestCounter.increment(5.0);
```

**Anti-patterns:**

- Don't use for values that can decrease
- Don't create new counters for each operation

### Gauges

**Use for:** Current state values (active connections, queue size, temperature)

```java
// ✅ Good - Gauge tied to an object's state
private final AtomicInteger activeConnections = new AtomicInteger(0);

Gauge.builder("active_connections")
    .description("Number of active database connections")
    .register(meterRegistry, activeConnections, AtomicInteger::get);

// ✅ Good - Using a supplier function
Gauge.builder("jvm_memory_used")
    .register(meterRegistry, this, obj -> Runtime.getRuntime().totalMemory());
```

### Timers

**Use for:** Measuring duration and frequency of events

```java
// ✅ Good - Initialize once
private final Timer requestTimer = Timer.builder("http_request_duration")
    .description("HTTP request duration")
    .register(meterRegistry);

// Usage patterns
Timer.Sample sample = Timer.start(meterRegistry);
// ... do work ...
sample.stop(requestTimer);

// Or using recordCallable
requestTimer.recordCallable(() -> {
    // ... timed operation ...
    return result;
});
```

## Tags/Labels: The Complete Guide

### Static Tags (Recommended)

Initialize metrics with known, finite tag sets at application startup:

```java
// ✅ Excellent - Finite, predictable tag values
private final Counter httpRequestsCounter = Counter.builder("http_requests_total")
    .description("Total HTTP requests")
    .tag("method", "GET")
    .tag("endpoint", "/api/users")
    .register(meterRegistry);

// ✅ Good - Create a map for multiple static tag combinations
private final Map<String, Counter> methodCounters = Map.of(
    "GET", createCounter("GET"),
    "POST", createCounter("POST"),
    "PUT", createCounter("PUT")
);
```

### Dynamic Tags (Use with Extreme Caution)

**The Cardinal Sin: Unbounded Dynamic Tags**

```java
// ❌ NEVER DO THIS - Creates unlimited metrics
Counter.builder("user_requests")
    .tag("user_id", userId) // If userId is unbounded, this will cause memory leaks
    .register(meterRegistry)
    .increment();
```

**Why this is problematic:**

- Each unique tag combination creates a new time series in Prometheus
- Prometheus performance degrades with high cardinality

**Safe Dynamic Tag Patterns:**

```java
// ✅ Acceptable - Bounded dynamic tags with known finite values
private final Map<String, Counter> statusCounters = new ConcurrentHashMap<>();

public void recordHttpResponse(String method, int statusCode) {
    // Limit to standard HTTP methods and status code ranges
    String normalizedMethod = normalizeHttpMethod(method);
    String statusClass = statusCode / 100 + "xx"; // 2xx, 4xx, 5xx

    String key = normalizedMethod + "_" + statusClass;
    statusCounters.computeIfAbsent(key, k ->
        Counter.builder("http_responses_total")
            .tag("method", normalizedMethod)
            .tag("status_class", statusClass)
            .register(meterRegistry)
    ).increment();
}

private String normalizeHttpMethod(String method) {
    return Set.of("GET", "POST", "PUT", "DELETE", "PATCH")
        .contains(method.toUpperCase()) ? method.toUpperCase() : "OTHER";
}
```

### Tag Value Guidelines

- **Keep values short and normalized**: Long, detailed tag values often indicate high cardinality - normalize them into categories
- **Limit cardinality**: Aim for < 1000 unique combinations per metric
- **Use enums when possible**: Ensures finite, known values

```java
// ✅ Good - Using enums for guaranteed finite values
public enum DatabaseOperation {
    SELECT, INSERT, UPDATE, DELETE
}

Counter.builder("database_operations_total")
    .tag("operation", operation.name().toLowerCase())
    .register(meterRegistry);
```

## Naming Conventions

### Metric Names

Follow Prometheus naming conventions:

```java
// ✅ Good metric names
"http_requests_total"           // Counter: use _total suffix
"http_request_duration_seconds" // Timer/Histogram: include unit
"database_connections_active"   // Gauge: describe current state
"jvm_memory_used_bytes"        // Include units in name
"cache_hits_total"
"queue_size_current"

// ❌ Poor metric names
"httpRequests"                 // Use snake_case, not camelCase
"requests"                     // Too generic
"duration"                     // No unit specified
"count"                        // Not descriptive
```

### Tag Names

```java
// ✅ Good tag names
.tag("method", "GET")          // Clear, standard HTTP concept
.tag("status_code", "200")     // Standard terminology
.tag("endpoint", "/api/users") // Clear path identifier
.tag("service", "user-api")    // Service identification

// ❌ Poor tag names
.tag("m", "GET")              // Too abbreviated
.tag("httpMethod", "GET")     // Avoid camelCase
.tag("path_template", "/api/users/{id}") // Underscores in values OK, not in keys
```

## Common Anti-Patterns and Solutions

### 1. Creating Metrics in Hot Paths

**Why this is bad:**

- **Warning spam**: Micrometer logs a warning on first duplicate registration, then debug messages thereafter - problematic if debug logging is enabled
- **Performance overhead**: Registration involves mutex locks and map operations on every request
- **Concurrency bugs**: Race conditions in Micrometer's registration code can cause inconsistent metric state
- **Memory leaks**: High cardinality tags create unbounded metric instances that never get cleaned up

```java
// ❌ Bad - Creates metrics repeatedly in request handling
public void handleRequest(String userId) {
    Counter.builder("user_requests")
        .tag("user", userId)
        .register(meterRegistry)
        .increment();
}

// ✅ Good - Pre-create metrics outside hot paths
private final Counter requestsCounter = Counter.builder("requests_total")
    .description("Total requests processed")
    .register(meterRegistry);

public void handleRequest(String userId) {
    requestsCounter.increment();

    // If you need user-specific metrics, use bounded patterns:
    String userTier = getUserTier(userId); // "free", "premium", "enterprise"
    getUserTierCounter(userTier).increment();
}
```

### 2. High Cardinality Tags

```java
// ❌ Bad - Each user creates new time series
.tag("user_id", request.getUserId())
.tag("session_id", request.getSessionId())
.tag("request_id", request.getRequestId())

// ✅ Good - Bounded, meaningful dimensions
.tag("user_tier", getUserTier(request.getUserId()))  // "free", "premium", "enterprise"
.tag("feature", request.getFeature())               // Limited set of features
.tag("region", request.getRegion())                 // Limited set of regions
```

### 3. Not Using Description

```java
// ❌ Missing context
Counter.builder("requests").register(meterRegistry);

// ✅ Good - Clear description for operators
Counter.builder("http_requests_total")
    .description("Total number of HTTP requests processed by the application")
    .register(meterRegistry);
```

## Memory and Performance Considerations

### Registry Management

```java
// ✅ Use a single registry instance across your application
@Configuration
public class MicrometerConfig {

    @Bean
    @Primary
    public MeterRegistry meterRegistry() {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        // Configure common tags that apply to all metrics
        registry.config().commonTags("application", "datahub", "environment", activeProfile);
        return registry;
    }
}
```

Benefits: simplified configuration, consistent tagging, single endpoint management.

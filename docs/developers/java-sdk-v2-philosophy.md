# Why We Hand-Crafted the Java SDK V2 (Instead of Generating It)

## The Question

When building DataHub's Java SDK V2, we faced a choice that every API platform eventually confronts: should we generate our SDK from OpenAPI specs, or hand-craft it?

OpenAPI code generation is seductive. Tools like OpenAPI Generator promise instant SDKs in dozens of languages. Run a command, get a client—complete with type-safe models, proper serialization, and comprehensive endpoint coverage. Why would anyone choose to write thousands of lines of code by hand?

We chose to hand-craft. This document explains why.

## When Code Generation Works Beautifully

Let's be clear: code generation isn't wrong. It's incredibly effective when your abstraction boundary aligns with your wire protocol.

**CRUD APIs**: If your API exposes resources like `GET /users/{id}`, `POST /users`, `DELETE /users/{id}`, a generated client is perfect:

```java
User user = client.getUser(123);
client.createUser(newUser);
client.deleteUser(456);
```

The user's mental model—"I want to fetch/create/delete a user"—maps directly to HTTP operations. There's no translation needed.

**Protocol Buffers**: Google's protobuf generators are exemplary because the `.proto` file **is** the contract:

```protobuf
service UserService {
  rpc GetUser(UserId) returns (User);
  rpc ListUsers(ListRequest) returns (UserList);
}
```

The service definition becomes the client API with perfect fidelity. What you define is what users get.

**The Pattern**: Code generation excels when **the API's conceptual model matches user mental models**, and the wire protocol fully captures domain semantics.

## The Semantic Gap: Why DataHub Is Different

DataHub doesn't fit this mold. Our metadata platform has a semantic gap between what users want to do and what the HTTP API exposes.

### The Aspect-Based Model

DataHub stores metadata as discrete "aspects"—properties, tags, ownership, schemas. But users don't think in aspects. They think:

- "I want to add a 'PII' tag to this dataset"
- "I need to assign ownership to John"
- "This table should be in the Finance domain"

An OpenAPI-generated client would expose:

```java
// What the API provides
client.updateGlobalTags(entityUrn, globalTagsPayload);
client.updateOwnership(entityUrn, ownershipPayload);
```

But to use this, you need to know:

- What is `GlobalTags`? How do I construct it?
- Should I use PUT (full replacement) or PATCH (incremental update)?
- How do I avoid race conditions when multiple systems update tags?
- Where do tags even live—in system aspects or editable aspects?

This is expert-level knowledge pushed onto every user.

### The Patch Complexity

DataHub supports both full aspect replacement (PUT) and JSON Patch (incremental updates). The generated client would expose both:

```java
// Full replacement
void putGlobalTags(Urn entityUrn, GlobalTags tags);

// JSON Patch
void patchGlobalTags(Urn entityUrn, JsonPatch patch);
```

Now users must decide when to use each. Patches are safer (no race conditions), but how do you construct a JsonPatch? Do you use a PatchBuilder? Hand-write JSON?

Every user solves this problem independently, reinventing best practices.

### The Mode Problem

DataHub has dual aspects: **system aspects** (written by ingestion pipelines) and **editable aspects** (written by humans via UI/SDK). Users editing metadata should write to editable aspects, but pipelines should write to system aspects.

A generated client doesn't understand this distinction. It just exposes endpoints. Users must learn DataHub's aspect model to route correctly.

## Five Principles of Hand-Crafted SDKs

Our hand-crafted SDK addresses these gaps through five design principles.

### 1. Semantic Layers Translate Domain Concepts

The SDK provides operations that match how users think:

```java
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("fact_revenue")
    .build();

// Think "add a tag", not "construct and PUT a GlobalTags aspect"
dataset.addTag("pii");

// Think "assign ownership", not "build an Ownership aspect"
dataset.addOwner("urn:li:corpuser:jdoe", OwnershipType.TECHNICAL_OWNER);

client.entities().upsert(dataset);
```

The SDK translates `addTag()` into the correct:

- Aspect type (GlobalTags)
- Operation type (JSON Patch for safety)
- Aspect variant (editable, in SDK mode)
- JSON path (into the aspect structure)

This is **semantic translation**—mapping domain intent to wire protocol. Generators can't do this because the semantics live in institutional knowledge, not OpenAPI specs.

### 2. Opinionated APIs: The 95/5 Rule

We optimized for the 95% case and provided escape hatches for the 5%.

**The 95% case**: Incremental metadata changes—add a tag, update ownership, set a domain.

```java
dataset.addTag("sensitive")
       .addOwner(ownerUrn, type)
       .setDomain(domainUrn);

client.entities().update(dataset);
```

Users never think about PUT vs PATCH, aspect construction, or batch strategies. It just works.

**The 5% case**: Complete aspect replacement, custom MCPs, or operations V2 doesn't support.

```java
// Drop to V1 SDK for full control
RestEmitter emitter = client.emitter();
MetadataChangeProposalWrapper mcpw = /* custom logic */;
emitter.emit(mcpw).get();
```

This philosophy—**make simple things trivial, complex things possible**—requires intentional API design. Generators produce flat API surfaces where every operation has equal weight.

### 3. Encoding Expert Knowledge

Every platform accumulates tribal knowledge:

- "Always use patches for concurrent-safe updates"
- "Editable aspects override system aspects in SDK mode"
- "Batch operations to avoid Kafka load spikes"
- "Schema field names don't always match aspect names"

A generated client leaves this knowledge in Slack threads and documentation. Users discover best practices through painful trial and error.

The hand-crafted SDK **encodes** this knowledge:

```java
// Users call addTag(), SDK internally:
// - Creates a JSON Patch (not full replacement)
// - Targets the editable aspect in SDK mode
// - Accumulates patches for atomic emission
// - Uses the correct field paths
```

The SDK becomes **executable documentation** of best practices. This scales better than tribal knowledge.

### Why Not an ORM Approach?

Tools like Hibernate, SQLAlchemy, and Pydantic+ORM excel at managing complex object graphs in transactional applications. Why didn't we use this pattern?

**Metadata operations follow different patterns than OLTP workloads:**

1. **Bulk mutations** - "Tag 50 datasets as PII" requires only URNs and the operation, not loading full object graphs
2. **Point lookups** - "Get this dataset's schema before querying" is a direct fetch, no relationship navigation needed
3. **Read-modify-write** - "Infer quality scores from schema statistics" involves fetching an aspect, transforming it, and patching it back

ORMs optimize for relationship traversal (`dataset.container.database.catalog`), session lifecycle management, and automatic dirty tracking. But:

- **Relationship traversal** is handled by DataHub's search and graph query APIs, not in-memory navigation
- **Explicit patches** are central to our design—we want `addTag()` visible in code, not hidden behind session flush
- **Session complexity** adds cognitive overhead without benefit for metadata's bulk/point/patch patterns

The result: a simpler, more explicit API that matches how developers actually work with metadata.

### 4. Centralized Maintenance vs Distributed Pain

Generated clients push maintenance costs onto users. When we improve DataHub:

- **Add a new endpoint**: Users regenerate their client. Breaking change? Every team upgrades simultaneously.
- **Change error handling**: Regenerate. Update all call sites.
- **Optimize batch operations**: Can't—that logic lives in user code, reinvented by every team.

Hand-crafted SDKs centralize expertise:

- **Add convenience methods**: Users pull the SDK update. No code changes required.
- **Improve retry logic**: Fixed once in the SDK. All users benefit immediately.
- **Optimize batching**: Built into the SDK. Users get better performance automatically.

The total maintenance cost is **lower** because we fix problems once instead of every team solving them independently.

### 5. Progressive Disclosure

Generated clients are flat—every endpoint is equally visible. Hand-crafted SDKs enable **progressive disclosure**: simple tasks are simple, complexity is opt-in.

**Day 1 user**: Create and tag a dataset

```java
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("my_table")
    .build();

dataset.addTag("pii");
client.entities().upsert(dataset);
```

No need to understand aspects, patches, or modes.

**Week 1 user**: Manage governance

```java
dataset.addOwner(ownerUrn, type)
       .setDomain(domainUrn)
       .addTerm(termUrn);
```

Still pure domain operations.

**Month 1 user**: Understand update vs upsert

```java
// update() emits only patches (for existing entities)
Dataset existing = client.entities().get(urn);
Dataset mutable = existing.mutable();  // Get writable copy
mutable.addTag("sensitive");
client.entities().update(mutable);

// upsert() emits full aspects + patches
Dataset newEntity = Dataset.builder()...;
client.entities().upsert(newEntity);
```

Complexity revealed **when needed**, not upfront.

### 6. Immutability by Default

Entities fetched from the server are **read-only by default**, enforcing explicit mutation intent.

**The Problem:**

Traditional SDKs allow silent mutation of fetched objects:

```java
Dataset dataset = client.get(urn);
// Pass to function - might it mutate dataset? Who knows!
processDataset(dataset);
// Is dataset still the same? Must read all code to know
```

**The Solution:**

Immutable-by-default makes mutation intent explicit:

```java
Dataset dataset = client.get(urn);
// dataset is read-only - safe to pass anywhere
processDataset(dataset);

// Want to mutate? Make it explicit
Dataset mutable = dataset.mutable();
mutable.addTag("updated");
client.entities().upsert(mutable);
```

**Benefits:**

- **Safety:** Can't accidentally mutate shared references
- **Clarity:** `.mutable()` call signals write intent
- **Debugging:** Easier to track where mutations happen
- **Concurrency:** Safe to share read-only entities across threads

**Design Inspiration:**

This pattern is common in modern APIs because immutability scales better than defensive copying:

- **Rust's ownership model** - mut vs immutable borrows
- **Python's frozen dataclasses** - `@dataclass(frozen=True)`
- **Java's immutable collections** - `Collections.unmodifiableList()`
- **Functional programming principles** - immutable data structures

When you see `.mutable()` in our SDK, you're seeing battle-tested patterns from languages designed for safety and concurrency.

## What This Costs (And Why It's Worth It)

Hand-crafting isn't free:

- **3,000+ lines of code** across entity classes, caching, and operations
- **457 tests** validating workflows, not just HTTP mechanics
- **13 documentation guides** teaching patterns, not just parameters
- **Ongoing maintenance** as DataHub evolves

But this investment compounds. Every hour we spend on the SDK saves hundreds of hours across our user community. The SDK makes metadata management **effortless** instead of just **possible**.

Compare total cost of ownership:

| Approach         | Initial Dev | User Onboarding | Ongoing Support | Innovation Speed |
| ---------------- | ----------- | --------------- | --------------- | ---------------- |
| Generated Client | Hours       | High (steep)    | High (repeated) | Slow (coupled)   |
| Hand-Crafted SDK | Weeks       | Low (gradual)   | Low (central)   | Fast (buffered)  |

After 6-12 months, the hand-crafted SDK becomes cheaper because centralized expertise scales better than distributed tribal knowledge.

## The Philosophy: What SDKs Should Be

This isn't about generated vs hand-crafted code. It's about what we believe SDKs **should be**.

**SDKs are not just API wrappers.** They are:

- **Semantic layers** that translate domain concepts to wire protocols
- **Knowledge repositories** that encode best practices
- **Usability interfaces** that optimize for human cognition
- **Evolution buffers** that allow internals to improve while APIs remain stable

Code generation is perfect when **the API is the abstraction**. But for domain-rich platforms where users think in terms of datasets, lineage, and governance—not HTTP verbs and JSON payloads—hand-crafted SDKs aren't just better. They're necessary.

## When Should You Generate? When Should You Craft?

**Generate when**:

- Your API's conceptual model matches user mental models
- The wire protocol fully captures domain semantics
- Operations are mostly stateless CRUD
- You prioritize API coverage over workflow optimization

**Hand-craft when**:

- Domain concepts require translation to wire protocol
- Users need guidance on best practices
- Stateful workflows matter (accumulate changes, emit atomically)
- You prioritize usability over feature completeness

DataHub falls firmly in the second category. Our users don't want to learn aspect models, patch formats, or mode routing. They want to **add a tag to a dataset** and have it just work.

That's what the hand-crafted SDK delivers.

## Conclusion: Empathy at Scale

In an era of automation, there's pressure to generate everything. But some problems demand craftsmanship.

The hand-crafted SDK is an act of **empathy at scale**. It says: "We understand your problems. We've encoded the solutions. You shouldn't have to become a DataHub expert to use DataHub."

A generated client says: "Here's our API. Figure it out."

A hand-crafted SDK says: "Here's how to solve your problems."

That difference is why we invested in hand-crafting. And it's why our users can focus on their data, not our API internals.

---

**Document Status**: Design Philosophy
**Author**: DataHub OSS Team
**Last Updated**: 2025-01-06

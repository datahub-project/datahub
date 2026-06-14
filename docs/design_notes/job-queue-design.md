# JobQueue SDK: Distributed Work Claiming for DataHub Services

## Problem

Multiple DataHub services need distributed work coordination:

- **Task processing**: Multiple worker instances discover and execute tasks (e.g., workflow runs, ingestion requests) without duplicates
- **Ownership coordination**: Multiple instances coordinate ownership of long-running jobs (e.g., monitoring pipelines, scheduled workflows)

Both implement the same pattern — discover work, atomically claim it via CAS, release when done — but today each service must build this from scratch.

## Goal

A shared SDK that any DataHub service can use to:

1. **Discover** available work (pluggable strategy)
2. **Claim** work atomically (exactly-one-winner via CAS)
3. **Release** work when done

No subclassing, no per-domain wrapper classes. Callers configure behavior via constructor arguments and lambdas.

## Non-Goals

- Heartbeat / lease renewal semantics (caller's responsibility — just delay releasing)
- Sweeper / expiry logic is provided as an **opt-in** component. The caller is responsible for running exactly one sweeper process (distributed leader election is out of scope).
- Dedicated `ClaimInfo` PDL aspect (may be added later, parked for now)
- Replacing existing entity models (the SDK works with whatever aspects already exist)

---

## Architecture

```
┌──────────────────────────────┐
│  JobQueue                    │  ← Caller-facing API
│  poll() → List[Job]          │
│  Job.release()               │
├──────────────────────────────┤
│  Discovery  │ Claim │Sweeper │  ← Composed internally
│  poll()     │ try/  │sweep() │
│  →WorkItems │ rel   │        │
├─────────────┴───────┴────────┤
│  ConditionalWriter           │  ← CAS mechanics
│  read_versioned_aspect()     │
│  write_if_version()          │
├──────────────────────────────┤
│  DataHubGraph                │  ← Transport (existing)
└──────────────────────────────┘
```

The caller interacts with `JobQueue` for normal claim/release, and optionally with `Sweeper` (on a dedicated master process) for stale claim cleanup. Everything below is configuration: which events to poll, which aspect to CAS on, what "claimed" and "released" look like — all supplied as constructor args and lambdas.

---

## Layer 1: ConditionalWriter

Pure CAS mechanics over any aspect on any entity. Zero domain knowledge.

### API

```python
@dataclass
class CASResult:
    success: bool
    new_version: Optional[str] = None  # version after write (if success)
    reason: Optional[str] = None       # "conflict", "error", etc. (if failure)

@dataclass
class VersionedAspect:
    """An aspect value paired with its version string."""
    version: str
    aspect: Optional[AspectBase] = None


class ConditionalWriter:
    """Thin wrapper around DataHubGraph for conditional aspect writes."""

    def __init__(self, graph: DataHubGraph): ...

    def read_version(self, entity_urn: str, aspect_name: str) -> str:
        """Read the current version of an aspect. "-1" if absent."""
        ...

    def read_versioned_aspect(
        self,
        entity_urn: str,
        aspect_name: str,
        aspect_class: Type[AspectBase],
    ) -> VersionedAspect:
        """Read version and typed aspect in a single GMS call.

        Replaces the two-call pattern of read_version() + read_aspect().
        Returns VersionedAspect with aspect=None if type doesn't match.
        """
        ...

    def read_aspect(
        self,
        entity_urn: str,
        aspect_name: str,
        aspect_class: Type[AspectBase],
    ) -> Optional[AspectBase]:
        """Read the current value of an aspect. Returns None if absent."""
        ...

    def write_if_version(
        self,
        entity_urn: str,
        aspect_name: str,
        aspect_value: AspectBase,
        expected_version: str,
        track_new_version: bool = True,
    ) -> CASResult:
        """
        Write aspect only if current version matches expected_version.

        Uses If-Version-Match header. GMS validates server-side:
        - Version matches → write succeeds, returns new version
        - Version mismatch → write rejected, returns conflict

        When track_new_version=False, skips the post-write read (saves 1 RT).
        """
        ...
```

### Implementation Notes

- `read_version` uses `graph.get_entity_as_mcps()` and extracts `systemMetadata.version`
- `read_versioned_aspect` merges `read_version` + `read_aspect` into a single HTTP call
- `write_if_version` uses `graph.emit_mcp()` with `headers={"If-Version-Match": expected_version}`
- When `track_new_version=False`, skips the post-write `read_version()` call (used by release)
- Precondition failures (412 / version mismatch) are caught and returned as `CASResult(success=False, reason="conflict")`
- All other exceptions propagate to the caller
- Relies on GMS `ConditionalWriteValidator` (available since DataHub 0.10.5)

---

## Layer 2a: Claim

Atomic claim and release of a single entity. Internally tracks held versions.

### API

```python
class Claim:
    def __init__(
        self,
        graph: DataHubGraph,
        aspect_name: str,
        aspect_class: Type[_Aspect],
        make_claimed: Callable[[str, Optional[_Aspect]], _Aspect],   # (owner_id, current) → claimed aspect
        make_released: Callable[[str, Optional[_Aspect]], _Aspect], # (owner_id, current) → released aspect
        is_claimed: Callable[[_Aspect], bool],                      # aspect → already claimed?
    ):
        """
        Args:
            graph: DataHub graph client
            aspect_name: The aspect used for claiming (e.g., "workflowTaskStatus")
            aspect_class: The aspect's Python class
            make_claimed: Callable that takes (owner_id, current_aspect) and returns
                the aspect value representing "claimed by owner".  current_aspect is
                the existing value (None if absent), allowing the callable to preserve
                fields it does not own.
            make_released: Callable that takes (owner_id, current_aspect) and returns
                the aspect value representing "released by owner".
            is_claimed: Predicate that inspects an existing aspect and returns True
                if it is already in "claimed" state.  Used to prevent sequential
                overwrites — without this, a second caller could read the latest
                version and CAS-write over a legitimate claim.
        """
        self._writer = ConditionalWriter(graph)
        self._aspect_name = aspect_name
        self._aspect_class = aspect_class
        self._make_claimed = make_claimed
        self._make_released = make_released
        self._is_claimed = is_claimed
        self._held: Dict[str, _HeldClaim] = {}  # urn → held claim info

    def try_claim(
        self,
        urn: str,
        owner_id: str,
        claim_hint: Optional[VersionedAspect] = None,
    ) -> bool:
        """
        Attempt to claim an entity.

        If claim_hint is provided and its aspect type matches the claim
        aspect class, the initial read is skipped (saves 1 RT).

        Reads the current aspect value and version.  If the aspect already
        indicates a claim (via is_claimed), returns False immediately.
        Otherwise CAS-writes the claimed value.

        Tracks the new version and cached aspect on success (used by release).

        Returns:
            True if claim succeeded, False if already claimed or lost to
            another claimant during CAS.
        """
        # Use hint if available and type matches (0 RT), else read (1 RT).
        if (claim_hint is not None
                and claim_hint.aspect is not None
                and isinstance(claim_hint.aspect, self._aspect_class)):
            version = claim_hint.version
            current = claim_hint.aspect
        else:
            va = self._writer.read_versioned_aspect(urn, self._aspect_name, self._aspect_class)
            version = va.version
            current = va.aspect

        # Check if already claimed.
        if current is not None and self._is_claimed(current):
            return False

        result = self._writer.write_if_version(
            urn, self._aspect_name, self._make_claimed(owner_id, current), version
        )
        if result.success:
            self._held[urn] = _HeldClaim(
                version=result.new_version,
                aspect=self._make_claimed(owner_id, current),
            )
        return result.success

    def release(self, urn: str, owner_id: str) -> bool:
        """
        Release a previously claimed entity.

        Uses the tracked version and cached aspect from try_claim to
        CAS-write the released value without additional reads (1 RT).

        Returns:
            True if release succeeded.
        """
        held = self._held.pop(urn, None)
        if held is None:
            return False
        result = self._writer.write_if_version(
            urn, self._aspect_name,
            self._make_released(owner_id, held.aspect),
            held.version,
            track_new_version=False,
        )
        return result.success

    @property
    def held_urns(self) -> Set[str]:
        """URNs currently claimed by this instance."""
        return set(self._held.keys())
```

### How It Works

```
Worker A                          GMS                           Worker B
   │                               │                               │
   │  read_versioned_aspect()      │                               │
   │ ─────────────────────────────>│   version="-1", aspect=None   │
   │ <─────────────────────────────│                               │
   │                               │  read_versioned_aspect()      │
   │                               │<──────────────────────────────│
   │                               │  version="-1", aspect=None    │
   │                               │──────────────────────────────>│
   │  write(aspect, version="-1")  │                               │
   │ ─────────────────────────────>│                               │
   │              OK (version="1") │                               │
   │ <─────────────────────────────│                               │
   │                               │  write(aspect, version="-1")  │
   │                               │<──────────────────────────────│
   │                               │  412 PRECONDITION FAILED      │
   │                               │──────────────────────────────>│
   │                               │                               │
   │  ✅ Claimed                   │                    ❌ Lost    │
```

**Optimizations over the naive approach:**

1. `read_versioned_aspect()` merges `read_version` + `read_aspect` into one HTTP call
2. When MCLDiscovery provides a `claim_hint` matching the claim aspect type, the read is skipped entirely (0 RT)
3. `release()` uses the cached aspect from claim time and `track_new_version=False` — just 1 RT (write only)

The `is_claimed` check adds a safety net: even if a worker reads the same version as an existing claim (e.g., the claimer's write was followed by a release that set a new version), `is_claimed` prevents overwriting a live claim sequentially.

### Field-Name Shortcut

For aspects with simple flat fields, a factory method generates the lambdas:

```python
@classmethod
def from_fields(
    cls,
    graph: DataHubGraph,
    aspect_name: str,
    aspect_class: Type[AspectBase],
    owner_field: str,                              # e.g., "workerId"
    state_field: str,                              # e.g., "status"
    claimed_state: str,                            # e.g., "RUNNING"
    released_state: str,                           # e.g., "COMPLETED"
    timestamp_field: Optional[str] = None,         # e.g., "startTimeMs"
    extra_claimed_fields: Optional[Dict] = None,   # additional fields on claim
    extra_released_fields: Optional[Dict] = None,  # additional fields on release
) -> "Claim":
    """
    Build a Claim from field names instead of lambdas.

    Generates make_claimed/make_released/is_claimed automatically.
    When a current aspect is available, all existing fields are preserved
    and only the claim-managed fields are overwritten.  When current is
    None, a new aspect is constructed from scratch.
    """
    def _make_claimed(owner_id: str, current: Optional[AspectBase]) -> AspectBase:
        fields = dict(current._inner_dict) if current is not None else {}
        fields.update({owner_field: owner_id, state_field: claimed_state})
        if timestamp_field:
            fields[timestamp_field] = now_ms()
        if extra_claimed_fields:
            fields.update(extra_claimed_fields)
        return aspect_class(**fields)

    def _make_released(owner_id: str, current: Optional[AspectBase]) -> AspectBase:
        fields = dict(current._inner_dict) if current is not None else {}
        fields.update({owner_field: owner_id, state_field: released_state})
        if timestamp_field:
            fields[timestamp_field] = now_ms()
        if extra_released_fields:
            fields.update(extra_released_fields)
        return aspect_class(**fields)

    def _is_claimed(aspect: AspectBase) -> bool:
        return (
            getattr(aspect, state_field, None) == claimed_state
            and getattr(aspect, owner_field, None) is not None
        )

    return cls(
        graph=graph,
        aspect_name=aspect_name,
        aspect_class=aspect_class,
        make_claimed=_make_claimed,
        make_released=_make_released,
        is_claimed=_is_claimed,
    )
```

---

## Layer 2b: Discovery

Pluggable strategy for finding work candidates. Returns URNs.

### Protocol

```python
@dataclass
class WorkItem:
    """A discovered work candidate with an optional pre-fetched aspect hint."""
    urn: str
    claim_hint: Optional[VersionedAspect] = None

class Discovery(Protocol):
    def poll(self) -> List[WorkItem]:
        """Return work items that are candidates for claiming."""
        ...
```

### Implementations

#### MCLDiscovery

Polls the DataHub MCL event stream. Filters by entity type, aspect name, and a caller-provided predicate.

```python
class MCLDiscovery:
    def __init__(
        self,
        graph: DataHubGraph,
        consumer_id: str,                                    # unique per instance
        entity_type: str,                                    # e.g., "workflowTask"
        aspect_name: str,                                    # e.g., "workflowTaskInput"
        is_candidate: Callable[[_Aspect], bool],             # filter lambda
        topic: str = "MetadataChangeLog_Versioned_v1",
        batch_size: int = 10,
        poll_timeout_s: int = 5,
        lookback_days: int = 1,
        events_consumer: Optional[Any] = None,               # inject pre-built consumer
    ): ...

    def poll(self) -> List[WorkItem]:
        """
        Poll MCL events, parse aspects, filter with is_candidate lambda.
        Commits offsets after processing. Returns WorkItems with claim_hint
        populated from the MCL event's systemMetadata and deserialized aspect.
        """
        ...
```

**Usage:**

```python
discovery = MCLDiscovery(
    graph=graph,
    consumer_id=f"scheduler-{instance_id}",
    entity_type="workflowTask",
    aspect_name="workflowTaskInput",
    is_candidate=lambda input_aspect: input_aspect.priority == "HIGH",
)
```

#### SearchDiscovery

Queries DataHub search for entities matching filters. Useful for startup catch-up, fallback recovery, or ownership-style work where entities already exist.

```python
class SearchDiscovery:
    def __init__(
        self,
        graph: DataHubGraph,
        entity_type: str,                    # e.g., "workflowTask"
        filters: Dict[str, List[str]],       # field → values
        sort_field: Optional[str] = None,    # e.g., "requestedAt"
        max_results: int = 100,
    ): ...

    def poll(self) -> List[WorkItem]:
        """
        Search for entities matching filters. Returns WorkItems
        with claim_hint=None (search results don't include versions).
        """
        ...
```

**Usage:**

```python
discovery = SearchDiscovery(
    graph=graph,
    entity_type="workflowTask",
    filters={"status": ["PENDING"]},
)
```

---

## Layer 2c: Sweeper

Optional periodic cleanup of stale claims. Discovers entities that appear to be stale, reads each one to confirm, then force-releases via CAS write.

**Requirement:** Only one sweeper process should run at a time. The caller ensures this (e.g., a dedicated master process). Distributed leader election is not part of the SDK.

### API

```python
@dataclass
class SweepResult:
    """Result of a single sweep cycle."""
    swept: List[str]      # URNs successfully force-released
    failed: List[str]     # URNs where CAS write failed (version changed)
    skipped: List[str]    # URNs that weren't actually stale after reading


class Sweeper:
    """Periodic cleanup of stale claims."""

    def __init__(
        self,
        graph: DataHubGraph,
        aspect_name: str,
        aspect_class: Type[_Aspect],
        is_stale: Callable[[_Aspect], bool],      # aspect → stale?
        make_swept: Callable[[_Aspect], _Aspect],  # current → swept aspect
        discovery: Discovery,                       # find sweep candidates
    ): ...

    def sweep(self) -> SweepResult:
        """Run one sweep cycle.

        1. Poll discovery for candidates (entities in claimed state).
        2. For each candidate, read the current versioned aspect.
        3. Check is_stale — skip if not stale.
        4. CAS-write the swept aspect value.
        5. Return results.

        CAS conflicts are expected and harmless — they mean the entity
        was released or re-claimed between the read and write.
        """
        ...
```

### Field-Name Shortcut

```python
@classmethod
def from_fields(
    cls,
    graph: DataHubGraph,
    aspect_name: str,
    aspect_class: Type[_Aspect],
    entity_type: str,                              # e.g., "workflowTask"
    owner_field: str,                              # e.g., "workerId"
    state_field: str,                              # e.g., "status"
    claimed_state: str,                            # e.g., "RUNNING"
    swept_state: str,                              # e.g., "TIMED_OUT"
    timestamp_field: str,                          # e.g., "startTimeMs"
    timeout_ms: int,                               # stale threshold
    extra_swept_fields: Optional[Dict] = None,
    max_results: int = 100,
) -> "Sweeper":
    """Build a Sweeper from field names.

    Creates a SearchDiscovery that finds entities in claimed_state,
    and generates is_stale/make_swept from field names and timeout.
    """
    ...
```

### How It Works

```
Sweeper (master)                  GMS
   │                               │
   │  SearchDiscovery.poll()       │
   │  (find entities in RUNNING)   │
   │ ─────────────────────────────>│
   │          [urn:a, urn:b]       │
   │ <─────────────────────────────│
   │                               │
   │  read_versioned_aspect(urn:a) │
   │ ─────────────────────────────>│
   │   version="5", startTimeMs=T  │
   │ <─────────────────────────────│
   │                               │
   │  (now - T) > timeout? Yes     │
   │                               │
   │  write_if_version(urn:a,      │
   │    status=TIMED_OUT, v="5")   │
   │ ─────────────────────────────>│
   │              OK               │
   │ <─────────────────────────────│
   │                               │
   │  ✅ Swept urn:a               │
```

The Sweeper does NOT use `Claim` — it doesn't hold claims, it only force-releases them. It uses `ConditionalWriter` directly, and `track_new_version=False` since it doesn't need to track versions.

When the sweeper CAS-writes `status=TIMED_OUT`, any held version in a worker's `Claim` instance becomes stale — if the worker later tries `job.release()`, the CAS fails harmlessly (version mismatch).

### Usage

```python
# Sweeper (run on a dedicated master process)
sweeper = Sweeper.from_fields(
    graph=graph,
    aspect_name="workflowTaskStatus",
    aspect_class=WorkflowTaskStatusClass,
    entity_type="workflowTask",
    owner_field="workerId",
    state_field="status",
    claimed_state="RUNNING",
    swept_state="TIMED_OUT",
    timestamp_field="startTimeMs",
    timeout_ms=3600_000,  # 1 hour
)

# Run periodically (caller's responsibility)
while running:
    result = sweeper.sweep()
    logger.info("Swept %d, failed %d, skipped %d",
                len(result.swept), len(result.failed), len(result.skipped))
    time.sleep(600)  # every 10 minutes
```

---

## Layer 3: JobQueue

Composes Discovery + Claim into a single caller-facing API.

### API

```python
class Job:
    """Handle to a claimed work item."""

    def __init__(self, urn: str, _queue: "JobQueue"):
        self.urn = urn
        self._queue = _queue

    def release(self) -> bool:
        """Release this job's claim."""
        return self._queue._claim.release(self.urn, self._queue._owner_id)


class JobQueue:
    def __init__(
        self,
        discovery: Discovery,
        claim: Claim,
        owner_id: str,
    ):
        self._discovery = discovery
        self._claim = claim
        self._owner_id = owner_id

    def poll(self) -> List[Job]:
        """
        Discover work candidates, attempt to claim each one.
        Returns only successfully claimed jobs. Passes discovery
        hints to try_claim to reduce round trips.
        """
        claimed = []
        for item in self._discovery.poll():
            if self._claim.try_claim(item.urn, self._owner_id, claim_hint=item.claim_hint):
                claimed.append(Job(urn=item.urn, _queue=self))
        return claimed

    @property
    def held_jobs(self) -> List[Job]:
        """All jobs currently claimed by this instance."""
        return [Job(urn=urn, _queue=self) for urn in self._claim.held_urns]

    def release_all(self) -> None:
        """Release all held claims. Call on shutdown."""
        for urn in list(self._claim.held_urns):
            self._claim.release(urn, self._owner_id)
```

---

## Usage: Event-Driven Task Processing

Workers discover new workflow tasks via MCL events, claim them with lambda constructors, process them, and release:

```python
queue = JobQueue(
    discovery=MCLDiscovery(
        graph=graph,
        consumer_id=f"scheduler-{instance_id}",
        entity_type="workflowTask",
        aspect_name="workflowTaskInput",
        is_candidate=lambda inp: inp.priority == "HIGH",
        batch_size=10,
        poll_timeout_s=5,
    ),
    claim=Claim(
        graph=graph,
        aspect_name="workflowTaskStatus",
        aspect_class=WorkflowTaskStatusClass,
        make_claimed=lambda owner, current: WorkflowTaskStatusClass(
            status="RUNNING",
            workerId=owner,
            startTimeMs=now_ms(),
        ),
        make_released=lambda owner, current: WorkflowTaskStatusClass(
            status="COMPLETED",
            workerId=owner,
        ),
        is_claimed=lambda aspect: aspect.status == "RUNNING"
            and aspect.workerId is not None,
    ),
    owner_id=instance_id,
)

# Worker loop
while running:
    for job in queue.poll():
        try:
            run_workflow(job.urn)
        finally:
            job.release()
```

## Usage: Search-Based Ownership

Workers use search to find unclaimed pipelines at startup, claim them for the lifetime of the process, and release on shutdown:

```python
queue = JobQueue(
    discovery=SearchDiscovery(
        graph=graph,
        entity_type="dataJob",
        filters={"status": ["ACTIVE"]},
    ),
    claim=Claim(
        graph=graph,
        aspect_name="pipelineAssignment",
        aspect_class=PipelineAssignmentClass,
        make_claimed=lambda owner, current: PipelineAssignmentClass(
            assignee=owner,
            state="OWNED",
            assignedAt=now_ms(),
        ),
        make_released=lambda owner, current: PipelineAssignmentClass(
            assignee=owner,
            state="UNOWNED",
            releasedAt=now_ms(),
        ),
        is_claimed=lambda aspect: aspect.state == "OWNED"
            and aspect.assignee is not None,
    ),
    owner_id=instance_id,
)

# Startup: claim pipelines
for job in queue.poll():
    start_pipeline(job.urn)

# Shutdown: release all
for job in queue.held_jobs:
    stop_pipeline(job.urn)
    job.release()
```

## Usage: Field-Name Shortcut

When the aspect has straightforward fields, skip the lambdas:

```python
queue = JobQueue(
    discovery=MCLDiscovery(
        graph=graph,
        consumer_id=f"scheduler-{instance_id}",
        entity_type="workflowTask",
        aspect_name="workflowTaskInput",
        is_candidate=lambda inp: inp.priority == "HIGH",
    ),
    claim=Claim.from_fields(
        graph=graph,
        aspect_name="workflowTaskStatus",
        aspect_class=WorkflowTaskStatusClass,
        owner_field="workerId",
        state_field="status",
        claimed_state="RUNNING",
        released_state="COMPLETED",
        timestamp_field="startTimeMs",
    ),
    owner_id=instance_id,
)
```

---

## Design Decisions

### Why lambdas instead of a generic `ClaimInfo` aspect?

A dedicated `ClaimInfo` aspect (with standardized `ownerId`, `claimedAt`, `expiresAt` fields) would let the SDK be fully self-contained. We're parking this for now because:

1. **Many entities already have a natural claim aspect** — e.g., a status aspect with `status=RUNNING` IS the claim. Splitting claim from status creates a two-phase problem (claim succeeds, status write fails).
2. **No schema changes required** — the SDK works with whatever aspects already exist on the entity.
3. **Can add later** — if many systems need claiming, a standardized aspect becomes more compelling. The SDK's lambda-based design means `ClaimInfo` can be introduced as a new option without changing the `Claim` class.

### Why is heartbeat/expiry not in the SDK?

- **Heartbeat** = the caller choosing when to release. The SDK does not need to know.
- **Expiry** is now provided via the optional `Sweeper` class. The sweeper detects stale claims (via a caller-defined `is_stale` predicate) and force-releases them via CAS. The caller controls timeout, cadence, and swept state — the SDK provides the mechanics.
- **Stale claim theft** = if the previous owner crashed and a sweeper released their claim (or the aspect was reset), the next `try_claim` succeeds naturally via CAS.

**Requirement:** Only one sweeper process should run at a time. The caller ensures this (e.g., a dedicated master process). Distributed leader election is not part of the SDK.

### Why is Discovery separate from Claim?

They solve orthogonal problems:

- **Discovery**: "Which URNs should I consider?" (varies — MCL events, search queries, static config)
- **Claim**: "Can I atomically take ownership of this URN?" (always the same — CAS on an aspect)

Separating them means:

- A single `Claim` instance can be shared across multiple discovery strategies (e.g., MCL primary + search fallback)
- Discovery implementations can be tested without CAS infrastructure
- New discovery strategies plug in without touching claim logic

---

## Dependencies

### Required

- **DataHubGraph client** (Python SDK) — already part of the DataHub Python SDK
- **GMS ConditionalWriteValidator** — available since DataHub 0.10.5
- **DataHub MCL Events API** — for `MCLDiscovery` (used by DataHub Actions today)

### No New Dependencies

- No new PDL aspects
- No new GMS endpoints
- No external services (Redis, ZooKeeper, etc.)
- No changes to entity-registry.yml

---

## Package Location

```
datahub/                           # existing Python SDK
└── sdk/
    └── patterns/
        ├── _shared/               # shared utilities across pattern SDKs
        │   ├── __init__.py
        │   └── conditional_writer.py   # Layer 1: CAS mechanics (CASResult, ConditionalWriter)
        └── job_queue/             # job queue pattern
            ├── __init__.py
            ├── claim.py           # Layer 2a: Claim + from_fields
            ├── discovery.py       # Layer 2b: Discovery protocol + implementations
            ├── sweeper.py         # Layer 2c: Sweeper + SweepResult
            └── job_queue.py       # Layer 3: JobQueue + Job
```

This lives in `datahub.sdk.patterns` (`metadata-ingestion/src/datahub/sdk/patterns/`) so any DataHub service can import it without cross-dependencies. `ConditionalWriter` is in `_shared/` because it's a general CAS primitive reusable by future pattern SDKs.

---

## Testing Strategy

### Unit Tests

- `ConditionalWriter`: Mock `DataHubGraph`, verify correct headers and error classification
- `Claim`: Mock `ConditionalWriter`, verify version tracking, conflict handling, held_urns state
- `Claim.try_claim`: Verify `read_aspect` + `is_claimed` check — when the aspect is already in claimed state, `try_claim` returns `False` without attempting CAS
- `Claim.from_fields`: Verify generated `make_claimed`, `make_released`, and `is_claimed` lambdas produce correct aspect values and predicate behavior
- `MCLDiscovery`: Mock event consumer, verify filtering with `is_candidate` lambda
- `SearchDiscovery`: Mock GraphQL, verify filter construction
- `JobQueue`: Mock Discovery + Claim, verify only claimed URNs returned as Jobs

### Integration Tests

- Two `Claim` instances race to claim the same URN — exactly one wins
- Winner releases, second instance claims successfully
- `MCLDiscovery` + `Claim` end-to-end with real GMS

### Smoke Tests

- Multi-worker `JobQueue.poll()` against shared GMS — no duplicate execution
- Failure scenario: worker crashes without releasing, sweeper cleans up, another worker claims

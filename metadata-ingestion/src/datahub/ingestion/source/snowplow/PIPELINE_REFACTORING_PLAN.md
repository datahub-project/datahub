# Snowplow Pipeline Modeling Refactoring Plan

## Executive Summary

This document outlines the plan to refactor the Snowplow connector's pipeline modeling from **DataFlow-per-Event-Spec** to **Single Physical Pipeline** architecture. This change aligns with Ryan's customer requirements (ownership, lineage, out-of-box clarity) while improving scalability and reducing entity duplication.

---

## 1. Current State Analysis

### 1.1 Current Architecture (DataFlow per Event Spec)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CURRENT: DataFlow Per Event Spec                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  DataFlow: "Add to Cart" Event (event_spec_1)                               │
│  ├── DataJob: IP Lookup Enrichment                                          │
│  ├── DataJob: UA Parser Enrichment                                          │
│  ├── DataJob: Campaign Attribution Enrichment                               │
│  └── DataJob: Loader                                                        │
│                                                                              │
│  DataFlow: "Page View" Event (event_spec_2)                                 │
│  ├── DataJob: IP Lookup Enrichment      ◀── DUPLICATE                       │
│  ├── DataJob: UA Parser Enrichment      ◀── DUPLICATE                       │
│  ├── DataJob: Campaign Attribution      ◀── DUPLICATE                       │
│  └── DataJob: Loader                    ◀── DUPLICATE                       │
│                                                                              │
│  ... × N event specs                                                        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘

Scale Problem:
  - 100 event specs × 10 enrichments = 1000 DataJobs
  - Plus 100 Loader DataJobs
  - Total: 1100 DataJobs for what is physically 11 jobs
```

### 1.2 Problems with Current Approach

| Problem                   | Impact                                                            |
| ------------------------- | ----------------------------------------------------------------- |
| **Entity Explosion**      | O(events × enrichments) entities created                          |
| **Doesn't Match Reality** | Snowplow has ONE pipeline that processes ALL events               |
| **Confusing for Users**   | "Why do I see IP Lookup 100 times?"                               |
| **Redundant Information** | Same field lineage duplicated per event spec                      |
| **Scalability**           | Large customers with 500+ event specs would create 5000+ DataJobs |

### 1.3 What the Current Implementation Does Well

- ✅ Field-level lineage extraction from enrichments
- ✅ Enrichment configuration parsing
- ✅ Warehouse table URN detection
- ✅ Column-level lineage (input fields → output fields)

---

## 2. Proposed Architecture (Option A: Single Physical Pipeline)

### 2.1 Target Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    PROPOSED: Single Physical Pipeline                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  DataFlow: "Snowplow Pipeline" (one per physical pipeline)                  │
│  ├── DataJob: Collector                                                     │
│  │   └── outputDatasets: [all_event_spec_urns]                              │
│  │                                                                           │
│  ├── DataJob: IP Lookup Enrichment                                          │
│  │   └── fineGrainedLineages: user_ipaddress → geo_* fields                 │
│  │                                                                           │
│  ├── DataJob: UA Parser Enrichment                                          │
│  │   └── fineGrainedLineages: useragent → br_*, os_*, dvce_* fields         │
│  │                                                                           │
│  ├── DataJob: Campaign Attribution Enrichment                               │
│  │   └── fineGrainedLineages: page_url → mkt_* fields                       │
│  │                                                                           │
│  └── DataJob: Loader                                                        │
│      └── inputDatasets: [all_event_spec_urns]                               │
│      └── outputDatasets: [warehouse_table_urn]                              │
│                                                                              │
│  Total: 1 DataFlow + ~12 DataJobs (regardless of event spec count)          │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Entity Relationships

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ENTITY RELATIONSHIPS                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  DATA LINEAGE (via UpstreamLineage on Datasets):                            │
│                                                                              │
│  ┌────────────────┐         ┌────────────────┐         ┌───────────────┐   │
│  │  Atomic Event  │────────▶│  Event Spec 1  │────────▶│               │   │
│  │    Dataset     │         │    Dataset     │         │   Warehouse   │   │
│  └────────────────┘         └────────────────┘    ┌───▶│     Table     │   │
│                                                   │    │               │   │
│  ┌────────────────┐         ┌────────────────┐    │    └───────────────┘   │
│  │  Schema:       │────────▶│  Event Spec 2  │────┤                        │
│  │  page_view     │         │    Dataset     │    │                        │
│  └────────────────┘         └────────────────┘    │                        │
│                                                   │                        │
│  ┌────────────────┐         ┌────────────────┐    │                        │
│  │  Schema:       │────────▶│  Event Spec 3  │────┘                        │
│  │  cart_item     │         │    Dataset     │                             │
│  └────────────────┘         └────────────────┘                             │
│                                                                              │
│  PIPELINE VISUALIZATION (via DataFlow/DataJob):                             │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  DataFlow: Snowplow Pipeline                                         │   │
│  │                                                                       │   │
│  │  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐          │   │
│  │  │Collector │──▶│IP Lookup │──▶│UA Parser │──▶│ Loader   │          │   │
│  │  └──────────┘   └──────────┘   └──────────┘   └──────────┘          │   │
│  │                                                                       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.3 Benefits of Proposed Architecture

| Benefit                 | Description                                                |
| ----------------------- | ---------------------------------------------------------- |
| **Scalability**         | O(enrichments) entities instead of O(events × enrichments) |
| **Accuracy**            | Matches Snowplow's physical reality (one pipeline)         |
| **Clarity**             | Users understand "these are my enrichments"                |
| **Preserved Lineage**   | Field-level lineage still visible on DataJobs              |
| **Ryan's Requirements** | Ownership flows through Dataset lineage (unchanged)        |

---

## 3. Implementation Plan

### 3.1 Files to Modify

| File                                             | Changes                                      |
| ------------------------------------------------ | -------------------------------------------- |
| `processors/pipeline_processor.py`               | Major refactoring - single pipeline creation |
| `dependencies.py`                                | Update `IngestionState` if needed            |
| `tests/unit/snowplow/test_pipeline_processor.py` | Update tests for new behavior                |

### 3.2 Detailed Changes

#### 3.2.1 `pipeline_processor.py` - `_extract_pipelines()` Method

**Current Behavior:**

- Iterates through ALL event specifications
- Creates ONE DataFlow per event specification
- Stores mapping `event_spec_id → dataflow_urn`

**New Behavior:**

- Creates ONE DataFlow per physical pipeline
- DataFlow represents the entire Snowplow processing pipeline
- No per-event-spec DataFlow creation

```python
# BEFORE (current)
def _extract_pipelines(self) -> Iterable[MetadataWorkUnit]:
    for event_spec in event_specs:
        # Create DataFlow for EACH event specification
        dataflow_urn = make_data_flow_urn(
            orchestrator="snowplow",
            flow_id=f"{pipeline_id}_event_{event_spec.id}",  # Per event spec
            ...
        )
        self.state.event_spec_dataflow_urns[event_spec.id] = dataflow_urn
        yield from self.emit_aspects(...)

# AFTER (proposed)
def _extract_pipelines(self) -> Iterable[MetadataWorkUnit]:
    # Create ONE DataFlow for the physical pipeline
    if not physical_pipelines:
        return

    pipeline = physical_pipelines[0]
    dataflow_urn = make_data_flow_urn(
        orchestrator="snowplow",
        flow_id=pipeline.id,  # Physical pipeline ID
        cluster=self.config.env,
        platform_instance=self.config.platform_instance,
    )

    # Store single pipeline DataFlow URN
    self.state.pipeline_dataflow_urn = dataflow_urn

    # Emit DataFlow with pipeline info
    yield from self.emit_aspects(
        entity_urn=dataflow_urn,
        aspects=[
            DataFlowInfoClass(
                name=f"Snowplow Pipeline: {pipeline.name}",
                description=self._build_pipeline_description(pipeline),
                customProperties={
                    "pipelineId": pipeline.id,
                    "pipelineName": pipeline.name,
                    "eventSpecCount": str(len(event_specs)),
                    ...
                },
            ),
            StatusClass(removed=False),
            container_aspect,
        ],
    )
```

#### 3.2.2 `pipeline_processor.py` - `_extract_enrichments()` Method

**Current Behavior:**

- Iterates through ALL event specs
- Creates enrichment DataJobs for EACH event spec
- Results in N × M DataJobs (N event specs × M enrichments)

**New Behavior:**

- Creates enrichment DataJobs ONCE for the physical pipeline
- Each DataJob attached to single pipeline DataFlow
- Results in M DataJobs (M enrichments)

```python
# BEFORE (current)
def _extract_enrichments(self) -> Iterable[MetadataWorkUnit]:
    for event_spec_id in self.state.emitted_event_spec_ids:
        event_spec_dataflow_urn = self.state.event_spec_dataflow_urns.get(event_spec_id)
        event_spec_urn = self.urn_factory.make_event_spec_dataset_urn(event_spec_id)

        for enrichment in enrichments:
            # Create enrichment for THIS event spec
            yield from self._emit_enrichment_datajob(
                enrichment=enrichment,
                dataflow_urn=event_spec_dataflow_urn,  # Per event spec
                input_dataset_urns=[event_spec_urn],   # Single event spec
                warehouse_table_urn=warehouse_table_urn,
            )

# AFTER (proposed)
def _extract_enrichments(self) -> Iterable[MetadataWorkUnit]:
    if not self.state.pipeline_dataflow_urn:
        return

    # Collect ALL event spec URNs as inputs
    all_event_spec_urns = [
        self.urn_factory.make_event_spec_dataset_urn(event_spec_id)
        for event_spec_id in self.state.emitted_event_spec_ids
    ]

    # Create enrichments ONCE for the pipeline
    for enrichment in enrichments:
        yield from self._emit_enrichment_datajob(
            enrichment=enrichment,
            dataflow_urn=self.state.pipeline_dataflow_urn,  # Single pipeline
            input_dataset_urns=all_event_spec_urns,         # ALL event specs
            warehouse_table_urn=warehouse_table_urn,
        )

    # Create single Loader job
    if warehouse_table_urn:
        yield from self._emit_loader_datajob(
            dataflow_urn=self.state.pipeline_dataflow_urn,
            input_dataset_urns=all_event_spec_urns,
            warehouse_table_urn=warehouse_table_urn,
        )
```

#### 3.2.3 `pipeline_processor.py` - `_emit_enrichment_datajob()` Method

**Changes:**

- Update DataJob URN generation (no event spec suffix needed)
- Keep fine-grained lineage extraction (unchanged)
- Update input datasets to reference atomic fields

```python
def _emit_enrichment_datajob(
    self,
    enrichment: Enrichment,
    dataflow_urn: str,
    input_dataset_urns: List[str],
    warehouse_table_urn: Optional[str],
) -> Iterable[MetadataWorkUnit]:
    # Create DataJob URN (no event spec suffix)
    datajob_urn = make_data_job_urn_with_flow(
        flow_urn=dataflow_urn,
        job_id=enrichment.id,  # Just enrichment ID, not per event spec
    )

    # Extract field-level lineage (use first event spec for field references)
    # All event specs have same atomic fields, so this is equivalent
    fine_grained_lineages = self._extract_enrichment_field_lineage(
        enrichment=enrichment,
        event_schema_urns=input_dataset_urns[:1] if input_dataset_urns else [],
        warehouse_table_urn=warehouse_table_urn,
    )

    # ... rest unchanged
```

#### 3.2.4 `dependencies.py` - `IngestionState` Updates

```python
@dataclass
class IngestionState:
    # ... existing fields ...

    # REMOVE (no longer needed):
    # event_spec_dataflow_urns: Dict[str, str] = field(default_factory=dict)

    # ADD (single pipeline tracking):
    pipeline_dataflow_urn: Optional[str] = None
```

#### 3.2.5 Optional: Add Collector DataJob

For completeness, add a Collector DataJob to represent the event collection stage:

```python
def _emit_collector_datajob(
    self,
    dataflow_urn: str,
    output_dataset_urns: List[str],
) -> Iterable[MetadataWorkUnit]:
    """Emit Collector DataJob that receives raw events."""
    datajob_urn = make_data_job_urn_with_flow(
        flow_urn=dataflow_urn,
        job_id="collector",
    )

    yield from self.emit_aspects(
        entity_urn=datajob_urn,
        aspects=[
            DataJobInfoClass(
                name="Collector",
                type="STREAMING",
                description=(
                    "Snowplow Collector that receives raw events from trackers.\n\n"
                    "**Input**: HTTP requests from web/mobile/server trackers\n"
                    "**Output**: Raw events to enrichment stage"
                ),
                customProperties={
                    "stage": "collector",
                    "endpoints": ", ".join(self._get_collector_endpoints()),
                },
            ),
            DataJobInputOutputClass(
                inputDatasets=[],  # External input
                outputDatasets=output_dataset_urns,
            ),
            StatusClass(removed=False),
        ],
    )
```

---

## 4. Field-Level Lineage Preservation

### 4.1 How Field Lineage Works (Unchanged)

The field-level lineage extraction remains unchanged. Enrichments operate on **atomic fields** which are identical across all event specs.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│           FIELD LINEAGE (PRESERVED IN OPTION A)                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  DataJob: IP Lookup Enrichment                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  fineGrainedLineages:                                                │   │
│  │                                                                       │   │
│  │  Input (any Event Spec)           Output (Warehouse Table)           │   │
│  │  ─────────────────────            ───────────────────────            │   │
│  │  user_ipaddress      ──────────▶  geo_country                        │   │
│  │                      ──────────▶  geo_city                           │   │
│  │                      ──────────▶  geo_region                         │   │
│  │                      ──────────▶  geo_latitude                       │   │
│  │                      ──────────▶  geo_longitude                      │   │
│  │                                                                       │   │
│  │  transformOperation: "IP_LOOKUP"                                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  This mapping is IDENTICAL for all event specs because:                     │
│  • user_ipaddress is an ATOMIC FIELD (present in all events)                │
│  • Enrichments process atomic fields, not custom schema fields              │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Why Duplication Was Unnecessary

| Field            | Source       | Present In      |
| ---------------- | ------------ | --------------- |
| `user_ipaddress` | Atomic Event | ALL event specs |
| `useragent`      | Atomic Event | ALL event specs |
| `page_url`       | Atomic Event | ALL event specs |
| `page_urlquery`  | Atomic Event | ALL event specs |

Since enrichments only read atomic fields (which are shared), the field lineage is identical across all event specs. Duplicating it N times adds no information.

---

## 5. Testing Strategy

### 5.1 Unit Tests to Update

| Test File                    | Changes Needed                           |
| ---------------------------- | ---------------------------------------- |
| `test_pipeline_processor.py` | Update to expect single DataFlow         |
| `test_pipeline_processor.py` | Update to expect non-duplicated DataJobs |
| `test_pipeline_processor.py` | Verify field lineage still extracted     |

### 5.2 New Test Cases

```python
class TestSinglePipelineArchitecture:
    """Tests for Option A: Single Physical Pipeline."""

    def test_single_dataflow_created(self):
        """Verify only ONE DataFlow is created per physical pipeline."""
        # Given: 10 event specs, 5 enrichments
        # When: Pipeline extraction runs
        # Then: 1 DataFlow created (not 10)
        pass

    def test_enrichments_not_duplicated(self):
        """Verify enrichment DataJobs are created once, not per event spec."""
        # Given: 10 event specs, 5 enrichments
        # When: Enrichment extraction runs
        # Then: 5 DataJobs created (not 50)
        pass

    def test_field_lineage_preserved(self):
        """Verify field-level lineage is still extracted correctly."""
        # Given: IP Lookup enrichment with geo database configured
        # When: Enrichment extraction runs
        # Then: fineGrainedLineages contains user_ipaddress → geo_* mappings
        pass

    def test_all_event_specs_as_inputs(self):
        """Verify Loader DataJob has all event specs as inputs."""
        # Given: 10 event specs
        # When: Loader job created
        # Then: inputDatasets contains all 10 event spec URNs
        pass
```

### 5.3 Integration Test Verification

After implementation, verify with real BDP:

1. DataFlow count matches physical pipeline count (typically 1)
2. DataJob count matches enrichment count + 1 (loader)
3. Field lineage visible in DataHub UI
4. No duplicate entities

---

## 6. Migration Considerations

### 6.1 Backward Compatibility

| Aspect             | Impact                        | Mitigation                                      |
| ------------------ | ----------------------------- | ----------------------------------------------- |
| **Existing URNs**  | DataFlow URNs will change     | Soft delete old entities via stateful ingestion |
| **Config options** | `extract_pipelines` unchanged | No config changes needed                        |
| **API calls**      | Same API calls                | No BDP API changes                              |

### 6.2 Entity Cleanup

The stateful ingestion handler will automatically soft-delete old DataFlow/DataJob entities that are no longer emitted:

```python
# Old URNs (will be soft-deleted):
urn:li:dataFlow:(snowplow,pipeline123_event_eventspec1,PROD)
urn:li:dataFlow:(snowplow,pipeline123_event_eventspec2,PROD)
...

# New URN (will be created):
urn:li:dataFlow:(snowplow,pipeline123,PROD)
```

---

## 7. Summary of Changes

### 7.1 Code Changes

| Component         | Before                          | After                              |
| ----------------- | ------------------------------- | ---------------------------------- |
| DataFlow creation | N DataFlows (per event spec)    | 1 DataFlow (per physical pipeline) |
| DataJob creation  | N × M DataJobs                  | M DataJobs                         |
| State tracking    | `event_spec_dataflow_urns` dict | `pipeline_dataflow_urn` string     |
| Enrichment inputs | Single event spec URN           | All event spec URNs                |

### 7.2 Entity Count Comparison

| Scenario                        | Current                      | Option A                |
| ------------------------------- | ---------------------------- | ----------------------- |
| 10 event specs, 5 enrichments   | 10 DataFlows, 60 DataJobs    | 1 DataFlow, 6 DataJobs  |
| 100 event specs, 10 enrichments | 100 DataFlows, 1100 DataJobs | 1 DataFlow, 11 DataJobs |
| 500 event specs, 15 enrichments | 500 DataFlows, 8000 DataJobs | 1 DataFlow, 16 DataJobs |

### 7.3 What Stays the Same

- ✅ Schema → Event Spec → Warehouse lineage (via UpstreamLineage)
- ✅ Field-level lineage extraction
- ✅ Ownership propagation (flows through Dataset lineage)
- ✅ Enrichment configuration parsing
- ✅ All other processors unchanged

---

## 8. Implementation Checklist

- [ ] Update `_extract_pipelines()` to create single DataFlow
- [ ] Update `_extract_enrichments()` to create DataJobs once
- [ ] Update `IngestionState` to track single pipeline URN
- [ ] Remove `event_spec_dataflow_urns` from state
- [ ] Add optional Collector DataJob
- [ ] Update `_emit_loader_datajob()` to use all event spec URNs
- [ ] Update unit tests
- [ ] Run integration tests with real BDP
- [ ] Verify in DataHub UI
- [ ] Document changes

---

## 9. Approval

**Author:** Claude Code
**Date:** 2026-01-19
**Status:** Pending Review

Please review this plan and provide feedback or approval to proceed with implementation.

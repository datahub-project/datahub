- Start Date: 2026-01-28
- RFC PR: (after opening the RFC PR, update this with a link to it and update the file name)
- Discussion Issue: (GitHub issue this was discussed in before the RFC, if any)
- Implementation PR(s): (leave this empty)

# Agents and Related Assets in Metadata Model

## Summary

This RFC proposes adding support for AI agents and related agentic assets to DataHub's metadata model. As organizations build agent platforms and deploy AI agents into production, they need a centralized catalog to discover, understand, and govern these assets - similar to how DataHub catalogs data assets today.

This includes modeling not just agents themselves, but the entire agentic ecosystem: AI agents, tools/MCPs (Model Context Protocol), vector stores, knowledge graphs, and their relationships to existing data assets.

## Basic example

```graphql
# Query an agent and its related assets
query {
  agent(urn: "urn:li:agent:customer-support-assistant") {
    name
    description
    status # ACTIVE, DORMANT, RESTRICTED, BETA
    goals
    owner {
      name
    }
    usesTools {
      name
      type # MCP, API, etc.
    }
    usesModels {
      name
      platform # bedrock, azure, openai
    }
    consumesDatasets {
      name
      platform # could be snowflake, pinecone, chroma, etc.
    }
    codeRepository {
      url
    }
    evaluationMetrics {
      goalAchievementRate
      sentimentScore
      errorRate
      timestamp
    }
    usageStats {
      conversationsPerWeek
      lastActiveDate
    }
  }
}
```

Example URN structure:

- Agent: `urn:li:agent:customer-support-assistant`
- Tool: `urn:li:agentTool:order-lookup-mcp`
- Vector Store Dataset: `urn:li:dataset:(urn:li:dataPlatform:pinecone,customer-kb,PROD)` (uses existing Dataset entity)

## Motivation

### The Problem

Organizations are building agent platforms and deploying AI agents into production, but face challenges:

1. **Discoverability**: Teams cannot easily find existing agents before building new ones, leading to duplication
2. **Lack of Trust**: Unlike data assets with quality scores, agents lack visible trust indicators (evaluation metrics, reliability scores, usage patterns)
3. **Fragmented Ecosystem**: Agents depend on multiple assets (tools, models, vector stores, data products) with no unified view of relationships
4. **Governance Gap**: No central place to understand what agents exist, who owns them, what data they access, and how they're performing
5. **Reusability Friction**: Power users want to fork/extend existing agents but can't easily discover implementation details or assess reliability

### Key Use Cases

**Productivity & Discovery**:

- Developer searches for "customer support agent" before building one from scratch
- Team discovers available MCP tools in the organization that any agent can use
- Agent autonomously discovers and registers tools during workflow building

**Trust & Evaluation**:

- Users assess agent reliability through goal achievement rates and evaluation scores
- Stakeholders track agent performance trends similar to data quality monitoring
- Teams understand if an agent is production-ready (active) or experimental (beta)

**Lineage & Relationships**:

- Understand which agents consume which data products
- Trace from a dataset to all agents that use it for RAG or tool calls
- See the complete dependency graph: Agent → Tools → Models → Vector Stores → Datasets

**Governance**:

- Track agent ownership and lifecycle (active, dormant, deprecated)
- Audit which agents have access to sensitive data
- Understand usage patterns and resource consumption

### Why DataHub?

Organizations already use DataHub as their data catalog. Since data and AI are tightly coupled (agents consume data products), maintaining separate catalogs creates:

- Fragmented user experience
- Broken lineage across data and AI boundaries
- Duplicate governance processes

DataHub's extensible metadata model makes it the natural home for agentic assets.

## Requirements

### Core Entity Requirements

1. **Agent Entity**:

   - Name, description, type (conversational, backend, autonomous)
   - Status (active, dormant, restricted, beta, production)
   - Goals/objectives the agent is designed to achieve
   - Owner and team information
   - Link to code repository (GitHub/BitBucket/GitLab URL)
   - Creation date, last modified date, last active date
   - Tags, glossary terms, documentation

2. **Agent Tool Entity** (MCP/tools):

   - Name, description, tool type (MCP, API, function)
   - Input/output schema or signature
   - Owner and documentation
   - Which agents use this tool (inverse relationship)
   - Auto-generated vs manually created
   - Tool registry reference (if applicable)

3. **Relationships**:

   - Agent → uses → Tool (many-to-many)
   - Agent → uses → ML Model (reference existing MLModel entity)
   - Agent → consumes → Dataset/Data Product (for RAG, context, tool data)
     - Includes vector store datasets (Pinecone indexes, Chroma collections, etc.)
   - Agent → backed by → Code Repository (potentially new entity or URL)
   - Tool → registered by → Agent (for auto-generated tools)

4. **Observability & Trust Metrics**:

   - Usage patterns (conversations/invocations, active users)
   - Evaluation scores (goal achievement, accuracy, sentiment)
   - Error rates and reliability indicators
   - Integration with OpenTelemetry traces and evaluation platforms

5. **Search & Discovery**:

   - Search across agent descriptions, goals, and capabilities
   - Filter by status, owner, evaluation scores, relationships
   - Agent profile pages showing dependencies, metrics, and lineage

6. **Extensibility**:
   - Framework agnostic (LangGraph, LangChain, custom frameworks)
   - Support for vector column types and vector database platforms
   - Support for future asset types (knowledge graphs, prompts)
   - Flexible ingestion (manifest files, API, OpenTelemetry, auto-discovery)

## Non-Requirements

The following are explicitly out of scope for this RFC (may be addressed in future work):

1. **Agent Execution**: DataHub will catalog agents but not execute them
2. **Prompt Management**: Detailed prompt versioning and templating (could be future extension)
3. **Real-time Debugging**: Deep trace debugging UI (basic trace linking only)
4. **Cost Tracking**: Detailed cost analysis per agent execution (basic metrics only)
5. **Access Control Enforcement**: DataHub documents permissions but doesn't enforce agent runtime access
6. **Agent Marketplace**: Public/private marketplaces for sharing agents (future possibility)
7. **Model Fine-tuning Tracking**: Detailed ML training lineage (use existing ML Model entity)
8. **Multi-agent Orchestration**: Workflow DAGs for multi-agent systems (future extension)
9. **Evaluation Framework**: DataHub ingests evaluation results but doesn't run evaluations
10. **Semantic Layer Integration**: While agents may use semantic layers, detailed semantic model cataloging is separate

## Detailed design

### Core Entities

#### 1. Agent Entity

An Agent represents an AI system that can perceive, reason, and act to achieve specific goals.

**URN Format**: `urn:li:agent:<agent_id>`

**Core Properties**:

- Name, description, type (conversational, backend, autonomous, hybrid)
- Goals/objectives the agent is designed to achieve
- Status (active, dormant, beta, production, deprecated)
- Owner and documentation
- Link to code repository

**Key Relationships**:

- **Uses Tools**: Agent → AgentTool (which tools can this agent invoke?)
- **Uses Models**: Agent → MLModel (which LLMs does this agent use?)
- **Consumes Data**: Agent → Dataset (which data does this agent access for RAG, context, tools?)
  - This includes vector store datasets (Pinecone, Chroma, Weaviate indexes)

**Observability & Trust**:

- Usage metrics (invocations, conversations, active users)
- Evaluation metrics (goal achievement, accuracy, sentiment scores)
- Error rates and reliability indicators

**Open Questions**:

- What metadata is most important for discoverability?
- How should evaluation criteria be defined (standard vs agent-specific)?
- Should we track individual conversations or just aggregates?

#### 2. AgentTool Entity

An AgentTool represents a capability that agents can invoke (MCP tools, APIs, functions).

**URN Format**: `urn:li:agentTool:<tool_id>`

**Core Properties**:

- Name, description, tool type (MCP, REST API, function, custom)
- Input/output schema (for discoverability and validation)
- Whether auto-generated by an agent
- Owner and documentation

**Key Relationships**:

- **Used By Agents**: AgentTool → Agent (which agents use this tool?)
- **Registered By**: AgentTool → Agent (if auto-generated)

**Open Questions**:

- Should tools be versioned explicitly?
- How to handle tool schema evolution?
- Should we catalog tool performance metrics separately?

#### 3. Vector Store Support

Vector stores (Pinecone, Chroma, Weaviate, Qdrant, etc.) are modeled as **DataPlatforms** in DataHub's existing architecture:

- Vector store = DataPlatform (like Snowflake, Postgres)
- Index/Collection = Dataset (like tables)
- Fields = Schema columns, with support for **vector column types**

**What's needed**:

- Add "vector" as a column data type in schema definitions
- Create platform integrations for common vector databases:
  - Pinecone
  - Chroma
  - Weaviate
  - Qdrant
  - pgvector (Postgres extension)
  - OpenSearch (vector fields)

Agents consume vector store datasets via the same `Agent → consumes → Dataset` relationship used for other data sources.

### Ingestion & Integration

**Manifest-based Ingestion**:

```yaml
agents:
  - id: customer-support-assistant
    name: Customer Support Assistant
    type: CONVERSATIONAL
    goals:
      - "Resolve customer inquiries about orders"
    tools: [order-lookup-mcp, shipping-tracker-api]
    models: [urn:li:mlModel:(...)]
    datasets: [urn:li:dataset:(...)]

tools:
  - id: order-lookup-mcp
    name: Order Lookup MCP
    type: MCP
    description: "Looks up order details by order ID"
```

**Observability Integration**:

- Agents emit OpenTelemetry traces with agent URN
- Evaluation platforms push metrics via Kafka/API
- Usage statistics aggregated from traces

**Open Questions**:

- What's the preferred ingestion method (manifest files, API, auto-discovery)?
- How should evaluation metrics flow into DataHub?
- Should we support code repository scanning for auto-discovery?

### Key Concepts

**Agent Card**: Similar to "dataset card" - a profile page showing:

- Agent description and goals
- Trust indicators (evaluation scores, usage patterns)
- Dependency graph (tools, models, data)
- Lineage (data → agent → actions)

**Trust Metrics**: Like data quality but for agents:

- Evaluation scores (goal achievement, accuracy)
- Usage patterns (conversations/week, error rates)
- Historical trends to build confidence

**Tool Discovery**: Both humans and agents should be able to:

- Search for existing tools before building new ones
- Understand tool capabilities via schemas
- See which agents use which tools

## How we teach this

**Key metaphor**: "Agents are to AI what Datasets are to Data"

Agents will be entities in DataHub, just like Datasets, Dashboards, or ML Models. They'll use the same patterns for ownership, tagging, glossary terms, documentation, lineage, and discovery.

**Terminology**:

- **Agent**: An AI system that can perceive, reason, and act to achieve goals
- **Agent Tool**: A capability that an agent can invoke (MCP, API, function)
- **Agent Card**: The profile page displaying agent metadata
- **Evaluation Metrics**: Measurements of agent performance (similar to data quality metrics)

## Drawbacks

1. **Scope Expansion**: DataHub becomes "catalog for everything" rather than focused data catalog
2. **Complexity**: New entity types add learning curve and maintenance burden
3. **Fast-Moving Landscape**: AI patterns evolve rapidly, design may need frequent updates
4. **Integration Burden**: Organizations must integrate agent platforms with DataHub
5. **Time Series Storage**: Usage and evaluation metrics create large time series datasets

## Alternatives

### Alternative 1: Extend MLModel Entity

Model agents as a subtype of MLModel. **Rejected** - agents are applications/services, not models.

### Alternative 2: Extend Service Entity

Model agents as Services with AI aspects. **Rejected** - Service entity doesn't capture AI-specific semantics.

### Alternative 3: Separate Agent Catalog

Build standalone catalog, integrate via lineage API. **Rejected** - users explicitly want unified catalog.

### Alternative 4: Minimal Agent Entity + External Storage

Lightweight catalog, detailed metrics external. **Partially adopted** - store aggregated metrics in DataHub, link to external traces.

### Prior Art

- **MLflow Model Registry**: Model cards, lineage, versioning
- **Backstage Service Catalog**: Unified catalog for services, APIs, docs with typed relationships
- **IBM Agent Platform**: Agent cards showing dependencies, tool catalog, evaluation metrics

## Rollout / Adoption Strategy

**Breaking Changes**: None. This is purely additive (new entities, new APIs).

**Adoption Path**:

1. Create manifest files for agents
2. Ingest using DataHub CLI or API
3. View agents in DataHub UI
4. Optionally enable live metrics via OpenTelemetry

**Backward Compatibility**: All existing entities, aspects, and APIs unchanged.

## Future Work

This section outlines potential extensions that build on the agent catalog foundation:

1. **Multi-Agent Systems**: Model agent orchestration and workflows (AgentWorkflow DAG entity)
2. **Prompt Template Registry**: Catalog reusable prompts and their usage
3. **Agent Marketplace**: Public/private marketplace for sharing agents
4. **Fine-tuned Model Tracking**: Deep integration with ML model lifecycle
5. **Cost Tracking**: Token usage, compute costs, ROI metrics
6. **Advanced Evaluation**: Custom criteria builder, A/B testing, benchmark datasets
7. **Semantic Search**: Federated search across agent knowledge bases
8. **Access Control**: Policy enforcement and access auditing
9. **Performance Benchmarking**: Compare agents on standardized benchmarks
10. **Conversational Context**: Catalog conversation histories
11. **Agent Version Management**: Track versions, rollouts, and rollbacks
12. **Health Monitoring**: Proactive alerts on score drops, error spikes
13. **Knowledge Graph Entity**: Model knowledge graphs separately from vector stores
14. **Agent-Generated Artifacts**: Catalog outputs created by agents
15. **Federation**: Sync with external agent registries if they emerge

## Unresolved questions

### 1. Knowledge Graph Modeling

**Question**: How to model knowledge graphs used by agents?

**Options**:

- **A**: New entity `KnowledgeGraph`
- **B**: Special Dataset subtype (like vector stores)
- **C**: Collection of Datasets with special relationship types
- **D**: Out of scope for initial release

**Recommendation**: **Option D** (defer to future work), revisit if strong user need emerges.

---

### 2. Service User Entity for Agent Identity

**Question**: Should agents automatically create/link to Service User entities?

**Context**: DataHub recently added service users for non-human identities. Agents are non-human.

**Options**:

- **A**: Automatically create Service User when Agent is created
- **B**: Optional relationship: Agent → runs as → Service User
- **C**: Separate concern, handle in access control layer

**Recommendation**: **Option B** (optional relationship), discuss with identity team.

---

### 3. Conversation Entity

**Question**: Should individual conversations be entities in DataHub?

**Use Cases**: Debug specific conversation, link to evaluation results, build datasets

**Concerns**: High cardinality (millions), short-lived, may belong in observability platform

**Recommendation**: Link to external trace viewer initially, revisit if strong use case emerges.

---

### 4. Agent Version Management

**Question**: How to handle agent versions?

**Options**:

- **A**: Version in URN: `urn:li:agent:customer-support:v1.2.3`
- **B**: Version as aspect: `AgentVersion` with semantic version
- **C**: Link to git commit hash only (code repository)
- **D**: No explicit versions, rely on git history

**Recommendation**: **Option C** + **D** initially (git commit hash), add versioning later if needed.

---

### 5. Evaluation Criteria Standardization

**Question**: Should DataHub define standard evaluation metrics?

**Options**:

- **A**: Define enum of standard metrics (GOAL_ACHIEVEMENT, ACCURACY, etc.)
- **B**: Freeform metric names (strings), maintain registry/catalog
- **C**: Hybrid: suggest standards but allow custom

**Recommendation**: **Option C** (hybrid). Provide suggested metric names, allow custom via `customProperties`.

---

### 6. Tool Schema Validation

**Question**: Should DataHub validate tool input/output schemas?

**Options**:

- **A**: Validate schemas are valid JSON Schema on ingestion
- **B**: Store as opaque strings, no validation
- **C**: Validate and index for searchability

**Recommendation**: **Option A** initially (validate format), **Option C** as future enhancement.

---

### 7. OTEL Semantic Conventions Alignment

**Question**: Should we adopt OTEL semantic conventions for GenAI when they stabilize?

**Context**: OTEL community developing GenAI conventions (trace attributes for LLM calls)

**Recommendation**: Monitor OTEL GenAI conventions, adopt when stable.

---

### 8. Agent Type Taxonomy

**Question**: What values should `AgentType` enum have?

**Proposed**:

- CONVERSATIONAL (chatbot-style)
- BACKEND (autonomous tasks, no user interaction)
- AUTONOMOUS (fully autonomous decision-making)
- HYBRID (mixed mode)

**Alternatives**:

- Task-based: CUSTOMER_SUPPORT, DATA_ANALYSIS, CODE_GENERATION
- Architecture-based: REACTIVE, PROACTIVE, DELIBERATIVE

**Seeking Feedback**: Which taxonomy is most useful for discovery and filtering?

---

### 9. Time Series Aspect Granularity

**Question**: At what granularity should time series metrics be stored?

**Options**:

- Raw: Every execution (high storage cost)
- Aggregated: Hourly, daily, weekly
- Hybrid: Raw for recent (7 days), aggregated for historical

**Recommendation**: **Hybrid approach** - 7 days raw, 90 days hourly, 2 years daily

---

**Open for Community Feedback**: Please comment on these unresolved questions in the RFC PR!

# AI Agent

An AI Agent is a first-class catalog entity representing an AI agent — a system built on one or more
LLMs that uses tools and skills to accomplish tasks (LangChain agents, Google ADK agents, Snowflake
Cortex agents, DataHub's own "Ask DataHub" assistants, etc.). Cataloging agents lets teams discover
what agents exist, understand what they can do and what they depend on, govern them, and track their
lineage into the data they touch.

Agents come in three flavors, distinguished by the `source.type` of the `aiAgentInfo` aspect:

- **System** (`SYSTEM`): bootstrapped at deployment time (e.g. via YAML) and not editable by end users.
- **Native** (`NATIVE`): created and managed on DataHub by end users through the UI or API.
- **External** (`EXTERNAL`): managed outside DataHub but cataloged here for discovery and governance.

This is an origin/governance axis, not a functional one — a single-agent, multi-agent, or copilot
distinction (if needed) is layered on with the standard `subTypes` aspect.

## Identity

AI Agents are identified by a single field:

- **id**: A unique identifier for the agent, typically a human-readable slug such as `ask-datahub`
  or a generated UUID.

An example URN is `urn:li:aiAgent:ask-datahub`. The identifier is platform-agnostic; the platform, if
any, is carried on the `dataPlatformInstance` aspect rather than in the key.

## Important Capabilities

### Agent Info

The core properties live on the `aiAgentInfo` aspect:

- **name**: Display name, searchable with autocomplete.
- **tagline**: A short (~120 char) one-line summary shown on agent cards, hero subtitles, and picker
  dropdowns. When absent, surfaces fall back to truncating the description.
- **description**: What the agent does **and when to route to it**. The recommended style is
  "X does Y. Use when Z." — for router-style agents (like Ask DataHub) this text is read verbatim when
  deciding whether to delegate, so the "Use when…" tail is load-bearing rather than decorative.
- **instructions**: Custom base instructions injected into the agent's system prompt, beyond the
  standard boilerplate.
- **source**: The origin of the agent definition (see below).
- **created** / **lastModified**: Audit stamps.

### Source and Agent Families

The `source` field (an `AIAgentSource`) records the agent's origin `type` (`SYSTEM` / `NATIVE` /
`EXTERNAL`) and, optionally, a `clonedFrom` reference to the agent this one was cloned from
(the `ClonedFromAgent` relationship). Clones inherit their base's configuration verbatim — one
frontend module, one tool palette, one set of instructions — and differ only in the artifacts they
manage. Grouping a base agent and its clones this way forms an **Agent Family**.

### Dependencies

The `aiAgentDependencies` aspect captures what the agent relies on, as typed relationships to other
first-class entities:

- **skills** → [Agent Skill](agentSkill.md) entities the agent adopts (`AgentHasSkill`).
- **tools** → [API](api.md) entities the agent invokes (`AgentUsesTool`). This is a lineage edge: the
  tools are upstream of the agent, contributing to the `repo → service → api → agent` chain.
- **models** → `mlModel` entities (the LLMs) the agent runs on (`AgentUsesModel`).

### Versioning, Governance, and Health

AI Agents support the standard governance aspects — ownership, tags, glossary terms, domains,
structured properties, and institutional memory — as well as native versioning (`versionProperties`,
so successive revisions of an agent group into a version set) and health via the shared incidents
subsystem (`incidentsSummary`). Agents can also participate in lineage through `upstreamLineage`.

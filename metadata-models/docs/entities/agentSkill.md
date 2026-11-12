# Agent Skill

An Agent Skill is a high-level, reusable capability bundle that AI agents adopt — specialized
prompts/instructions plus the tools needed to apply them. Skills sit one level above tools: where a
tool is a single callable, a skill packages domain expertise (the "how" and "when") together with the
low-level tools it relies on. The same skill can be adopted by many agents, so modeling it as its own
entity avoids duplicating that expertise on every agent.

Skills follow a "git as source of truth" pattern (e.g. the agentskills.io standard): the skill is
defined in a repository and cataloged in DataHub for discovery, governance, and reuse.

## Identity

Agent Skills are identified by a single field:

- **id**: A unique identifier for the skill, typically a human-readable slug such as
  `customer-service-skill` or a generated UUID.

An example URN is `urn:li:agentSkill:customer-service-skill`.

## Important Capabilities

### Skill Info

The `agentSkillInfo` aspect holds the skill's definition:

- **name**: Display name, searchable with autocomplete.
- **description**: What the skill does and when to use it.
- **instructions**: The markdown body of the skill's `SKILL.md` file (excluding YAML frontmatter) —
  the execution guidance loaded when the skill is activated.
- **sourceRepository**: Where the skill is defined (see below).
- **requiredTools**: The [API](./api.md) entities the skill needs to operate (`SkillRequiresTool`).
- **created** / **lastModified**: Audit stamps.

### Source Repository

The `sourceRepository` field (a `SkillSourceRepository`) points to the git location that owns the
skill definition. It carries either a `repositoryUrn` (preferred, when the [Repository](./repository.md)
is already cataloged in DataHub) or an external `url`, plus a `path` to the definition file within the
repository (e.g. `customer-service/SKILL.md`).

### Relationship to Agents

Agents adopt skills via the `AgentHasSkill` relationship declared on the agent's
`aiAgentDependencies` aspect — so an Agent Skill profile surfaces the agents that use it, and a skill
in turn exposes the tools it requires. This makes the agent → skill → tool chain traversable from
either end.

### Governance and Versioning

Agent Skills support the standard governance aspects — ownership, tags, glossary terms, domains,
structured properties, and institutional memory — plus native versioning (`versionProperties`).

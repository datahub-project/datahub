# Repository

A Repository is a first-class representation of a source-code repository (GitHub, GitLab, an internal
SCM, etc.). It is the **genesis node of the software-to-data lifecycle** —
`repo → service → api → app → dataset` — and produces the [Services](service.md), [APIs](api.md), and
[Applications](application.md) cataloged elsewhere. A Repository is not a tabular asset: it has no
columns, queries, statistics, quality, or preview.

## Identity

Repositories are identified by a single field:

- **id**: A unique identifier for the repository. By convention this is `<platform>.<org>/<name>`
  (e.g. `github.acme/payments`, `gitlab.acme/data-platform`); native or unknown-platform repos may
  omit the platform prefix.

An example URN is `urn:li:repository:github.acme/payments`. The key itself is platform-agnostic — the
platform is carried on the `dataPlatformInstance` aspect rather than in the key.

## Important Capabilities

### Repository Properties

The `repositoryProperties` aspect holds the descriptive metadata:

- **name**: Display name, searchable with autocomplete.
- **description**: What the repository contains.
- **defaultBranch**: The default branch (e.g. `main`, `master`).
- **languages**: The programming languages used, conventionally ordered most-prevalent first
  (repositories are frequently polyglot, so this is a list).
- **license**: The repository's license (e.g. `Apache-2.0`, `MIT`).
- **homepageUrl**: A link to the repository's homepage or documentation.
- **archived**: Whether the repository is archived (read-only / no longer maintained).
- **created** / **lastModified**: Audit stamps.

### Source Binding

The `repositorySource` aspect records where the repository lives and how it is identified in its
source system: the `externalUrl` (web or clone URL) and the `externalId` (e.g. a GitHub numeric repo
id or an `org/name` slug).

### Lineage

The `repositoryLineage` aspect records provenance between repositories — currently `forkOf`, the
repository this one is a git fork of (`RepositoryForkOf`).

Beyond fork lineage, a Repository is the upstream anchor of the software-to-data chain: the Services
and APIs it produces point back to it via their `SourcedFrom` edges, and [Agent Skills](agentSkill.md)
may name it as their source of truth. A Repository profile therefore surfaces "what it produces" as
incoming `SourcedFrom` relationships.

### Governance

Repositories support the standard governance aspects — ownership, tags, glossary terms, domains,
structured properties, and institutional memory — plus browse paths (`browsePathsV2`) and the standard
`subTypes` aspect (e.g. `GIT_REPOSITORY`).

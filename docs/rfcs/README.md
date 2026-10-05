# DataHub RFCs

This directory contains Request for Comments (RFC) documents for substantial changes to DataHub.

## What is an RFC?

RFCs are design documents that propose significant changes to DataHub. See [the RFC process documentation](../rfc.md) for full details on when and how to submit an RFC.

## Fresh Start (2025)

**Note:** RFCs have moved back to the main DataHub repository (from the separate `datahub-project/rfcs` repo) to improve visibility and reduce contributor friction.

This represents a fresh start for the RFC process. Historical RFCs from before 2025 remain accessible in:

- Git history (commit `dbb4c84cb2` and earlier)
- Archived external repository: https://github.com/datahub-project/rfcs

Historical RFCs will be migrated to this directory on an as-needed basis when they become relevant for discussion or implementation.

## Directory Structure

- **[active/](./active/)** - RFCs currently under discussion or implementation
- **[accepted/](./accepted/)** - RFCs that have been implemented and shipped
- **template.md** - Template for creating new RFCs (available in the repository)

## Active RFCs

- [OpenLineage REST endpoint specification compliance](./active/17034-openlineage-spec-compliance.md) — [RFC PR #17034](https://github.com/datahub-project/datahub/pull/17034); separate implementation in [#19257](https://github.com/datahub-project/datahub/pull/19257).

See the [pull requests with the "RFC" label](https://github.com/datahub-project/datahub/pulls?q=is%3Apr+is%3Aopen+label%3ARFC) for other pending proposals.

## Accepted RFCs

No RFCs have been accepted in this new structure yet.

## Contributing an RFC

1. Read the [RFC process documentation](../rfc.md)
2. Copy `template.md` to `active/000-my-feature.md`
3. Fill in the template with your proposal
4. Submit a pull request with the "RFC" label
5. Gather feedback and iterate on the design

For questions or early feedback, post in the #contribute-code channel on [DataHub Slack](../slack.md).

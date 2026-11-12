---
title: CLI profile upgrade notes
slug: /cli-profile-upgrade
---

# CLI profile upgrade notes

### CLI initialization from scripts

When you run `datahub init` without an interactive terminal, it now stops if an existing
connection points to a different server ([PR #21214](https://github.com/datahub-project/datahub/pull/21214)). This protects your saved connection from being changed
by another script or agent. To connect to another instance, use a separate name, for example
`datahub init --profile acme-staging --host https://acme-staging.example.com/gms --token "<token>"`.
To intentionally replace an existing connection, add `--force`. Refreshing a token for the same
server continues to work. Existing connection files and environment variables keep working.

See [Breaking Changes by Version](breaking-changes.md) for other upgrade requirements.

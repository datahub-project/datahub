# Ingestion Security Comparison

DataHub supports three ways to ingest metadata. They differ primarily in where credentials are stored and what network access is required.

## Quick Comparison

|                       | **Credentials**                          | **Runs From**                                                 | **Network**                                  | **Firewall/VPN**                 |
| --------------------- | ---------------------------------------- | ------------------------------------------------------------- | -------------------------------------------- | -------------------------------- |
| **UI Ingestion**      | Encrypted in DataHub                     | DataHub's infrastructure                                      | DataHub → sources                            | Required for private sources     |
| **CLI Ingestion**     | Local files/env vars                     | Wherever you execute it (often personal machine, CI/CD, etc.) | CLI → DataHub + sources                      | Depends on where CLI runs        |
| **Remote Executor** ☁️ | Your infrastructure (AWS Secrets, K8s, etc.) | Deployed in your infrastructure (K8s, ECS, etc.)              | Executor → DataHub + sources (outbound only) | Not required                     |

## Where Credentials Live

UI ingestion stores credentials in DataHub. CLI and Remote Executor both run in your infrastructure, but differ in how credentials are managed:

- **CLI**: Uses recipe files with credentials (typically environment variables or local secret managers)
- **Remote Executor**: Integrates with enterprise secret managers (AWS Secrets Manager, External Secrets Operator, etc.)

## Network Patterns

- **UI**: DataHub reaches out to sources from DataHub's infrastructure.
- **CLI**: Can run from anywhere (personal machine, CI/CD, cloud instance). Wherever it runs needs access to both DataHub and sources.
- **Remote Executor**: Deployed in your infrastructure with access to DataHub and sources. Only makes outbound connections (no inbound ports or VPN needed).

## When to Use What

Most organizations use a mix:

- Sensitive production sources behind firewalls → Remote Executor
- Cloud SaaS tools (Looker, Tableau) → UI ingestion
- CI/CD pipelines, dev/test → CLI ingestion

Main differentiator: UI runs from DataHub's infrastructure, CLI and Remote Executor run in yours.

## More Info

- [UI Ingestion Guide](ui-ingestion.md)
- [CLI Installation](cli.md#installation)
- [Remote Executor Overview](managed-datahub/remote-executor/about.md)
- [Scheduling CLI Ingestion with Airflow](https://datahubproject.io/docs/metadata-ingestion/schedule_docs/airflow)
- [Personal Access Tokens](authentication/personal-access-tokens.md)

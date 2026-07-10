## Overview

[Monte Carlo](https://www.montecarlodata.com/) is a data observability platform that monitors warehouse and lake tables for freshness, volume, schema and field-quality issues and raises alerts/incidents when they breach.

This connector ingests Monte Carlo **monitors**, **custom (SQL) rules** and **alerts/incidents** and models them as DataHub **Assertions**, so the native "Validation" tab on a dataset reflects Monte Carlo's observability coverage and incident history.

## Concept Mapping

| Monte Carlo Concept    | DataHub Concept                                                                           | Notes                                                                     |
| ---------------------- | ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| `"montecarlo"`         | [Data Platform](https://docs.datahub.com/docs/generated/metamodel/entities/dataplatform/) |                                                                           |
| Monitor                | [Assertion](https://docs.datahub.com/docs/generated/metamodel/entities/assertion/)        | One `CUSTOM` assertion per monitor; native type kept in custom props.     |
| Custom (SQL) rule      | [Assertion](https://docs.datahub.com/docs/generated/metamodel/entities/assertion/)        | One `CUSTOM` assertion per rule; SQL captured in `customAssertion.logic`. |
| Monitored asset (MCON) | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset/)            | Resolved via `getTable` and `connection_to_platform_map`.                 |
| Alert / Incident       | Assertion Run Event                                                                       | Emitted as an `AssertionRunEvent` failure on the corresponding assertion. |

Every monitor/rule is modeled as a `CUSTOM` assertion (matching the established connector pattern, e.g. Snowflake DMFs), with the Monte Carlo native type, resource id and data-quality dimension round-tripped via `customProperties`.

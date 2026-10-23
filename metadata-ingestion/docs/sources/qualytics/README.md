## Overview

[Qualytics](https://www.qualytics.io/) is a data quality platform. It profiles connected
datastores, infers and enforces quality checks against them, and records the anomalies
those checks produce.

This connector brings that signal onto the datasets your DataHub already catalogues
from Snowflake, BigQuery, Databricks, S3 and similar. Quality checks become
**assertions**, anomalies and check results become **assertion run events**, and
Qualytics profiles become **dataset and field profiles**, all attached to **those
existing datasets** by reconstructing each dataset URN from the Qualytics datastore's
connection metadata. It creates no parallel Qualytics catalog and emits no schemas,
which the warehouse source owns. Stateful ingestion removes assertions whose checks
were deleted in Qualytics.

## Concept Mapping

| Qualytics Concept             | DataHub Concept                                                                           | Notes                                                                                                                                                                                                                   |
| ----------------------------- | ----------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Datastore                     | —                                                                                         | Not emitted. It is the key that resolves each container onto the platform DataHub already catalogues it under.                                                                                                          |
| Container (table, view, file) | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset/)            | Resolved to the _underlying_ platform's dataset URN, not a Qualytics one.                                                                                                                                               |
| Quality check                 | [Assertion](https://docs.datahub.com/docs/generated/metamodel/entities/assertion/)        | One `CUSTOM` assertion per check, with the Qualytics rule type in `nativeType` and its configuration in `nativeParameters`. All 49 rule types are mapped; see below.                                                    |
| Check result (`is_passing`)   | Assertion Run Event                                                                       | The current verdict, and the only source of _passes_ — Qualytics records anomalies, not successes.                                                                                                                      |
| Anomaly                       | Assertion Run Event                                                                       | A failure, with Qualytics' message and the count of offending records in `unexpectedCount`. Windowed; 30 days by default.                                                                                               |
| Container profile             | Dataset Profile                                                                           | Row count from the latest profile operation.                                                                                                                                                                            |
| Field profile                 | Dataset Field Profile                                                                     | Min, max, mean, median, quartiles, standard deviation, distinct and null counts, value histogram.                                                                                                                       |
| Field                         | —                                                                                         | No `schemaMetadata` is emitted; see Limitations. Field-level signal reaches DataHub through field profiles and the `schemaField` URNs on assertions.                                                                    |
| Global tag                    | —                                                                                         | Not emitted yet. `globalTags` replaces the whole aspect, so writing ours onto a dataset the warehouse source owns would wipe its other tags. Likely to arrive as tags on the assertions, which this connector does own. |
| `"qualytics"`                 | [Data Platform](https://docs.datahub.com/docs/generated/metamodel/entities/dataplatform/) | The platform on every assertion's `dataPlatformInstance`, with this deployment's `platform_instance`. No `qualytics` datasets are emitted.                                                                              |

### Rule type coverage

All 49 Qualytics rule types map to DataHub's assertion vocabulary — scope, operator and
aggregation — grouped by what they assert:

| Group                         | Rule types                                                                                                                          |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Null and emptiness            | `notNull`, `anyNotNull`, `notEmpty`                                                                                                 |
| Uniqueness                    | `unique`, `distinctCount`                                                                                                           |
| Numeric range and aggregate   | `between`, `minValue`, `maxValue`, `greaterThan`, `lessThan`, `positive`, `notNegative`, `sum`                                      |
| Pattern and PII               | `matchesPattern`, `containsCreditCard`, `containsEmail`, `containsUrl`, `containsSocialSecurityNumber`, `isCreditCard`, `isAddress` |
| Set membership                | `expectedValues`, `requiredValues`, `existsIn`, `notExistsIn`                                                                       |
| String length                 | `minLength`, `maxLength`                                                                                                            |
| Temporal                      | `afterDateTime`, `beforeDateTime`, `betweenTimes`, `notFuture`                                                                      |
| Volume                        | `volumetric`, `minPartitionSize`, `maxPartitionSize`, `fieldCount`                                                                  |
| Freshness                     | `freshness`, `timeDistributionSize`                                                                                                 |
| Schema                        | `expectedSchema`, `isType`                                                                                                          |
| Expression and metric         | `satisfiesExpression`, `aggregationComparison`, `metric`                                                                            |
| Field comparison              | `equalTo`, `equalToField`, `greaterThanField`, `lessThanField`                                                                      |
| Cross-dataset and inferential | `isReplicaOf`, `dataDiff`, `entityResolution`, `predictedBy`                                                                        |

Some Qualytics semantics have no DataHub equivalent — there is no standard operator for
"every listed value must appear" (`requiredValues`) or for a cross-dataset diff. Those
use DataHub's `_NATIVE_` sentinel, so the UI renders Qualytics' own description rather
than a misleading standard operator. **Nothing is ever dropped**: a rule type newer than
your connector build is still emitted as a custom assertion with its configuration
intact, and reported under `unmapped_rule_types`.

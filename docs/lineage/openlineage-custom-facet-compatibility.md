# OpenLineage custom-facet compatibility

## OpenLineage requirements

OpenLineage permits custom facets through the facet maps' `additionalProperties`. Every facet must contain URI-valued `_producer` and `_schemaURL` fields. The core JSON Schema therefore accepts a custom run facet whose `_schemaURL` points to the generic `RunFacet` definition, because `RunFacet` extends `BaseFacet` and permits additional properties.

Custom-facet conformance has additional semantic requirements that the core JSON Schema cannot enforce:

- The facet must use a distinct project prefix.
- `_schemaURL` must point to the corresponding version of the custom facet schema.
- The URL must be canonical, versioned, and immutable.
- Custom facet names should follow `{prefix}{name}{entity}Facet`; attached keys should follow `{prefix}_{name}`.

Consequently, a payload can validate against the core event schema while still not be a properly published, schema-backed custom facet.

Primary sources:

- [OpenLineage 1.45.0 custom-facet rules](https://github.com/OpenLineage/OpenLineage/blob/1.45.0/spec/OpenLineage.md#facets)
- [OpenLineage 1.45.0 `BaseFacet` and facet maps](https://github.com/OpenLineage/OpenLineage/blob/1.45.0/spec/OpenLineage.json)
- [OpenLineage versioning rules](https://github.com/OpenLineage/OpenLineage/blob/1.45.0/spec/Versioning.md)

## Facets handled by DataHub

| Key                      | Attachment | Producer status                                                                                                     | Emitted schema identity                                                                                                                                                                    | Assessment                                                                                                                                                         |
| ------------------------ | ---------- | ------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `airflow`                | Run        | Emitted by the Apache Airflow OpenLineage provider as `AirflowRunFacet`                                             | Inherits the Python client's generic `RunFacet` schema URL. Airflow contains a dedicated `AirflowRunFacet.json`, but the class does not override `_get_schema()` to reference it.          | Structurally accepted; not properly schema-backed as emitted. The key uses the project name but does not follow the recommended `{prefix}_{name}` form.            |
| `spark_jobDetails`       | Run        | Emitted by OpenLineage Spark 1.33.0 and 1.45.0                                                                      | `SparkJobDetailsFacet` extends Java `DefaultRunFacet`, whose `_schemaURL` is the generic `RunFacet` definition. No corresponding dedicated schema is present in the Spark facet resources. | Structurally accepted and project-prefixed; not properly schema-backed as emitted.                                                                                 |
| `spark_version`          | Run        | Historical OpenLineage Spark facet; deprecated in 1.2.0 and removed in 1.4.0 in favor of `ProcessingEngineRunFacet` | Extended `DefaultRunFacet`, so emitted the generic `RunFacet` schema URL. A standalone `version-facet.json` existed but was not referenced by the emitted facet.                           | Legacy, structurally accepted, and not properly schema-backed as emitted. Not emitted by OpenLineage Spark 1.33.0 or 1.45.0.                                       |
| `spark_properties`       | Run        | Emitted by OpenLineage Spark 1.33.0 and 1.45.0                                                                      | `SparkPropertyFacet` extends `DefaultRunFacet`, so it emits the generic `RunFacet` schema URL. No corresponding dedicated schema is present in the Spark facet resources.                  | Structurally accepted and project-prefixed; not properly schema-backed as emitted.                                                                                 |
| `spark.logicalPlan`      | Run        | Emitted by OpenLineage Spark 1.33.0 and 1.45.0                                                                      | `LogicalPlanFacet` extends `DefaultRunFacet`, so it emits the generic `RunFacet` schema URL. A Spark logical-plan schema exists, but the emitted facet does not reference it.              | Structurally accepted; not properly schema-backed as emitted. The dot-separated key deviates from the recommended `{prefix}_{name}` form.                          |
| `unknownSourceAttribute` | Run        | Deprecated Airflow compatibility facet; current Airflow directs consumers to `AirflowRunFacet` instead              | The Airflow class inherits the generic Python `RunFacet` schema URL. DataHub Airflow fixtures from OpenLineage 1.18.0–1.39.0 show generic `BaseFacet` URLs. No dedicated schema was found. | Legacy and not properly schema-backed. The key also lacks the required distinct project prefix.                                                                    |
| `processing_engine`      | Run        | Official `ProcessingEngineRunFacet`                                                                                 | Dedicated, versioned OpenLineage facet schema; generated as a typed field in OpenLineage Java 1.45.0.                                                                                      | Standard, spec-compliant facet. It is not a custom facet, and DataHub reads it through the generated typed field rather than the historical compatibility catalog. |

## Primary producer evidence

### OpenLineage Spark

OpenLineage Spark 1.33.0 and 1.45.0 emit `spark_jobDetails`, `spark_properties`, and `spark.logicalPlan` from custom facet builders. Their facet classes extend `OpenLineage.DefaultRunFacet`. In OpenLineage Java 1.33.0 and 1.45.0, that class fixes `_schemaURL` to:

```text
https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet
```

Sources:

- [Spark 1.45.0 custom facet builders](https://github.com/OpenLineage/OpenLineage/tree/1.45.0/integration/spark/shared/src/main/java/io/openlineage/spark/agent/facets/builder)
- [Spark 1.45.0 custom facet classes](https://github.com/OpenLineage/OpenLineage/tree/1.45.0/integration/spark/shared/src/main/java/io/openlineage/spark/agent/facets)
- [Spark 1.45.0 custom schema resources](https://github.com/OpenLineage/OpenLineage/tree/1.45.0/integration/spark/shared/facets/spark/v1)
- [Spark 1.33.0 integration](https://github.com/OpenLineage/OpenLineage/tree/1.33.0/integration/spark)
- [Historical Spark 1.2.2 `SparkVersionFacet`](https://github.com/OpenLineage/OpenLineage/blob/1.2.2/integration/spark/shared/src/main/java/io/openlineage/spark/agent/facets/SparkVersionFacet.java)
- [Historical Spark 1.2.2 version-facet schema](https://github.com/OpenLineage/OpenLineage/blob/1.2.2/integration/spark/shared/facets/spark/v1/version-facet.json)

### Apache Airflow

The current provider emits `AirflowRunFacet` under the `airflow` key and emits the deprecated `UnknownOperatorAttributeRunFacet` under `unknownSourceAttribute`. Both classes inherit the OpenLineage Python `RunFacet` implementation and do not override `_get_schema()`, so their emitted `_schemaURL` is the generic `RunFacet` URL. Airflow publishes a dedicated `AirflowRunFacet.json`, but the emitted facet does not reference it.

Sources:

- [Airflow custom facet classes](https://github.com/apache/airflow/blob/e24f5294645c20f55069c2f2a1b890eb3ff8cc2a/providers/openlineage/src/airflow/providers/openlineage/plugins/facets.py)
- [Airflow `airflow` and `unknownSourceAttribute` emission](https://github.com/apache/airflow/blob/e24f5294645c20f55069c2f2a1b890eb3ff8cc2a/providers/openlineage/src/airflow/providers/openlineage/utils/utils.py)
- [Airflow dedicated `AirflowRunFacet` schema](https://github.com/apache/airflow/blob/e24f5294645c20f55069c2f2a1b890eb3ff8cc2a/providers/openlineage/src/airflow/providers/openlineage/facets/AirflowRunFacet.json)
- [OpenLineage Python `RunFacet` schema behavior](https://github.com/OpenLineage/OpenLineage/blob/6e80a32e144aae1f94669fe5d90d6a04dc7716b1/client/python/src/openlineage/client/generated/base.py)

## DataHub behavior

DataHub validates the typed event envelope and the standard facet fields it consumes against its bundled OpenLineage 1.45.0 schema set before mapping. Standard facet keys must appear on the correct attachment point, but alternate or omitted facet schema metadata remains compatible. Unknown and producer-specific custom facet objects are accepted as opaque data. Schema resolution is classpath-only; request-provided URLs are never fetched.

The six historical facets in this document use a stricter policy only to select their specialized converter mappings. DataHub applies those mappings when all of the following match:

1. the facet is attached to `run.facets`;
2. the facet key is one of the six declared compatibility keys;
3. `_producer` matches the documented Airflow or Spark producer family;
4. `_schemaURL` matches the generic identity emitted by that producer family; and
5. the fields consumed by DataHub have the expected JSON shapes.

Spark compatibility accepts the generic `RunFacet` identity. Airflow compatibility accepts generic `RunFacet` and `BaseFacet` identities because both appear in supported historical payloads. A known key with a nonmatching producer or schema identity remains a valid opaque `BaseFacet` value and is not mapped. Unknown custom facets behave the same way. This preserves OpenLineage extensibility without allowing a familiar key alone to select producer-specific behavior.

Compatibility contributions use a fixed catalog order and retain the first value when multiple compatibility facets produce the same DataHub custom-property key. The official typed `processing_engine` facet is applied afterward and takes precedence over compatibility properties with the same DataHub keys. Collision and deprecation logs include only facet and property names, never submitted values.

`spark_version` and `unknownSourceAttribute` remain mapped for backward compatibility but are deprecated. `processing_engine` is an official standard facet and is validated against its bundled OpenLineage schema rather than this compatibility policy.

Sources:

- [DataHub compatibility catalog](https://github.com/datahub-project/datahub/tree/master/metadata-integration/java/openlineage-converter/src/main/java/io/datahubproject/openlineage/customfacet)
- [DataHub Airflow compatibility fixtures](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion-modules/airflow-plugin/tests/integration/goldens)
- [DataHub Spark fixtures](https://github.com/datahub-project/datahub/tree/master/metadata-integration/java/acryl-spark-lineage/src/test/resources/ol_events)

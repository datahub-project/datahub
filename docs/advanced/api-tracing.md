# API Tracing

## Introduction

DataHub's asynchronous APIs enable high-volume data operations, particularly for bulk ingestion processes. While these 
APIs optimize throughput, they previously lacked built-in validation mechanisms for operation status. Consequently, 
detecting processing issues required direct monitoring of backend system metrics and logs.

To address this limitation, DataHub implemented a trace/request ID system that enables end-to-end tracking of write 
operations. This tracing mechanism is particularly crucial given DataHub's multi-stage write architecture, where data 
propagates through multiple components and persists across distinct storage systems. The trace ID maintains continuity 
throughout this complex processing pipeline, providing visibility into the operation's status at each stage of execution.

The system effectively balances the performance benefits of asynchronous processing with the operational necessity of 
request tracking and validation. This enhancement significantly improves observability without compromising the 
throughput advantages of bulk operations.

## Architecture Overview

Shown below is the write path for an asynchronous write within DataHub. For more information about MCPs please see
the documentation on [MetadataChangeProposal & MetadataChangeLog Events](/docs/advanced/mcp-mcl.md).

<p align="center">
  <img width="90%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/advanced/mcp-mcl/async-ingestion.svg"/>
</p>

A successful write operation requires data persistence in at least one storage system, though typically both primary and 
search storage systems must be updated. The storage architecture consists of two main components:

* Primary Storage: Comprises MySQL, Postgres, or Cassandra, serving as the persistent store for all non-Timeseries aspects.
* Search Storage: Utilizes either Elasticsearch or OpenSearch systems.

In most operational scenarios, write operations must successfully complete across both storage layers to maintain system 
consistency and ensure complete data availability.

## Trace API

The trace API's status retrieval functionality requires three key identifiers to locate specific write operations: 
the trace ID (unique to the request), the URN, and the aspect name. This combination of identifiers ensures precise 
operation tracking within the system.

For batch operations involving multiple URNs and aspects, a single trace ID is assigned to monitor the entire request. 
In asynchronous mode, the system maintains independent status tracking for each aspect within the batch, allowing for 
granular operation monitoring.

The API returns a comprehensive status report that includes:

* Per-aspect success/failure status
* Detailed status breakdowns for each storage system
* Write states as defined in the [Write States](#Write-States) documentation
* Error information from MCP processing, when applicable, to facilitate debugging

This structured approach to status reporting enables precise monitoring of complex write operations across the system's 
various components.

### Retrieving the `trace id`

DataHub's asynchronous APIs provide trace ID information through two distinct mechanisms:

* HTTP Response Header: A W3C-compliant `traceparent` header is included in all API responses
The complete header value serves as a valid trace ID
* System Metadata: For OpenAPI v3 APIs and those returning systemMetadata, the trace ID is accessible via the 
`telemetryTraceId` property within systemMetadata

While these two trace ID formats differ structurally—with the `traceparent` adhering to W3C's Trace Context
specification—both formats are fully compatible with the Trace API for operation tracking purposes.

Header Example:
```text
traceparent: 00-00062c53a468cbd8077e7dd079846870-9199effb49910b4e-01
```

`SystemMetadata` Example:
```json
[
  {
    "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
    "status": {
      "value": {
        "removed": false
      },
      "systemMetadata": {
        "properties": {
          "telemetryLog": "false",
          "telemetryQueueSpanId": "ee9e40edcb66ce4f",
          "telemetryTraceId": "00062c53a468cbd8077e7dd079846870",
          "telemetryEnqueuedAt": "1737587612508"
        }
      }
    }
  }
]
```

### Write States

As mentioned earlier, there are multiple states for an aspect write both storage systems. These states are as follows:

| Write State              | Description                                                                                            |
|--------------------------|--------------------------------------------------------------------------------------------------------|
| `ERROR`                  | This state indicates an error occurred when processing the write request.                              |
| `PENDING`                | A pending state indicates that the write is queued and the consumer has not yet processed the message. |
| `ACTIVE_STATE`           | The write was successful and is the current value.                                                     |
| `HISTORIC_STATE`         | The write was successful, however it has been overwritten by a newer value.                            |
| `NO_OP`                  | The write is not applicable for a given storage system.                                                |
| `UNKNOWN`                | We are unable to determine the state of the write and no record of its failure exists either.          |
| `TRACE_NOT_IMPLEMENTED`  | We have not yet implemented tracing a particular aspect type. This applies to Timeseries aspects.      |

### Using the Trace API

The Trace API is implemented as an OpenAPI endpoint and can be used both programmatically and through the Swagger UI.

Required Values:
* `traceId` - The `trace id` associated with the write request. See the previous [Retrieving the `trace id`](#retrieving-the-trace-id) section for how to find this id.
* URN/Aspect names - These are passed as a POST body and should represent at least a subset of the URN/aspects from the initial request.
    An example is shown here for a single URN and 2 aspects [`datasetInfo`, `status`].
    ```json
    {
       "urn:li:dataset:(urn:li:dataPlatform:bigquery,transactions.user_profile,PROD)": ["datasetInfo", "status"]
    }
    ```
* Authorization token

Optional Parameters:
* `onlyIncludeErrors` (default: `true`) - If this parameter is set to `true`, the response will only include status information on the failed aspects.
* `detailed` (default: `false`) - If set to `true`, will include detailed information from exceptions for failed MCPs.
* `skipCache` (default: `false`) - If set to `true`, will bypass a short-lived cache of the kafka consumer group offsets.

The following shows a few examples of requests/response pairs.
* Successful Write
  * Request for URN `urn:li:dataset:(urn:li:dataPlatform:bigquery,transactions.user_profile,PROD)` and aspect `status`
      ```shell
      curl -v 'http://localhost:8080/openapi/v1/trace/write/00062c2b698bcb28e92508f8f311802d?onlyIncludeErrors=false&detailed=true&skipCache=false' \
        -H 'accept: application/json' \
        -H 'Content-Type: application/json' \
        -H 'Authorization: Bearer <TOKEN>' \
        -d '{
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,transactions.user_profile,PROD)": [
          "status"
        ]
      }' | jq
      ```
  * Example response
      ```json
      {
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,transactions.user_profile,PROD)": {
          "status": {
            "success": true,
            "primaryStorage": {
              "writeStatus": "ACTIVE_STATE"
            },
            "searchStorage": {
              "writeStatus": "ACTIVE_STATE"
            }
          }
        }
      }
      ```
* Error with exception details
  * Example request
    ```shell
    curl -v 'http://localhost:8080/openapi/v1/trace/write/00062c543e4550c8400e6f6864471a20?onlyIncludeErrors=true&detailed=true&skipCache=false' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -H 'Authorization: Bearer <TOKEN>' \
      -d '{"urn:li:dataset:(urn:li:dataPlatform:bigquery,transactions.user_profile,PROD)": ["status"]}'
    ```
  * Example response
    ```json
        {
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,transactions.user_profile,PROD)": {
                "status": {
                    "success": false,
                    "primaryStorage": {
                        "writeStatus": "ERROR",
                        "writeExceptions": [
                            {
                                "message": "Expected version -100000, actual version -1",
                                "exceptionClass": "com.linkedin.metadata.aspect.plugins.validation.AspectValidationException",
                                "stackTrace": [
                                    "com.linkedin.metadata.aspect.plugins.validation.AspectValidationException.forPrecondition(AspectValidationException.java:33)",
                                    "com.linkedin.metadata.aspect.plugins.validation.AspectValidationException.forPrecondition(AspectValidationException.java:25)",
                                    "com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.validateVersionPrecondition(ConditionalWriteValidator.java:152)",
                                    "com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.lambda$validatePreCommitAspects$2(ConditionalWriteValidator.java:100)",
                                    "java.base/java.util.Optional.flatMap(Optional.java:289)",
                                    "com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.validatePreCommitAspects(ConditionalWriteValidator.java:98)",
                                    "com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator.validatePreCommit(AspectPayloadValidator.java:38)",
                                    "com.linkedin.metadata.aspect.batch.AspectsBatch.lambda$validatePreCommit$4(AspectsBatch.java:129)",
                                    "java.base/java.util.stream.ReferencePipeline$7$1.accept(ReferencePipeline.java:273)",
                                    "java.base/java.util.ArrayList$ArrayListSpliterator.forEachRemaining(ArrayList.java:1625)",
                                    "java.base/java.util.stream.AbstractPipeline.copyInto(AbstractPipeline.java:509)",
                                    "java.base/java.util.stream.AbstractPipeline.wrapAndCopyInto(AbstractPipeline.java:499)",
                                    "java.base/java.util.stream.ForEachOps$ForEachOp.evaluateSequential(ForEachOps.java:150)",
                                    "java.base/java.util.stream.ForEachOps$ForEachOp$OfRef.evaluateSequential(ForEachOps.java:173)",
                                    "java.base/java.util.stream.AbstractPipeline.evaluate(AbstractPipeline.java:234)",
                                    "java.base/java.util.stream.ReferencePipeline.forEach(ReferencePipeline.java:596)",
                                    "com.linkedin.metadata.aspect.batch.AspectsBatch.validatePreCommit(AspectsBatch.java:130)"
                                ]
                            }
                        ]
                    },
                    "searchStorage": {
                        "writeStatus": "ERROR",
                        "writeMessage": "Primary storage write failed."
                    }
                }
            }
        }
    ```

## Trace Performance

The Trace API's performance profile varies based on operation status:

Successful Operations:
  * Optimal performance through direct storage access
  * Requires single lookup operations from SQL and Elasticsearch
  * Bypasses Kafka interaction entirely

Error State Operations:
* Performance impact due to required Kafka topic inspection
  * Optimization mechanisms implemented:
    * Timestamp-based offset seeking for efficient topic traversal
    * Parallel trace processing with controlled concurrency
    * Offset caching system to enhance response times 
      * Cache bypass available via skipCache parameter when data currency is critical

The performance differential between success and error states stems primarily from the additional overhead of Kafka 
topic inspection required for error tracking and diagnosis.

For more detail, please see the [Design Notes](#design-notes) section.

## Trace Exporters

At the foundation of the trace instrumentation is OpenTelemetry which has been a part of DataHub for quite some time. As
documented in the [Monitoring](/docs/advanced/monitoring.md) section, OpenTelemetry can be configured to export traces
to external systems. For the Trace API to function, this external system is NOT required.

### Trace Log Export

A special log-based OpenTelemetry exporter was implemented for debugging purposes. When selectively activated for a given
request it will print `trace id`s and detailed timing information as the request traverses the different components of DataHub.
The output of these logs is also not required for the Trace API to function, however it leverages the same underlying OpenTelemetry
foundation.

Activating a trace log is done using one of these methods:
* HTTP Header: `X-Enable-Trace-Log: true`
* Cookie: `enable-trace-log: true`
  * javascript: `document.cookie = "enable-trace-log=true";`

Example logs for a single request with tracing logging enabled:
* GMS
```text
i.d.metadata.context.RequestContext:53 - RequestContext{actorUrn='urn:li:corpuser:datahub', sourceIP='172.18.0.5', requestAPI=OPENAPI, requestID='createAspect([dataset])', userAgent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'}
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: a2898a18f9f0c4f1, ParentId: dd746f079d1232ba, Name: ingestTimeseriesProposal, Duration: 0.03 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={async=true, batch.size=1}, capacity=128, totalAddedValues=2}
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: 02e058ff616e4c99, ParentId: 7ed88659811a8fdb, Name: produceMetadataChangeProposal, Duration: 0.03 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={messaging.destination_kind=topic, messaging.system=kafka, messaging.destination=MetadataChangeProposal_v1, messaging.operation=publish, queue.enqueued_at=1737418391958}, capacity=128, totalAddedValues=5}
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: 7ed88659811a8fdb, ParentId: dd746f079d1232ba, Name: ingestProposalAsync, Duration: 2.57 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={batch.size=1}, capacity=128, totalAddedValues=1}
```
* MCE Consumer
```text
c.l.m.k.MetadataChangeProposalsProcessor:89 - Got MCP event key: urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.daily_temperature,PROD), topic: MetadataChangeProposal_v1, partition: 0, offset: 75, value size: 412, timestamp: 1737418391959
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: a65075fe0982d873, ParentId: 02e058ff616e4c99, Name: consume, Duration: 0.01 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={messaging.destination_kind=topic, queue.duration_ms=4, messaging.system=kafka, messaging.destination=MetadataChangeProposal_v1, messaging.operation=receive, queue.enqueued_at=1737418391958}, capacity=128, totalAddedValues=6}
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: dd746f079d1232ba, ParentId: 0000000000000000, Name: POST /openapi/v3/entity/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Cclimate.daily_temperature%2CPROD%29/status, Duration: 16.18 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={request.api=OPENAPI, http.status_code=202, user.id=urn:li:corpuser:datahub, http.url=/openapi/v3/entity/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Cclimate.daily_temperature%2CPROD%29/status, request.id=createAspect([dataset]), http.method=POST}, capacity=128, totalAddedValues=6}
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: 94a019b95154c0e7, ParentId: 0cb378fe4f5ad185, Name: ingestProposalSync, Duration: 0.01 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={batch.size=0}, capacity=128, totalAddedValues=1}
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: 0cb378fe4f5ad185, ParentId: 68df6bc4729dc0a2, Name: ingestTimeseriesProposal, Duration: 0.25 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={async=false, batch.size=1}, capacity=128, totalAddedValues=2}
c.l.m.entity.EntityServiceImpl:988 - Ingesting aspects batch to database: AspectsBatchImpl{items=[ChangeMCP{changeType=UPSERT, urn=urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.daily_temperature,PROD), aspectName='status', recordTemplate={removed=false}, systemMetadata={lastObserved=1737418391954, version=1, properties={telemetryLog=true, telemetryQueueSpanId=02e058ff616e4c99, telemetryEnqueu...}]}
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: 4754a1c02dadec4c, ParentId: ef383b26f0040fc5, Name: retentionService, Duration: 0.09 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={batch.size=1}, capacity=128, totalAddedValues=1}
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: ef383b26f0040fc5, ParentId: 7ae629151400fc18, Name: ingestAspectsToLocalDB, Duration: 18.64 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={batch.size=1, dwizName=com.linkedin.metadata.entity.EntityServiceImpl.ingestAspectsToLocalDB}, capacity=128, totalAddedValues=2}
c.l.m.entity.EntityServiceImpl:1900 - Producing MCL for ingested aspect status, urn urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.daily_temperature,PROD)
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: f1a8a1da99f1ae23, ParentId: c5f8b3884060722c, Name: produceMetadataChangeLog, Duration: 0.10 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={messaging.destination_kind=topic, messaging.system=kafka, messaging.destination=MetadataChangeLog_Versioned_v1, messaging.operation=publish, queue.enqueued_at=1737418391982}, capacity=128, totalAddedValues=5}
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: c5f8b3884060722c, ParentId: 7ae629151400fc18, Name: emitMCL, Duration: 14.32 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={batch.size=1}, capacity=128, totalAddedValues=1}
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: 7ae629151400fc18, ParentId: 68df6bc4729dc0a2, Name: ingestProposalSync, Duration: 37.90 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={batch.size=1}, capacity=128, totalAddedValues=1}
c.l.m.k.MetadataChangeProposalsProcessor:128 - Successfully processed MCP event urn: urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.daily_temperature,PROD)
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: 68df6bc4729dc0a2, ParentId: 02e058ff616e4c99, Name: consume, Duration: 39.11 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={batch.size=1, dwizName=com.linkedin.metadata.kafka.MetadataChangeProposalsProcessor.consume}, capacity=128, totalAddedValues=2}
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: 04dc44653b634df2, ParentId: 02e058ff616e4c99, Name: consume, Duration: 0.03 ms
```
* MAE Consumer
```text
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={messaging.destination_kind=topic, queue.duration_ms=22, messaging.system=kafka, messaging.destination=MetadataChangeLog_Versioned_v1, messaging.operation=receive, queue.enqueued_at=1737418391982}, capacity=128, totalAddedValues=6}
c.l.metadata.kafka.MCLKafkaListener:96 - Invoking MCL hooks for consumer: generic-mae-consumer-job-client urn: urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.daily_temperature,PROD), aspect name: status, entity type: dataset, change type: UPSERT
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: 3c3c055c360dc8e4, ParentId: 1de99215a0e82697, Name: FormAssignmentHook, Duration: 0.06 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={dwizName=com.linkedin.metadata.kafka.MCLKafkaListener.FormAssignmentHook_latency}, capacity=128, totalAddedValues=1}
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: 8e238d0156baacc4, ParentId: 1de99215a0e82697, Name: IngestionSchedulerHook, Duration: 0.05 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={dwizName=com.linkedin.metadata.kafka.MCLKafkaListener.IngestionSchedulerHook_latency}, capacity=128, totalAddedValues=1}
c.l.m.s.e.update.ESBulkProcessor:85 - Added request id: urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2Cclimate.daily_temperature%2CPROD%29, operation type: UPDATE, index: datasetindex_v2
c.l.m.s.e.update.ESBulkProcessor:85 - Added request id: SIHRXj1ktF7qkwPBZO8w0A==, operation type: UPDATE, index: system_metadata_service_v1
c.l.m.s.e.update.ESBulkProcessor:85 - Added request id: 2p3742l4sFS3wcL82Qh2lQ==, operation type: UPDATE, index: system_metadata_service_v1
c.l.m.s.e.update.ESBulkProcessor:85 - Added request id: CfZKRLsf25/e3p3mURzlnA==, operation type: UPDATE, index: system_metadata_service_v1
c.l.m.s.e.update.ESBulkProcessor:85 - Added request id: 8tvhG5ARd5BOdEbqaZkE0g==, operation type: UPDATE, index: system_metadata_service_v1
c.l.m.s.e.update.ESBulkProcessor:85 - Added request id: rAvQOOBItiKAym622S4dcQ==, operation type: UPDATE, index: system_metadata_service_v1
c.l.m.s.e.update.ESBulkProcessor:85 - Added request id: YqT6TNy7MAMOAyVXh6abMA==, operation type: UPDATE, index: system_metadata_service_v1
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: 054ac726204b449c, ParentId: 1de99215a0e82697, Name: UpdateIndicesHook, Duration: 47.31 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={dwizName=com.linkedin.metadata.kafka.MCLKafkaListener.UpdateIndicesHook_latency}, capacity=128, totalAddedValues=1}
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: 14d9ded49a94c7b8, ParentId: 1de99215a0e82697, Name: IncidentsSummaryHook, Duration: 0.09 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={dwizName=com.linkedin.metadata.kafka.MCLKafkaListener.IncidentsSummaryHook_latency}, capacity=128, totalAddedValues=1}
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: a92d9e54ade6073b, ParentId: 1de99215a0e82697, Name: EntityChangeEventGeneratorHook, Duration: 9.10 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={dwizName=com.linkedin.metadata.kafka.MCLKafkaListener.EntityChangeEventGeneratorHook_latency}, capacity=128, totalAddedValues=1}
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: c06dc7f131e57fca, ParentId: 1de99215a0e82697, Name: SiblingAssociationHook, Duration: 0.07 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={dwizName=com.linkedin.metadata.kafka.MCLKafkaListener.SiblingAssociationHook_latency}, capacity=128, totalAddedValues=1}
c.l.metadata.kafka.MCLKafkaListener:139 - Successfully completed MCL hooks for consumer: generic-mae-consumer-job-client urn: urn:li:dataset:(urn:li:dataPlatform:snowflake,climate.daily_temperature,PROD)
i.d.metadata.context.TraceContext:366 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, SpanId: 1de99215a0e82697, ParentId: 02e058ff616e4c99, Name: consume, Duration: 58.67 ms
i.d.metadata.context.TraceContext:376 - Trace: 00062c2c3e1403109bbaf3d2e39adcd0, Attributes: AttributesMap{data={batch.size=1, dwizName=com.linkedin.metadata.kafka.MCLKafkaListener.consume}, capacity=128, totalAddedValues=2}
```

## Design Notes

For the initial implementation no specific OpenTelemetry infrastructure is required, however existing environment variables
for OpenTelemetry can continue to be used and will export the new spans if configured.

The Trace API implementation does not rely on any additional external systems or infrastructure. Due to this design
choice, the trace is determined by inspecting the 3 storage systems (Primary Storage (SQL/Cassandra), Elasticsearch/Opensearch,
Kafka topics) for the `trace id` or related timestamps.

The `trace id` is stored in systemMetadata in both SQL and ES. For ES specifically, the presence of the `trace id` in
the system metadata index is used as a proxy to determine a successful write to ES. 

The tracing feature will additionally fetch messages from the kafka topics (including the failed MCP topic) for 
more detailed error information. Pending states are derived from offsets of the message vs the current offsets of the 
consumer groups.
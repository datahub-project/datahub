# Appendix: Raw patches applied today

Supporting evidence for
[`16975-tls-kafka-oidc.md`](./16975-tls-kafka-oidc.md). The patches
below are what a production DataHub deployment currently has to
apply at container startup to make TLS work end-to-end, reproduced
verbatim from an internal `datahub-config` Helm chart
(`files/python-kafka-ssl/patches/`).

All file paths below are relative to the `datahub-project/datahub`
repository root. Everything is verified against the tip of `master`
in `datahub-project/datahub`
([3acb30f73](https://github.com/datahub-project/datahub/commit/3acb30f7388a0d63e05645a65d780a574df1cc77))
and `acryldata/datahub-helm`
([19cac5cc0](https://github.com/acryldata/datahub-helm/commit/19cac5cc0551cb54b529e7cd9c84b48e2804aaf5)).

## Background

### Python `confluent-kafka` Schema Registry SSL context

When constructing the HTTP session for Schema Registry calls
([#14576](https://github.com/datahub-project/datahub/issues/14576)),
`confluent-kafka` requires a pre-built `ssl.SSLContext` object;
passing `ssl.ca.location` as a string path is silently dropped. The
same five-line fix is needed in five files
(`confluent_schema_registry.py`, `kafka.py`, `kafka_emitter.py`,
`datahub_kafka_reader.py`, and `datahub-actions`'
`kafka_event_source.py`) — see A.2–A.6 below.

### `datahub-actions` bundled YAMLs

`docker/datahub-actions/config/executor.yaml` ships without any
`consumer_config` / `schema_registry_config` block, and
`doc_propagation_action.yaml` has a `consumer_config` block with
only `max.poll.interval.ms` and no SSL fields
([#4287](https://github.com/datahub-project/datahub/issues/4287),
[#14568](https://github.com/datahub-project/datahub/issues/14568)).
To configure TLS for these, operators hand-mount replacement
ConfigMaps — see A.7–A.8.

## A.1 Startup script

```bash
#!/usr/bin/env bash
# 001_patch_schema_registry_client.sh
set -euo pipefail
set -x
cd /
mkdir -p /tmp/python-patches-writable
cp -a /tmp/python-patches/. /tmp/python-patches-writable/

for patch in /tmp/python-patches-writable/*.patch; do
    echo "" >> "$patch"
done
git apply /tmp/python-patches-writable/*.patch

# ingest creates dynamic venvs, so we need to patch the run_ingest.sh
# script to copy our modified file into the venv at runtime
# can all be removed once datahub fixes their whole story around ssl,
# or when we fork it temporarily
sed -i '/# *Execute/ a\
cp "/metadata-ingestion/src/datahub/ingestion/source/confluent_schema_registry.py" "/tmp/datahub/ingest/venv-${venv_name}/lib/python3.10/site-packages/datahub/ingestion/source/confluent_schema_registry.py"
' /home/datahub/.venv/bin/run_ingest.sh
```

## A.2 `metadata-ingestion/src/datahub/ingestion/source/confluent_schema_registry.py` ([#14576](https://github.com/datahub-project/datahub/issues/14576))

```diff
diff --git a/metadata-ingestion/src/datahub/ingestion/source/confluent_schema_registry.py b/metadata-ingestion/src/datahub/ingestion/source/confluent_schema_registry.py
--- a/metadata-ingestion/src/datahub/ingestion/source/confluent_schema_registry.py
+++ b/metadata-ingestion/src/datahub/ingestion/source/confluent_schema_registry.py
@@ -1,5 +1,7 @@
 import json
 import logging
+import ssl
+from pprint import pformat
 from dataclasses import dataclass
 from hashlib import md5
 from typing import Any, List, Optional, Set, Tuple
@@ -50,12 +52,19 @@ class ConfluentSchemaRegistry(KafkaSchemaRegistryBase):
     ) -> None:
         self.source_config: KafkaSourceConfig = source_config
         self.report: KafkaSourceReport = report
-        self.schema_registry_client = SchemaRegistryClient(
-            {
-                "url": source_config.connection.schema_registry_url,
-                **source_config.connection.schema_registry_config,
-            }
+
+        logger.info("Creating Confluent Schema Registry client...")
+        logger.info(
+            "Schema Registry Config:\n%s",
+            pformat(source_config.connection.schema_registry_config, indent=2, width=80)
         )
+        ca_context = ssl.create_default_context(cafile=source_config.connection.schema_registry_config.get("ssl.ca.location"))
+        schema_registry_config = {
+            "url": source_config.connection.schema_registry_url,
+            **source_config.connection.schema_registry_config,
+            "ssl.ca.location": ca_context,
+        }
+        self.schema_registry_client = SchemaRegistryClient(schema_registry_config)
         self.known_schema_registry_subjects: List[str] = []
         try:
             self.known_schema_registry_subjects.extend(
@@ -63,6 +72,7 @@ class ConfluentSchemaRegistry(KafkaSchemaRegistryBase):
             )
         except Exception as e:
             logger.warning(f"Failed to get subjects from schema registry: {e}")
+            logger.warning(f"schema_registry_config: {schema_registry_config}")

         self.field_meta_processor = OperationProcessor(
             self.source_config.field_meta_mapping,
```

## A.3 `metadata-ingestion/src/datahub/ingestion/source/kafka/kafka.py` — connectivity test ([#14576](https://github.com/datahub-project/datahub/issues/14576))

```diff
diff --git a/metadata-ingestion/src/datahub/ingestion/source/kafka/kafka.py b/metadata-ingestion/src/datahub/ingestion/source/kafka/kafka.py
--- a/metadata-ingestion/src/datahub/ingestion/source/kafka/kafka.py
+++ b/metadata-ingestion/src/datahub/ingestion/source/kafka/kafka.py
@@ -1,6 +1,7 @@
 import concurrent.futures
 import json
 import logging
+import ssl
 from dataclasses import dataclass, field
 from typing import Dict, Iterable, List, Optional, Type, Union, cast

@@ -212,10 +213,12 @@ class KafkaConnectionTest:

     def schema_registry_connectivity(self) -> CapabilityReport:
         try:
+            ca_context = ssl.create_default_context(cafile=self.config.connection.schema_registry_config.get("ssl.ca.location"))
             SchemaRegistryClient(
                 {
                     "url": self.config.connection.schema_registry_url,
                     **self.config.connection.schema_registry_config,
+                    "ssl.ca.location": ca_context,
                 }
             ).get_subjects()
             return CapabilityReport(capable=True)
```

## A.4 `metadata-ingestion/src/datahub/emitter/kafka_emitter.py` ([#14576](https://github.com/datahub-project/datahub/issues/14576))

```diff
diff --git a/metadata-ingestion/src/datahub/emitter/kafka_emitter.py b/metadata-ingestion/src/datahub/emitter/kafka_emitter.py
--- a/metadata-ingestion/src/datahub/emitter/kafka_emitter.py
+++ b/metadata-ingestion/src/datahub/emitter/kafka_emitter.py
@@ -1,4 +1,5 @@
 import logging
+import ssl
 from typing import Callable, Dict, Optional, Union

 import pydantic
@@ -64,9 +65,11 @@ class KafkaEmitterConfig(ConfigModel):
 class DatahubKafkaEmitter(Closeable, Emitter):
     def __init__(self, config: KafkaEmitterConfig):
         self.config = config
+        ca_context = ssl.create_default_context(cafile=self.config.connection.schema_registry_config.get("ssl.ca.location"))
         schema_registry_conf = {
             "url": self.config.connection.schema_registry_url,
             **self.config.connection.schema_registry_config,
+            "ssl.ca.location": ca_context,
         }
         schema_registry_client = SchemaRegistryClient(schema_registry_conf)
```

## A.5 `metadata-ingestion/src/datahub/ingestion/source/datahub/datahub_kafka_reader.py` ([#14576](https://github.com/datahub-project/datahub/issues/14576))

Note: this patch also fixes a secondary bug where the reader
constructed `SchemaRegistryClient({"url": …})` without passing any of
the `schema_registry_config`, so TLS config was silently dropped even
before the CA-context bug hit.

```diff
diff --git a/metadata-ingestion/src/datahub/ingestion/source/datahub/datahub_kafka_reader.py b/metadata-ingestion/src/datahub/ingestion/source/datahub/datahub_kafka_reader.py
--- a/metadata-ingestion/src/datahub/ingestion/source/datahub/datahub_kafka_reader.py
+++ b/metadata-ingestion/src/datahub/ingestion/source/datahub/datahub_kafka_reader.py
@@ -1,4 +1,5 @@
 import logging
+import ssl
 from datetime import datetime
 from typing import Dict, Iterable, List, Tuple

@@ -40,6 +41,13 @@ class DataHubKafkaReader(Closeable):
         self.ctx = ctx

     def __enter__(self) -> "DataHubKafkaReader":
+        ca_context = ssl.create_default_context(cafile=self.config.connection.schema_registry_config.get("ssl.ca.location"))
+        schema_registry_config = {
+            "url": self.connection_config.schema_registry_url,
+            **self.config.connection.schema_registry_config,
+            "ssl.ca.location": ca_context,
+        }
+        schema_registry_client = SchemaRegistryClient(schema_registry_config)
         self.consumer = DeserializingConsumer(
             {
                 "group.id": self.group_id,
@@ -48,9 +56,7 @@ class DataHubKafkaReader(Closeable):
                 "auto.offset.reset": "earliest",
                 "enable.auto.commit": False,
                 "value.deserializer": AvroDeserializer(
-                    schema_registry_client=SchemaRegistryClient(
-                        {"url": self.connection_config.schema_registry_url}
-                    ),
+                    schema_registry_client=schema_registry_client,
                     return_record_name=True,
                 ),
             }
```

## A.6 `datahub-actions/src/datahub_actions/plugin/source/kafka/kafka_event_source.py` ([#4287](https://github.com/datahub-project/datahub/issues/4287))

```diff
diff --git a/datahub-actions/src/datahub_actions/plugin/source/kafka/kafka_event_source.py b/datahub-actions/src/datahub_actions/plugin/source/kafka/kafka_event_source.py
--- a/datahub-actions/src/datahub_actions/plugin/source/kafka/kafka_event_source.py
+++ b/datahub-actions/src/datahub_actions/plugin/source/kafka/kafka_event_source.py
@@ -13,6 +13,7 @@
 # limitations under the License.

 import logging
+import ssl
 import os
 from dataclasses import dataclass
 from typing import Any, Callable, Dict, Iterable, Optional
@@ -130,7 +131,12 @@ class KafkaEventSource(EventSource):
     def __init__(self, config: KafkaEventSourceConfig, ctx: PipelineContext):
         self.source_config = config
         schema_client_config = config.connection.schema_registry_config.copy()
+
+        ca_context = ssl.create_default_context(
+            cafile=self.source_config.connection.consumer_config.get("ssl.ca.location")
+        )
         schema_client_config["url"] = self.source_config.connection.schema_registry_url
+        schema_client_config["ssl.ca.location"] = ca_context
         self.schema_registry_client = SchemaRegistryClient(schema_client_config)

         async_commit_config: Dict[str, Any] = {}
```

## A.7 `docker/datahub-actions/config/executor.yaml` ([datahub-helm#601](https://github.com/acryldata/datahub-helm/issues/601))

```diff
diff --git a/docker/datahub-actions/config/executor.yaml b/docker/datahub-actions/config/executor.yaml
--- a/docker/datahub-actions/config/executor.yaml
+++ b/docker/datahub-actions/config/executor.yaml
@@ -18,6 +18,15 @@ source:
     connection:
       bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
       schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
+      consumer_config:
+        security.protocol: SSL
+        ssl.ca.location: ${KAFKA_PROPERTIES_SSL_CA_LOCATION:-/mnt/datahub/certs/ca-bundle/ca-bundle.crt}
+        ssl.certificate.location: ${KAFKA_PROPERTIES_SSL_CERTIFICATE_LOCATION:-/mnt/datahub/certs/kafka-keystore/client.cert.pem}
+        ssl.key.location: ${KAFKA_PROPERTIES_SSL_KEY_LOCATION:-/mnt/datahub/certs/kafka-keystore/client.key.pem}
+      schema_registry_config:
+        ssl.ca.location: ${KAFKA_PROPERTIES_SSL_CA_LOCATION:-/mnt/datahub/certs/ca-bundle/ca-bundle.crt}
+        ssl.certificate.location: ${KAFKA_PROPERTIES_SSL_CERTIFICATE_LOCATION:-/mnt/datahub/certs/kafka-keystore/client.cert.pem}
+        ssl.key.location: ${KAFKA_PROPERTIES_SSL_KEY_LOCATION:-/mnt/datahub/certs/kafka-keystore/client.key.pem}
     topic_routes:
       mcl: ${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:-MetadataChangeLog_Versioned_v1}
       pe: ${PLATFORM_EVENT_TOPIC_NAME:-PlatformEvent_v1}
```

## A.8 `docker/datahub-actions/config/doc_propagation_action.yaml` ([datahub-helm#601](https://github.com/acryldata/datahub-helm/issues/601))

```diff
diff --git a/docker/datahub-actions/config/doc_propagation_action.yaml b/docker/datahub-actions/config/doc_propagation_action.yaml
--- a/docker/datahub-actions/config/doc_propagation_action.yaml
+++ b/docker/datahub-actions/config/doc_propagation_action.yaml
@@ -21,6 +21,14 @@ source:
       schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
       consumer_config:
         max.poll.interval.ms: ${MAX_POLL_INTERVAL_MS:-60000} # 1 minute per poll
+        security.protocol: SSL
+        ssl.ca.location: ${KAFKA_PROPERTIES_SSL_CA_LOCATION:-/mnt/datahub/certs/ca-bundle/ca-bundle.crt}
+        ssl.certificate.location: ${KAFKA_PROPERTIES_SSL_CERTIFICATE_LOCATION:-/mnt/datahub/certs/kafka-keystore/client.cert.pem}
+        ssl.key.location: ${KAFKA_PROPERTIES_SSL_KEY_LOCATION:-/mnt/datahub/certs/kafka-keystore/client.key.pem}
+      schema_registry_config:
+        ssl.ca.location: ${KAFKA_PROPERTIES_SSL_CA_LOCATION:-/mnt/datahub/certs/ca-bundle/ca-bundle.crt}
+        ssl.certificate.location: ${KAFKA_PROPERTIES_SSL_CERTIFICATE_LOCATION:-/mnt/datahub/certs/kafka-keystore/client.cert.pem}
+        ssl.key.location: ${KAFKA_PROPERTIES_SSL_KEY_LOCATION:-/mnt/datahub/certs/kafka-keystore/client.key.pem}
     topic_routes:
       mcl: ${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:-MetadataChangeLog_Versioned_v1}
       pe: ${PLATFORM_EVENT_TOPIC_NAME:-PlatformEvent_v1}
```

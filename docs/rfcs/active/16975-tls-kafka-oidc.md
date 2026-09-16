- Start Date: 2026-04-10
- RFC PR: [datahub-project/datahub#16975](https://github.com/datahub-project/datahub/pull/16975)
- Discussion Issue: (TBD)
- Implementation PR(s):
  - [datahub-project/datahub#16997](https://github.com/datahub-project/datahub/pull/16997)
    — `fix(gms): forward ssl.keystore.type/truststore.type to schema registry client`
    — the `KafkaSchemaRegistryFactory` `@Value` allowlist addition described under
    Component wiring.
- Related work:
  - [datahub-project/datahub#15479](https://github.com/datahub-project/datahub/pull/15479)
    — `feat(datahub-frontend): add client-cert-based authentication for oidc`
    (in flight) — adds `private_key_jwt` (RFC 7523) using PEM key files.
    Composes with this RFC; lands on its own track.
  - [datahub-project/datahub#16652](https://github.com/datahub-project/datahub/pull/16652)
    — `feat(gms): allow user lookup for gms oidc` — GMS-side OBO user mapping.
  - [datahub-project/datahub#16977](https://github.com/datahub-project/datahub/pull/16977)
    — sibling RFC for MCP server improvements (OIDC OBO via Microsoft Entra ID).
  - [acryldata/datahub-helm#692](https://github.com/acryldata/datahub-helm/pull/692)
    — Helm chart changes for wiring PEM-based TLS materials into DataHub deployments.

# PEM-first TLS for DataHub outbound connections

## Summary

We run DataHub on AKS against an internal mTLS Kafka cluster and hit
persistent friction making the Kafka TLS configuration work
end-to-end across the Java and Python halves of the stack.
This mainly stems from missing patches in python services for TLS compatibility, and a non-uniform
configuration interface in the helm-chart.

This RFC proposes a single operator-facing Helm configuration object (`global.tls`)
that the chart templates into the runtime-native env vars each
component already understands. Alongside that, this RFC lists the small set of code fixes that
unblock TLS (with PEM as proposed default format) end-to-end.

Additionally, we reference a new OIDC client-cert feature
[#15479](https://github.com/datahub-project/datahub/pull/15479)
(`private_key_jwt` / RFC 7523) that composes with this RFC on its own
track and removes the need for using client-credentials for SSO.

## Basic example

An operator running DataHub on Kubernetes with an internal CA and
workload certs delivered via [External Secrets
Operator](https://external-secrets.io/) pulls the CA bundle and the
client cert/key out of their secret store into two Kubernetes secrets,
then points `global.tls` at them:

```yaml
# ExternalSecret: internal CA bundle (public, shared across workloads)
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: datahub-internal-ca
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: vault-backend
    kind: ClusterSecretStore
  target:
    name: datahub-internal-ca
  data:
    - secretKey: ca.pem
      remoteRef:
        key: pki/internal/ca
        property: chain
---
# ExternalSecret: per-workload mTLS client cert + key
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: datahub-workload-tls
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: vault-backend
    kind: ClusterSecretStore
  target:
    name: datahub-workload-tls
  data:
    - secretKey: tls.crt
      remoteRef:
        key: pki/workloads/datahub
        property: certificate
    - secretKey: tls.key
      remoteRef:
        key: pki/workloads/datahub
        property: private_key
```

```yaml
# DataHub Helm values
global:
  tls:
    enabled: true
    ca:
      secretName: datahub-internal-ca
      key: ca.pem
    cert:
      secretName: datahub-workload-tls
      key: tls.crt
    key:
      secretName: datahub-workload-tls
      key: tls.key
```

The chart mounts the referenced secrets into every component that
talks to Kafka, Schema Registry, or GMS, and templates them into the
env vars each runtime already understands natively
(`SPRING_KAFKA_PROPERTIES_SSL_*`, `KAFKA_SCHEMA_REGISTRY_SSL_*`,
`KAFKA_PROPERTIES_SSL_*`, `REQUESTS_CA_BUNDLE`).

## Motivation

Enterprise environments are shifting to credential-less service authentication because long-lived shared secrets (
passwords, PATs, client secrets) are operationally costly to manage, easy to leak, and trivially exploitable if stolen.
Many platforms already issue short-lived X.509 identities to workloads, making separate static credentials redundant and
counterproductive. Mutual-TLS client authentication (RFC 8705) is widely supported by enterprise IdPs and required for
FAPI-grade deployments.

The friction is visible in the issue tracker:

- datahub-project/datahub#4287 — Actions container not configurable with TLS
- datahub-project/datahub#14354 — MAE/MCE consumers can't reach Schema Registry with TLS + basic auth
- datahub-project/datahub#14568 — datahub-actions missing connection parameters
- datahub-project/datahub#14576 — SchemaRegistryClient doesn't pass ssl.ca.location to the SSL context
- datahub-project/datahub#5786 — actions pod can't reach MSK TLS-only brokers
- datahub-project/datahub#13223 — Kafka OAUTHBEARER misconfigured
- acryldata/datahub-helm#601 — chart has no unified SSL story; java services want JKS, python datahub-actions wants
  PEM/PKCS12, the only workaround is a hand-mounted ConfigMap with a custom executor.yaml
- acryldata/datahub-helm#687 — SCRAM auth against AWS MSK broken
- acryldata/datahub-helm#79 — kafka-setup unconditionally emits ssl.keystore.\*, preventing TLS-only (server-auth)
  deployments
- acryldata/mcp-server-datahub#52 — MCP server has no custom-CA path for GMS; the only workaround is disabling
  verification entirely

Making TLS work end-to-end across the Java and Python halves of the
stack currently requires ~200 lines of `git apply` patches, two YAML
overrides, and a `sed` hook into `run_ingest.sh`. The concrete code
gaps these patches fill, with their fixes, are walked through in
[`16975-tls-kafka-oidc-patches.md`](./16975-tls-kafka-oidc-patches.md).
Obviously, this is a non-preferred workaround, which adds additional friction
to every DataHub upgrade.

## Requirements

The design target is a single operator-facing Helm interface that
wires PEM TLS end-to-end across the Java and Python components of
the stack, so the ~200-line production patch bundle in the
[patches appendix](./16975-tls-kafka-oidc-patches.md) is no longer
needed. PEM is the format cert-manager, Vault PKI, and SPIRE emit,
so this also works out of the box in setups, that use those.

Concretely, this requires:

- **Helm chart** exposes a `global.tls` block and templates it into
  the runtime-native env vars of every component that talks to
  Kafka, Schema Registry, or GMS.
- **Python `confluent-kafka` Schema Registry clients** honor the
  configured CA bundle.
- **`datahub-actions` bundled YAMLs** ship with a working SSL surface
  driven by the `KAFKA_PROPERTIES_SSL_*` env vars.
- **Java Schema Registry factory** honors PEM keystore / truststore
  type configuration.
- **Fail-closed** on half-configured client identity: if exactly one
  of `global.tls.cert` / `global.tls.key` is set, the chart refuses
  to render.

## Non-Requirements

- **GMS ingress mTLS** — tracked at
  [#15755](https://github.com/datahub-project/datahub/issues/15755),
  orthogonal to outbound-client TLS.
- **Replacing SASL** — remains first-class.
- **JKS support in the Helm chart** — convert to PEM once with
  `openssl pkcs12`.
- **Per-endpoint CA / cert overrides** — not needed for the deployment
  shape that motivates this RFC; deferrable.
- **OIDC truststore wiring** — left alone; today's behavior (JVM
  default `cacerts`) is preserved and works for managed IdPs.
- **OIDC client certificate authentication** — in flight at
  [#15479](https://github.com/datahub-project/datahub/pull/15479)
  (`private_key_jwt` / RFC 7523 with PEM key/cert files via
  BouncyCastle). Composes with this RFC; not duplicated here.

## Detailed design

### Why PEM

Outbound-client TLS needs four inputs: a CA bundle, a client cert
(including any intermediate chain), a client private key, and
optionally a password for that key when it is encrypted. That's
the complete surface — certificate rotation is solved by remounting
the secret (format-agnostic), hardware tokens / PKCS#11 are out of
scope for a Helm values story, and FIPS-mode JDKs and
BoringSSL/librdkafka both accept PEM unchanged.

PEM is picked as the canonical format because it's what every
modern Kubernetes-native cert-delivery pipeline already emits:

| Source                                   | Default output format                                                                                                          |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| **cert-manager** (k8s de facto)          | PEM (`tls.crt`, `tls.key`, `ca.crt`) — this is the idiomatic Kubernetes TLS secret                                             |
| **`kubernetes.io/tls` secret type**      | PEM by definition                                                                                                              |
| **HashiCorp Vault PKI**                  | PEM (`certificate`, `private_key`, `issuing_ca` all PEM strings in the API)                                                    |
| **SPIFFE / SPIRE**                       | PEM (X.509-SVID via Workload API / `spiffe-helper`)                                                                            |
| **AWS ACM / ACM Private CA**             | PEM (API returns PEM-encoded strings)                                                                                          |
| **AWS MSK mTLS client**                  | ACM PCA → PEM; Confluent docs show `openssl pkcs12` as an extra step _only_ for legacy Java clients                            |
| **GCP Certificate Manager / CA Service** | PEM                                                                                                                            |
| **Azure Key Vault certificates**         | Stored internally as PKCS12, but mounted as PEM by the Azure CSI driver (`secrets-store-csi-driver-provider-azure`) by default |
| **External Secrets Operator**            | Whatever the backend emits — PEM end-to-end when paired with any of the above                                                  |
| **Let's Encrypt / ACME**                 | PEM                                                                                                                            |
| **Istio / Linkerd / Cilium mTLS**        | PEM                                                                                                                            |
| **EKS / GKE / AKS**                      | Secret blobs are format-agnostic; the workflows that populate them are all PEM                                                 |

The runtimes DataHub uses all accept PEM natively:

- **Java Kafka 2.7+** via `ssl.keystore.type=PEM`
  ([KIP-651](https://cwiki.apache.org/confluence/display/KAFKA/KIP-651+-+Support+PEM+format+for+SSL+certificates+and+private+key)).
  The pinned `kafka-clients:8.0.0-ccs` is well past that cutoff
  (subject to the version-mapping question in Unresolved).
- **Spring Boot Kafka** is format-agnostic — it forwards whatever
  property keys you set.
- **librdkafka** — PEM-native.
- **Confluent REST client** — PEM supported in recent versions.
- **Python `requests` / `httpx` / `certifi`** — PEM-only by design.

The JDK itself has been moving off JKS for years: **JDK 9+ defaults
to PKCS12, not JKS**, and JKS is marked legacy upstream. The
existing JKS example in `charts/datahub/values.yaml` is a vestige of
pre-KIP-651 Kafka, not a reflection of where the ecosystem is.

**Operators who have JKS today** can convert once, out of band:

```bash
# JKS → PKCS12 (skip if already PKCS12)
keytool -importkeystore \
  -srcstoretype JKS     -srckeystore  store.jks -srcstorepass  <jks-pw> \
  -deststoretype PKCS12 -destkeystore store.p12 -deststorepass <p12-pw>

# PKCS12 → PEM
openssl pkcs12 -in store.p12 -passin pass:<p12-pw> -out ca.pem  -nokeys -cacerts
openssl pkcs12 -in store.p12 -passin pass:<p12-pw> -out tls.crt -nokeys -clcerts
openssl pkcs12 -in store.p12 -passin pass:<p12-pw> -out tls.key -nocerts -nodes
```

Land the resulting PEM files in their secret store (Vault / AWS
Secrets Manager / Azure Key Vault / plain K8s Secret) and point
`global.tls` at them. Operators who would rather not convert keep
using the existing `springKafkaConfigurationOverrides` interface
with JKS / PKCS12 unchanged — `global.tls` is additive.

### Runtime-native env vars per hop

Each component already has an env var it natively consumes for its
TLS config. The Helm chart templates `global.tls` into these:

| Hop                                                                 | Runtime                                             | Env vars the runtime natively understands                                                                                                                                                              |
| ------------------------------------------------------------------- | --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Kafka broker, Java services (GMS, MAE/MCE, `datahub-system-update`) | Spring Boot Kafka                                   | `SPRING_KAFKA_PROPERTIES_SSL_KEYSTORE_TYPE=PEM`, `SPRING_KAFKA_PROPERTIES_SSL_KEYSTORE_LOCATION`, `SPRING_KAFKA_PROPERTIES_SSL_TRUSTSTORE_TYPE=PEM`, `SPRING_KAFKA_PROPERTIES_SSL_TRUSTSTORE_LOCATION` |
| Schema Registry, Java GMS                                           | Confluent serializer + `CachedSchemaRegistryClient` | `KAFKA_SCHEMA_REGISTRY_SSL_KEYSTORE_TYPE=PEM`, `KAFKA_SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION`, `KAFKA_SCHEMA_REGISTRY_SSL_TRUSTSTORE_TYPE=PEM`, `KAFKA_SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION`         |
| Schema Registry, MAE/MCE consumers                                  | Spring Boot prop passthrough                        | `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SSL_*` (same four keys)                                                                                                                                       |
| Kafka broker, `acryl-datahub-actions`                               | librdkafka                                          | `KAFKA_PROPERTIES_SSL_CA_LOCATION`, `KAFKA_PROPERTIES_SSL_CERTIFICATE_LOCATION`, `KAFKA_PROPERTIES_SSL_KEY_LOCATION`                                                                                   |
| GMS HTTP hop (`mcp-server-datahub`, `datahub` CLI, ingestion → GMS) | Python `requests` / `httpx` / `certifi`             | `REQUESTS_CA_BUNDLE`                                                                                                                                                                                   |
| OIDC IdP, `datahub-frontend` HTTPS outbound                         | JVM (`pac4j`, Play WS)                              | JVM default `cacerts` (unchanged)                                                                                                                                                                      |

Notes:

- `REQUESTS_CA_BUNDLE` is honored by `requests`, `httpx`, and
  `certifi` without explicit wire-through, covering the Python →
  GMS hop in `mcp-server-datahub`, the `datahub` CLI, and the
  ingestion emitter.
- `datahub-frontend` is a Play (JVM) app with no Kafka client. Its
  HTTPS outbound hops to GMS and the OIDC IdP use the JVM default
  trust store via Play WS.
- The mTLS client identity (cert + key for Kafka) flows through the
  Kafka-client namespaces above; GMS client auth stays token-based
  (`DATAHUB_SYSTEM_CLIENT_*`, PATs).

### Helm chart

#### Today

The operator-facing interface in `acryldata/datahub-helm`
(`charts/datahub/values.yaml`, lines 1278–1299) is two sibling
fields under `global`:

- `global.credentialsAndCertsSecrets` — `{name, path, secureEnv}`
  for the secret mount and password env vars.
- `global.springKafkaConfigurationOverrides` — a free-form map of
  Kafka client properties (locations, protocol, type, …) that the
  chart emits as `SPRING_KAFKA_PROPERTIES_*` env vars.

This interface is format-agnostic: the one documented example is
JKS-only, but an operator can already put
`ssl.keystore.type: PEM` into `springKafkaConfigurationOverrides`
today and the chart will pass it through unchanged.

That gets PEM working **for the Java Kafka broker connection** in
Spring Boot services (GMS, MAE/MCE, `datahub-system-update`) with
no code change, subject to the KIP-651 unresolved question — because
`KafkaEventConsumerFactory` and `DataHubKafkaProducerFactory` call
`KafkaProperties.buildConsumerProperties(null)` /
`buildProducerProperties(null)`, which forwards the env vars
unfiltered into the Java Kafka client.

It does **not** get PEM working for the Java Schema Registry hop,
even if the operator sets everything correctly in
`springKafkaConfigurationOverrides`. `KafkaSchemaRegistryFactory`
reads a hardcoded 5-field `@Value` allowlist
(`ssl.truststore.location`, `ssl.truststore.password`,
`ssl.keystore.location`, `ssl.keystore.password`,
`security.protocol`) and the consumer/producer factories call
`customizedProperties.putAll(getProperties(schemaRegistryConfig))`
**after** Spring Boot's auto-binding, so the factory's allowlisted
props overwrite whatever Spring auto-bound from
`SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SSL_*`. The keys that make
PEM work — `ssl.keystore.type` / `ssl.truststore.type` — are
silently dropped. This is a genuine code gap, fixed in Component
wiring below.

The existing interface also has no story for the Python
`REQUESTS_CA_BUNDLE` hop to GMS, and no single named place to say
"here is my CA / cert / key, wire it everywhere".

#### Proposed

Add a new `global.tls` block that serves as the single named input
for PEM TLS:

```yaml
global:
  tls:
    enabled: true
    ca:
      secretName: datahub-internal-ca
      key: ca.pem
    cert:
      secretName: datahub-workload-tls
      key: tls.crt
    key:
      secretName: datahub-workload-tls
      key: tls.key
    keyPasswordFile: # optional
      secretName: datahub-workload-tls
      key: key.pass
```

The chart mounts the referenced secrets and emits the runtime-native
env vars from the table above on every component that needs them,
matching each component's runtime. `global.tls` is strictly
additive: operators who use `global.credentialsAndCertsSecrets` +
`global.springKafkaConfigurationOverrides` directly — with JKS,
PKCS12, or PEM — keep working byte-for-byte unchanged.

**Python escape hatches, symmetric with the Spring side.** SSL config
has long-tail edge cases (cipher suites, CRL paths, endpoint
identification algorithm, split-PKI trust) that `global.tls` doesn't
express. On the Java side these are covered by the existing
`springKafkaConfigurationOverrides` free-form map, which the chart
emits as `SPRING_KAFKA_PROPERTIES_*` env vars that Spring Boot
auto-binds into the Kafka client config. The Python side gets two
sibling fields, mirroring the Spring/schema-registry split:

```yaml
global:
  # Spring Boot Kafka clients (GMS, MAE, MCE, datahub-system-update)
  springKafkaConfigurationOverrides:
    ssl.cipher.suites: "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256"
    ssl.endpoint.identification.algorithm: ""

  # librdkafka Kafka clients in Python components
  pythonKafkaConfigurationOverrides:
    ssl.cipher.suites: "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256"
    enable.ssl.certificate.verification: "true"

  # librdkafka Schema Registry REST clients in Python components
  pythonKafkaSchemaRegistryConfigurationOverrides:
    ssl.ca.location: /mnt/datahub/certs/schema-registry-ca.pem
```

The chart emits `pythonKafkaConfigurationOverrides` as
`KAFKA_PROPERTIES_*` env vars and
`pythonKafkaSchemaRegistryConfigurationOverrides` as
`KAFKA_SCHEMA_REGISTRY_PROPERTIES_*` env vars on every Python
component with a librdkafka client
(`acryl-datahub-actions`, ingestion pods, `mcp-server-datahub`).
The Python code auto-binds those prefixes into the librdkafka config
dict at client construction time — the same relaxed-binding
ergonomics Spring Boot provides on the Java side. Keys are
librdkafka vocabulary (`ssl.ca.location`, `ssl.certificate.location`,
`ssl.key.location`), which differs from Kafka Java client vocabulary
(`ssl.truststore.location`, `ssl.keystore.location`). Precedence:
`pythonKafka*` overrides win over `global.tls` for the same keys.

### Component wiring

- **Java Kafka client (broker connection)** — no code changes.
  `KafkaEventConsumerFactory` and `DataHubKafkaProducerFactory`
  already call `KafkaProperties.buildConsumerProperties(null)` /
  `buildProducerProperties(null)`
  ([
  `KafkaEventConsumerFactory.java:129`](../../metadata-service/factories/src/main/java/com/linkedin/gms/factory/kafka/KafkaEventConsumerFactory.java),
  [
  `DataHubKafkaProducerFactory.java:89`](../../metadata-service/factories/src/main/java/com/linkedin/gms/factory/kafka/DataHubKafkaProducerFactory.java)),
  which forwards every `SPRING_KAFKA_PROPERTIES_*` env var into the
  Kafka client config unfiltered. `datahub-system-update` reuses the
  same path
  ([
  `SystemUpdateConfig.java:168`](../../datahub-upgrade/src/main/java/com/linkedin/datahub/upgrade/config/SystemUpdateConfig.java)).
  A KIP-651-capable Kafka client consumes `ssl.keystore.type=PEM` +
  `ssl.keystore.location` natively; whether the pinned
  `kafka-clients:8.0.0-ccs` qualifies is an Unresolved question.
- **Java Schema Registry client** — small change in
  `KafkaSchemaRegistryFactory.java`. The factory reads
  `ssl.truststore.location`, `ssl.truststore.password`,
  `ssl.keystore.location`, `ssl.keystore.password`, and
  `security.protocol` via `@Value`
  ([lines 28–41](../../metadata-service/factories/src/main/java/com/linkedin/gms/factory/kafka/schemaregistry/KafkaSchemaRegistryFactory.java))
  and assembles them into a `Map<String, String>` that flows through
  `SerDeKeyValueConfig.getProperties(schemaRegistryConfig)`
  ([`KafkaConfiguration.java:83–90`](../../metadata-service/configuration/src/main/java/com/linkedin/metadata/config/kafka/KafkaConfiguration.java))
  into the Confluent serializer's internal
  `CachedSchemaRegistryClient`. It silently drops
  `ssl.keystore.type` / `ssl.truststore.type`. Adding two `@Value`
  fields and forwarding them to the props map — conditional on
  non-empty — closes the gap. No `SSLContext` / `PemSslContextFactory`
  / in-memory `KeyStore`. Conditional forwarding is required because
  Confluent's `SslFactory` null-checks the type field rather than
  empty-checks it, so forwarding an empty string would turn the
  existing JKS default path into `KeyStore.getInstance("")` and
  throw. The pinned `kafka-avro-serializer:8.0.0` REST client honors
  PEM via its own `SslFactory`
  ([confluentinc/schema-registry@v8.0.0](https://github.com/confluentinc/schema-registry/blob/v8.0.0/client/src/main/java/io/confluent/kafka/schemaregistry/client/security/SslFactory.java)),
  which forks Kafka's `DefaultSslEngineFactory` with explicit
  `PEM_TYPE` branches for both keystore and truststore and supports
  file-mode (`ssl.keystore.location` pointing at a combined PEM
  file), inline mode (`ssl.keystore.certificate.chain` +
  `ssl.keystore.key` as PEM strings), and encrypted private keys via
  `ssl.key.password`.
- **Python `confluent-kafka` Schema Registry clients** — five-file
  fix (`confluent_schema_registry.py`, `kafka.py`, `kafka_emitter.py`,
  `datahub_kafka_reader.py`, `datahub-actions`'
  `kafka_event_source.py`). Each constructs an `ssl.SSLContext` from
  the CA path and passes it via `ssl.ca.location` instead of the raw
  path string — working around
  [#14576](https://github.com/datahub-project/datahub/issues/14576).
  Diffs are in the patches appendix.
- **Python librdkafka env-var auto-binder** — small shared helper
  (~20 lines) that merges `KAFKA_PROPERTIES_*` and
  `KAFKA_SCHEMA_REGISTRY_PROPERTIES_*` env vars into the librdkafka
  `consumer_config` / `producer_config` / `schema_registry_config`
  dicts at client construction time. Called from the same five
  sites as the SSL context fix, so the two changes co-locate. The
  key mapping is `KAFKA_PROPERTIES_SSL_CIPHER_SUITES` →
  `ssl.cipher.suites` (strip prefix, lowercase, `_`→`.`). This gives
  the Python side the same relaxed-binding ergonomics Spring Boot
  provides for the Java side natively and makes
  `pythonKafkaConfigurationOverrides` /
  `pythonKafkaSchemaRegistryConfigurationOverrides` work without
  per-component YAML templating. Env-var-bound keys take precedence
  over values hardcoded in the bundled YAMLs, matching Spring Boot's
  precedence model.
- **`mcp-server-datahub` / `datahub` CLI / Python `DataHubGraph`** —
  no code changes. `requests` / `httpx` / `certifi` all honor
  `REQUESTS_CA_BUNDLE` natively. Client auth to GMS stays
  token-based.
- **`datahub-actions` bundled YAMLs** —
  `docker/datahub-actions/config/executor.yaml` and
  `doc_propagation_action.yaml` gain `consumer_config` /
  `schema_registry_config` blocks wired to the librdkafka env vars
  (`KAFKA_PROPERTIES_SSL_CA_LOCATION`,
  `KAFKA_PROPERTIES_SSL_CERTIFICATE_LOCATION`,
  `KAFKA_PROPERTIES_SSL_KEY_LOCATION`) with sensible defaults. See
  [A.7 / A.8 in the patches appendix](./16975-tls-kafka-oidc-patches.md).
- **`datahub-frontend`** — no code changes. The frontend has no
  Kafka hop (analytics tracking goes browser → GMS
  `TrackingController` → Kafka, not through the frontend). Its
  HTTPS outbound hops to GMS and the OIDC IdP use the JVM default
  trust store via Play WS.

## How we teach this

Adapt `docs/how/kafka-config.md` accordingly, document the helm values
at https://github.com/acryldata/datahub-helm.

## Drawbacks

- Touches three repos (`datahub-project/datahub`,
  `acryldata/datahub-helm`, `acryldata/mcp-server-datahub`).
- Operators who configure TLS today via
  `credentialsAndCertsSecrets` + `springKafkaConfigurationOverrides`
  see no change, but `global.tls` becomes the recommended path,
  creating two supported ways to configure TLS in the chart.

## Alternatives

- **Do nothing.** Operators keep maintaining the ~200-line production
  patch bundle reproduced in the
  [patches appendix](./16975-tls-kafka-oidc-patches.md).
- **Introduce a `DATAHUB_TLS_*` env-var namespace as the runtime
  interface.** Rejected: every runtime in scope already has a
  native env var (see the table in Detailed design), so this would
  add a fifth TLS namespace alongside the existing four without
  replacing any.
- **PKCS12 as canonical format.** Both runtimes support it, but
  cert-manager and Vault emit PEM; adds a conversion step.
- **JKS/PEM dual-path with an init container.** Complex and unnecessary
  given Java Kafka's native PEM support since 2.7.

## Rollout / Adoption Strategy

Non-breaking. Operators who configure TLS today via
`credentialsAndCertsSecrets` + `springKafkaConfigurationOverrides`
keep working unchanged. `global.tls` is additive: if it is unset,
the chart emits nothing new. Rollout steps are independently
shippable and can merge in any order:

1. Python `confluent-kafka` five-file fix: `ssl.ca.location`
   `SSLContext` wrap **and** `KAFKA_PROPERTIES_*` /
   `KAFKA_SCHEMA_REGISTRY_PROPERTIES_*` env-var auto-binder, applied
   at the same client construction sites.
2. `KafkaSchemaRegistryFactory.java` `@Value` addition for
   keystore/truststore `type`.
3. `datahub-actions` bundled YAMLs gain the SSL surface.
4. `mcp-server-datahub` honors `REQUESTS_CA_BUNDLE` (likely already
   does via `httpx` / `requests` defaults — needs a check).
5. Helm `global.tls` block plus `pythonKafkaConfigurationOverrides`
   and `pythonKafkaSchemaRegistryConfigurationOverrides` sibling
   fields landing in `acryldata/datahub-helm`.

## Future Work

- `tls_client_auth` (RFC 8705) for OIDC as a second credential-less
  client-auth method if PR #15479's `private_key_jwt` doesn't cover
  an IdP's requirements.
- Extend `global.tls` wiring to Elasticsearch, Neo4j, JDBC stores.

## Unresolved questions

- **KIP-651 support in the pinned Kafka client.** `build.gradle:221`
  pins `org.apache.kafka:kafka-clients:8.0.0-ccs` (Confluent's
  `-ccs` variant). KIP-651 landed in Apache Kafka 2.7. Whether
  `8.0.0-ccs` tracks an Apache Kafka release at or above 2.7 (i.e.
  supports `ssl.keystore.type=PEM` and `ssl.truststore.type=PEM`
  natively) is not determinable from the repo alone. Needs a check
  against Confluent Platform release notes or a smoke test with
  `SPRING_KAFKA_PROPERTIES_SSL_KEYSTORE_TYPE=PEM` against a real
  broker.
- **`mcp-server-datahub` `REQUESTS_CA_BUNDLE` behavior.** Needs a
  quick check that its HTTP client stack
  (`httpx` / `requests` / `certifi`) actually picks up
  `REQUESTS_CA_BUNDLE` without an explicit wire-through in the
  server's config layer.
- **Fate of the existing `credentialsAndCertsSecrets` +
  `springKafkaConfigurationOverrides` interface.** `global.tls` is
  additive; the existing fields keep working on day one. The open
  question is what happens long-term. Proposal: **fade them out**.
  Concretely: once `global.tls` has shipped and has a released
  chart version behind it, mark the existing fields as deprecated
  in `values.yaml` with a pointer to `global.tls`, replace the
  JKS-only example with a PEM `global.tls` example, and remove the
  deprecated fields after a reasonable deprecation window (e.g. two
  chart minor versions). Operators with JKS who don't want to
  convert would need to migrate before the removal window closes.
  Needs agreement from the chart maintainers on the timeline and
  on whether removal is acceptable at all, or whether the old
  interface stays indefinitely as an escape hatch.
- Any negative side-effects / complications from committing to PEM as
  the default format?
- Any further code adaptations needed beyond the ones listed in
  Requirements?
- **Combined cert+key file for the Java Kafka keystore.** Kafka's
  PEM file-mode keystore (`ssl.keystore.type=PEM` +
  `ssl.keystore.location=/path/file.pem`) expects a single file
  containing the cert chain **and** the private key. `global.tls`
  exposes `cert` and `key` as two separate secret references to
  match what cert-manager, Vault, and External Secrets produce.
  The Helm chart therefore needs a way to materialize the combined
  file inside the pod — projected volume, init container, or
  similar — without requiring the operator to pre-concatenate.
  Needs a decision on which mechanism the chart uses. librdkafka
  and the Python clients consume the separate files directly and
  are unaffected.

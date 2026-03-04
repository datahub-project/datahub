# DataHub Helm values template for per-worktree deployment.
# Variables are expanded by string.Template at deploy time:
#   DH_WORKTREE_NAME, DH_NAMESPACE, DH_DB_NAME, DH_ES_PREFIX,
#   DH_KAFKA_PREFIX, DH_HOSTNAME, DH_VERSION,
#   DH_MYSQL_HOST, DH_MYSQL_PORT, DH_KAFKA_HOST, DH_KAFKA_PORT,
#   DH_ES_HOST, DH_ES_PORT, DH_RELEASE_NAME

# ── Global ──────────────────────────────────────────────────────────────────
global:
  graph_service_impl: elasticsearch
  datahub_standalone_consumers_enabled: false

  elasticsearch:
    host: "${DH_ES_HOST}"
    port: "${DH_ES_PORT}"
    indexPrefix: "${DH_ES_PREFIX}"
    skipcheck: "false"

  kafka:
    bootstrap:
      server: "${DH_KAFKA_HOST}:${DH_KAFKA_PORT}"
    schemaregistry:
      # GMS internal schema registry — no external Schema Registry needed.
      # URL is auto-configured by chart templates based on release name.
      type: INTERNAL
    precreateTopics: true

    # Per-worktree topic names.
    # The chart has NO topicPrefix mechanism — each topic must be set individually.
    topics:
      metadata_change_event_name: "${DH_KAFKA_PREFIX}MetadataChangeEvent_v4"
      failed_metadata_change_event_name: "${DH_KAFKA_PREFIX}FailedMetadataChangeEvent_v4"
      metadata_audit_event_name: "${DH_KAFKA_PREFIX}MetadataAuditEvent_v4"
      datahub_usage_event_name: "${DH_KAFKA_PREFIX}DataHubUsageEvent_v1"
      metadata_change_proposal_topic_name: "${DH_KAFKA_PREFIX}MetadataChangeProposal_v1"
      failed_metadata_change_proposal_topic_name: "${DH_KAFKA_PREFIX}FailedMetadataChangeProposal_v1"
      metadata_change_log_versioned_topic_name: "${DH_KAFKA_PREFIX}MetadataChangeLog_Versioned_v1"
      metadata_change_log_timeseries_topic_name: "${DH_KAFKA_PREFIX}MetadataChangeLog_Timeseries_v1"
      platform_event_topic_name: "${DH_KAFKA_PREFIX}PlatformEvent_v1"
      datahub_upgrade_history_topic_name: "${DH_KAFKA_PREFIX}DataHubUpgradeHistory_v1"

    # Per-worktree consumer group IDs to prevent cross-worktree interference.
    consumer_groups:
      # Per-component upgrade history consumers (chart defaults to release-name-prefixed)
      datahub_upgrade_history_kafka_consumer_group_id:
        gms: "${DH_RELEASE_NAME}-duhe-consumer-job-client-gms"
        mae-consumer: "${DH_RELEASE_NAME}-duhe-consumer-job-client-mcl"
        mce-consumer: "${DH_RELEASE_NAME}-duhe-consumer-job-client-mcp"
      # GMS consumer groups
      datahub_usage_event_kafka_consumer_group_id: "${DH_WORKTREE_NAME}-datahub-usage-event-consumer"
      metadata_change_log_kafka_consumer_group_id: "${DH_WORKTREE_NAME}-generic-mae-consumer"
      platform_event_kafka_consumer_group_id: "${DH_WORKTREE_NAME}-generic-platform-event"
      metadata_change_event_kafka_consumer_group_id: "${DH_WORKTREE_NAME}-mce-consumer"
      metadata_change_proposal_kafka_consumer_group_id: "${DH_WORKTREE_NAME}-generic-mce-consumer"
      # Actions consumer groups
      datahub_actions_doc_propagation_consumer_group_id: "${DH_WORKTREE_NAME}-datahub-doc-propagation-action"
      datahub_actions_ingestion_executor_consumer_group_id: "${DH_WORKTREE_NAME}-ingestion-executor"
      datahub_actions_slack_consumer_group_id: "${DH_WORKTREE_NAME}-datahub-slack-action"
      datahub_actions_teams_consumer_group_id: "${DH_WORKTREE_NAME}-datahub-teams-action"

    # Per-worktree MCL hook consumer group suffixes
    metadataChangeLog:
      hooks:
        siblings:
          consumerGroupSuffix: "-${DH_WORKTREE_NAME}"
        updateIndices:
          consumerGroupSuffix: "-${DH_WORKTREE_NAME}"
        ingestionScheduler:
          consumerGroupSuffix: "-${DH_WORKTREE_NAME}"
        incidents:
          consumerGroupSuffix: "-${DH_WORKTREE_NAME}"
        entityChangeEvents:
          consumerGroupSuffix: "-${DH_WORKTREE_NAME}"
        forms:
          consumerGroupSuffix: "-${DH_WORKTREE_NAME}"

  sql:
    datasource:
      host: "${DH_MYSQL_HOST}:${DH_MYSQL_PORT}"
      hostForMysqlClient: "${DH_MYSQL_HOST}"
      port: "${DH_MYSQL_PORT}"
      url: "jdbc:mysql://${DH_MYSQL_HOST}:${DH_MYSQL_PORT}/${DH_DB_NAME}?verifyServerCertificate=false&useSSL=true&useUnicode=yes&characterEncoding=UTF-8&enabledTLSProtocols=TLSv1.2"
      driver: "com.mysql.cj.jdbc.Driver"
      username: "root"
      password:
        secretRef: mysql-secrets
        secretKey: mysql-root-password

  datahub:
    version: "${DH_VERSION}"
    gms:
      port: "8080"
    monitoring:
      enablePrometheus: false
    managed_ingestion:
      enabled: true
    systemUpdate:
      enabled: true
    metadata_service_authentication:
      enabled: false

# ── Prerequisites (all disabled — using shared infra namespace) ─────────────
elasticsearch:
  enabled: false
mysql:
  enabled: false
kafka:
  enabled: false
cp-schema-registry:
  enabled: false
neo4j:
  enabled: false
opensearch:
  enabled: false

# ── GMS (with embedded MAE/MCE/PE consumers) ───────────────────────────────
datahub-gms:
  enabled: true
  service:
    type: ClusterIP
  image:
    repository: "acryldata/datahub-gms"
    tag: "${DH_VERSION}"
    pullPolicy: "IfNotPresent"
  resources:
    requests:
      memory: 512Mi
      cpu: 200m
    limits:
      memory: 1536Mi
  extraEnvs:
    - name: MAE_CONSUMER_ENABLED
      value: "true"
    - name: MCE_CONSUMER_ENABLED
      value: "true"
    - name: PE_CONSUMER_ENABLED
      value: "true"
    - name: UI_INGESTION_ENABLED
      value: "true"
    - name: ENTITY_SERVICE_ENABLE_RETENTION
      value: "true"
    - name: ES_BULK_REFRESH_POLICY
      value: "NONE"
    - name: JAVA_OPTS
      value: "-Xms512m -Xmx512m"
    - name: DATAHUB_SYSTEM_CLIENT_ID
      value: "__datahub_system"
    - name: DATAHUB_SYSTEM_CLIENT_SECRET
      value: "JohnSnowKnowsNothing"

# ── Frontend ────────────────────────────────────────────────────────────────
datahub-frontend:
  enabled: true
  service:
    type: ClusterIP
  image:
    repository: "acryldata/datahub-frontend-react"
    tag: "${DH_VERSION}"
    pullPolicy: "IfNotPresent"
  resources:
    requests:
      memory: 256Mi
      cpu: 100m
    limits:
      memory: 768Mi
  extraEnvs:
    - name: DATAHUB_SECRET
      value: "YouKnowNothing"
    - name: DATAHUB_APP_VERSION
      value: "${DH_VERSION}"
    - name: DATAHUB_PLAY_MEM_BUFFER_SIZE
      value: "10MB"
    - name: JAVA_OPTS
      value: "-Xms256m -Xmx256m -Dhttp.port=9002 -Dconfig.file=datahub-frontend/conf/application.conf -Djava.security.auth.login.config=datahub-frontend/conf/jaas.conf -Dlogback.configurationFile=datahub-frontend/conf/logback.xml -Dlogback.debug=false -Dpidfile.path=/dev/null"
    - name: METADATA_SERVICE_AUTH_ENABLED
      value: "false"
  ingress:
    enabled: true
    annotations:
      traefik.ingress.kubernetes.io/router.entrypoints: web,websecure
    hosts:
      - host: "${DH_HOSTNAME}"
        paths:
          - /

# ── Actions ─────────────────────────────────────────────────────────────────
acryl-datahub-actions:
  enabled: true
  image:
    repository: "acryldata/datahub-actions"
    tag: "${DH_VERSION}-slim"
    pullPolicy: "IfNotPresent"
  resources:
    requests:
      memory: 128Mi
      cpu: 50m
    limits:
      memory: 384Mi
  extraEnvs:
    - name: DATAHUB_SYSTEM_CLIENT_ID
      value: "__datahub_system"
    - name: DATAHUB_SYSTEM_CLIENT_SECRET
      value: "JohnSnowKnowsNothing"
    - name: KAFKA_PROPERTIES_SECURITY_PROTOCOL
      value: "PLAINTEXT"

# ── Separate consumers (disabled — embedded in GMS) ─────────────────────────
datahub-mae-consumer:
  enabled: false
datahub-mce-consumer:
  enabled: false

# ── Setup jobs ──────────────────────────────────────────────────────────────
elasticsearchSetupJob:
  enabled: true
  image:
    repository: "acryldata/datahub-elasticsearch-setup"
    tag: "${DH_VERSION}"
    pullPolicy: "IfNotPresent"

# Disabled: system-update handles topic creation in v1.3.0+
kafkaSetupJob:
  enabled: false

# Disabled: mysql-setup job hardcodes the "datahub" DB name.
# DB and tables are created by _ensure_database in deploy.sh.
mysqlSetupJob:
  enabled: false

datahubSystemUpdate:
  enabled: true
  image:
    repository: "acryldata/datahub-upgrade"
    tag: "${DH_VERSION}"
    pullPolicy: "IfNotPresent"
    # Wrap entrypoint with timeout to prevent JVM daemon-thread hangs after completion.
    # timeout exits 124 on timeout, which we treat as success (work was done, JVM hung).
    command: ["/bin/bash", "-c", "timeout 600 /datahub/datahub-upgrade/scripts/start.sh \"$$@\" || [ $$? -eq 124 ]", "--"]
  nonblocking:
    image:
      command: ["/bin/bash", "-c", "timeout 600 /datahub/datahub-upgrade/scripts/start.sh \"$$@\" || [ $$? -eq 124 ]", "--"]

datahubUpgrade:
  enabled: true
  image:
    repository: "acryldata/datahub-upgrade"
    tag: "${DH_VERSION}"
    pullPolicy: "IfNotPresent"

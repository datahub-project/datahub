package com.linkedin.metadata.system_info.collectors;

import static org.testng.Assert.*;

import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.system_info.PropertyInfo;
import com.linkedin.metadata.system_info.SystemPropertiesInfo;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * Integration test that validates all configuration properties are explicitly classified as either
 * sensitive (should be redacted) or non-sensitive (should not be redacted).
 *
 * <p>This test prevents sensitive data leaks by requiring conscious classification of every
 * configuration property. If a new property is added, this test will fail until the property is
 * explicitly added to either SENSITIVE_PROPERTIES or NON_SENSITIVE_PROPERTIES.
 */
@Slf4j
@SpringBootTest(classes = {PropertiesCollectorConfigurationTest.TestConfiguration.class})
public class PropertiesCollectorConfigurationTest extends AbstractTestNGSpringContextTests {

  @Autowired private PropertiesCollector propertiesCollector;

  @Configuration
  @PropertySource(value = "classpath:/application.yaml", factory = YamlPropertySourceFactory.class)
  static class TestConfiguration {

    @Bean
    public PropertiesCollector propertiesCollector(Environment environment) {
      return new PropertiesCollector(environment);
    }
  }

  /**
   * Property keys that contain sensitive data and MUST be redacted. If you add a new sensitive
   * configuration property, add its key here.
   */
  private static final Set<String> SENSITIVE_PROPERTIES =
      Set.of(
          "password",
          "trustStorePassword",
          "keyPassword",
          "encryptionKey",
          "signingKey",
          "systemClientSecret",
          "datahub.gms.truststore.password",
          "datasourcePassword",
          "keyStorePassword",
          "salt",
          // Database passwords
          "ebean.password",
          "cassandra.datasourcePassword",
          "neo4j.password",
          // Elasticsearch security
          "elasticsearch.password",
          "elasticsearch.sslContext.keyPassword",
          "elasticsearch.sslContext.trustStorePassword",
          "elasticsearch.sslContext.keyStorePassword",
          // Services encryption
          "secretService.encryptionKey",
          // Environment variables that may contain sensitive paths/credentials
          "GIT_ASKPASS", // Can contain path to credential helper
          "PWD", // Current directory may contain sensitive info
          // CDC db password
          "mclProcessing.cdcSource.debeziumConfig.config.database.password");

  /**
   * Template patterns for sensitive properties that contain dynamic parts. Use [*] for numeric
   * indices and * for single property segments.
   */
  private static final Set<String> SENSITIVE_PROPERTY_TEMPLATES =
      Set.of(
          // Authentication and token service with dynamic indices
          "authentication.systemClientSecret",
          "authentication.tokenService.signingKey",
          "authentication.tokenService.salt",
          "authentication.authenticators[*].configs.signingKey",
          "authentication.authenticators[*].configs.salt");

  /**
   * Template patterns for non-sensitive configuration properties that contain dynamic parts. Use
   * [*] for numeric indices and * for single property segments. Note: Environment variables are
   * handled separately and don't need templates here.
   */
  private static final Set<String> NON_SENSITIVE_PROPERTY_TEMPLATES =
      Set.of(
          // Authentication configuration (not credentials) with dynamic indices
          "authentication.authenticators[*].type",
          "authentication.authenticators[*].configs.enabled",
          "authentication.authenticators[*].configs.guestUser",
          // Spring autoconfigure exclusions with dynamic indices
          "spring.autoconfigure.exclude[*]",
          // Cache configuration with dynamic entity/aspect combinations
          "cache.client.entityClient.entityAspectTTLSeconds.*.*",
          // Gradle test worker properties (Java system properties)
          "org.gradle.test.worker*",
          // System update properties
          "systemUpdate.*.enabled",
          "systemUpdate.*.batchSize",
          "systemUpdate.*.limit",
          "systemUpdate.*.delayMs",

          // Consistency checks configuration
          "consistencyChecks.checks.*.*",
          "consistencyChecks.gracePeriodSeconds",

          // Kafka topic Configs
          "kafka.topics.*.name",
          "kafka.topics.*.partitions",
          "kafka.topics.*.enabled",
          "kafka.topics.*.replicationFactor",
          "kafka.topics.*.configProperties.max.message.bytes",
          "kafka.topics.*.configProperties.retention.ms",
          "kafka.topicDefaults.configProperties.max.message.bytes",
          "kafka.topicDefaults.configProperties.retention.ms",
          "kafka.topicDefaults.partitions",
          "kafka.topicDefaults.replicationFactor",
          "kafka.setup.preCreateTopics",
          "kafka.setup.useConfluentSchemaRegistry",
          // IAM authentication flags
          "*.postgresUseIamAuth",
          "*.opensearchUseAwsIamAuth",
          // Bulk rules
          "featureFlags.*",
          "*.*nabled",
          "*.*.*nabled",
          "*.*.*.*nabled",
          "*.*.*.*.*nabled",
          "*.consumerGroupSuffix",
          "*.*.consumerGroupSuffix",
          "*.*.*.consumerGroupSuffix",
          "authentication.authenticators[*].configs.trustedIssuers",
          "authentication.authenticators[*].configs.allowedAudiences",
          "authentication.authenticators[*].configs.jwksUri",
          "authentication.authenticators[*].configs.userIdClaim",
          "authentication.authenticators[*].configs.algorithm",
          "authentication.authenticators[*].configs.discoveryUri",
          // Shim properties
          "elasticsearch.shim.*");

  /**
   * Property keys that should NOT be redacted. Add new non-sensitive properties here when they are
   * added to configuration.
   *
   * <p>Note: This includes properties that may contain sensitive keywords but are actually
   * configuration settings, not secrets.
   */
  private static final Set<String> NON_SENSITIVE_PROPERTIES =
      Set.of(
          // Authentication and authorization configuration (not credentials)
          "authentication",
          "authentication.systemClientId",
          "authenticators",
          "logAuthenticatorExceptions",
          "authorization",
          "authorization.defaultAuthorizer.cachePolicyFetchSize",
          "authorization.defaultAuthorizer.cacheRefreshIntervalSecs",
          "authorization.restApiAuthorization",
          "authorization.view.recommendations.peerGroupEnabled",
          "defaultAuthorizer",
          "restApiAuthorization",
          "authentication.enabled",
          "authentication.enforceExistenceEnabled",
          "authentication.excludedPaths",
          "authentication.logAuthenticatorExceptions",
          "authentication.sessionTokenDurationMs",
          "authentication.tokenService.issuer",
          "authentication.tokenService.signingAlgorithm",
          "authorization.defaultAuthorizer.enabled",
          "authorization.view.enabled",
          // Service and component names
          "secretService",
          "secretService.v1AlgorithmEnabled",
          "tokenService",
          // Configuration keys and settings (not secret keys)
          "key",
          "corpUserKey",
          "structuredPropertyKey",
          "keyStoreType",
          "keyStoreFile",
          "keyspace",
          "cassandra.keyspace",
          "maxObjectKeys",
          "elasticsearch.index.maxObjectKeys",
          "mainTokenizer",
          "elasticsearch.index.mainTokenizer",
          // Cache configuration
          "cache.client.entityClient.entityAspectTTLSeconds.corpuser.corpUserCredentials",
          "cache.client.entityClient.entityAspectTTLSeconds.corpuser.corpUserKey",
          "cache.client.entityClient.entityAspectTTLSeconds.corpuser.corpUserSettings",
          "cache.client.entityClient.entityAspectTTLSeconds.corpuser.corpUserStatus",
          "cache.client.entityClient.entityAspectTTLSeconds.corpuser.corpUserInfo",
          "cache.client.entityClient.entityAspectTTLSeconds.corpuser.corpUserEditableInfo",
          "cache.client.entityClient.entityAspectTTLSeconds.corpuser.globalTags",
          "cache.client.entityClient.entityAspectTTLSeconds.corpuser.groupMembership",
          "cache.client.entityClient.entityAspectTTLSeconds.corpuser.nativeGroupMembership",
          "cache.client.entityClient.entityAspectTTLSeconds.corpuser.roleMembership",
          "cache.client.entityClient.entityAspectTTLSeconds.corpuser.status",
          "cache.client.entityClient.entityAspectTTLSeconds.structuredProperty.structuredPropertyKey",
          "cache.client.entityClient.entityAspectTTLSeconds.structuredProperty.propertyDefinition",
          "cache.client.entityClient.entityAspectTTLSeconds.structuredProperty.status",
          "cache.client.entityClient.entityAspectTTLSeconds.dataset.usageFeatures",
          "cache.client.entityClient.entityAspectTTLSeconds.dataset.storageFeatures",
          "cache.client.entityClient.entityAspectTTLSeconds.dashboard.dashboardUsageStatistics",
          "cache.client.entityClient.entityAspectTTLSeconds.chart.usageFeatures",
          "cache.client.entityClient.entityAspectTTLSeconds.telemetry.telemetryClientId",
          "cache.client.entityClient.defaultTTLSeconds",
          "cache.client.entityClient.enabled",
          "cache.client.entityClient.maxBytes",
          "cache.client.entityClient.statsEnabled",
          "cache.client.entityClient.statsIntervalSeconds",
          "cache.client.usageClient.defaultTTLSeconds",
          "cache.client.usageClient.enabled",
          "cache.client.usageClient.maxBytes",
          "cache.client.usageClient.statsEnabled",
          "cache.client.usageClient.statsIntervalSeconds",
          "cache.homepage.entityCounts.ttlSeconds",
          "cache.primary.maxSize",
          "cache.primary.ttlSeconds",
          "cache.search.lineage.lightningThreshold",
          "cache.search.lineage.ttlSeconds",
          // Authentication mode flags (not credentials)
          "opensearchUseAwsIamAuth",
          "postgresUseIamAuth",
          // Time durations and settings
          "sessionTokenDurationMs",
          // Kafka serializer/deserializer class names
          "kafka.serde.event.key.serializer",
          "kafka.serde.event.key.deserializer",
          "kafka.serde.event.key.delegateDeserializer",
          "kafka.serde.usageEvent.key.serializer",
          "kafka.serde.usageEvent.key.deserializer",
          "kafka.serde.event.value.serializer",
          "kafka.serde.event.value.deserializer",
          "kafka.serde.event.value.delegateDeserializer",
          "kafka.serde.usageEvent.value.serializer",
          "kafka.serde.usageEvent.value.deserializer",
          "kafka.bootstrapServers",
          "kafka.consumer.bootstrapServers",
          "kafka.consumer.healthCheckEnabled",
          "kafka.consumer.maxPartitionFetchBytes",
          "kafka.consumer.mcl.aspectsToDrop",
          "kafka.consumer.mcl.autoOffsetReset",
          "kafka.consumer.mcl.fineGrainedLoggingEnabled",
          "kafka.consumer.mcp.autoOffsetReset",
          "kafka.consumer.metrics.maxExpectedValue",
          "kafka.consumer.metrics.percentiles",
          "kafka.consumer.metrics.slo",
          "kafka.consumer.pe.autoOffsetReset",
          "kafka.consumer.stopOnDeserializationError",
          "kafka.consumerPool.initialSize",
          "kafka.consumerPool.maxSize",
          "kafka.consumerPool.validationTimeoutSeconds",
          "kafka.consumerPool.validationCacheIntervalMinutes",
          "kafka.listener.concurrency",
          "kafka.producer.backoffTimeout",
          "kafka.producer.bootstrapServers",
          "kafka.producer.compressionType",
          "kafka.producer.deliveryTimeout",
          "kafka.producer.maxRequestSize",
          "kafka.producer.requestTimeout",
          "kafka.producer.retryCount",
          "kafka.schemaRegistry.awsGlue.region",
          "kafka.schemaRegistry.awsGlue.registryName",
          "kafka.schemaRegistry.type",
          "kafka.schemaRegistry.url",
          "kafka.schema.registry.security.protocol",
          "kafka.topics.dataHubUsage",
          "spring.kafka.security.protocol",
          // Entity/field names that happen to contain sensitive keywords
          "corpUserCredentials",
          // Environment variables (paths and non-sensitive settings)
          "auth",
          "SSH_AUTH_SOCK",
          "VSCODE_GIT_ASKPASS_MAIN",
          "VSCODE_GIT_ASKPASS_NODE",
          "VSCODE_GIT_ASKPASS_EXTRA_ARGS",
          "VSCODE_GIT_IPC_HANDLE",
          "VSCODE_INJECTION",
          "VSCODE_PROFILE_INITIALIZED",
          "VSCODE_DEBUGPY_ADAPTER_ENDPOINTS",
          "APPLICATION_NAME",
          "BUNDLED_DEBUGPY_PATH",
          "COLORTERM",
          "COMMAND_MODE",
          "COMPOSER_NO_INTERACTION",
          "CONSOLE_LOG_CHARSET",
          "CURSOR_AGENT",
          "CURSOR_TRACE_ID",
          "DISABLE_AUTO_UPDATE",
          "FILE_LOG_CHARSET",
          "FNM_ARCH",
          "FNM_COREPACK_ENABLED",
          "FNM_DIR",
          "FNM_LOGLEVEL",
          "FNM_MULTISHELL_PATH",
          "FNM_NODE_DIST_MIRROR",
          "FNM_RESOLVE_ENGINES",
          "FNM_VERSION_FILE_STRATEGY",
          "HOMEBREW_CELLAR",
          "HOMEBREW_PREFIX",
          "HOMEBREW_REPOSITORY",
          "HOME",
          "INFOPATH",
          "JAVA_HOME",
          "LANG",
          "LOGGED_APPLICATION_NAME",
          "LOGNAME",
          "MallocNanoZone",
          "ORIGINAL_XDG_CURRENT_DESKTOP",
          "PAGER",
          "PATH",
          "PID",
          "PIP_NO_INPUT",
          "PYDEVD_DISABLE_FILE_VALIDATION",
          "PYENV_SHELL",
          "SHELL",
          "SHLVL",
          "STRICT_URN_VALIDATION_ENABLED",
          "TERM",
          "TERM_PROGRAM",
          "TERM_PROGRAM_VERSION",
          "TMPDIR",
          "USER",
          "USER_ZDOTDIR",
          "XPC_FLAGS",
          "XPC_SERVICE_NAME",
          "ZDOTDIR",
          "__CFBundleIdentifier",
          "__CF_USER_TEXT_ENCODING",
          "ftp.nonProxyHosts",
          "http.nonProxyHosts",
          "npm_config_yes",
          "socksNonProxyHosts",
          "ELASTIC_VERSION",
          // Java system properties
          "apple.awt.application.name",
          "file.encoding",
          "file.separator",
          "stderr.encoding",
          "stdout.encoding",
          "java.awt.headless",
          "java.class.path",
          "java.class.version",
          "java.home",
          "java.io.tmpdir",
          "java.library.path",
          "java.runtime.name",
          "java.runtime.version",
          "java.specification.maintenance.version",
          "java.specification.name",
          "java.specification.vendor",
          "java.specification.version",
          "java.vendor",
          "java.vendor.url",
          "java.vendor.url.bug",
          "java.vendor.version",
          "java.version",
          "java.version.date",
          "java.vm.compressedOopsMode",
          "java.vm.info",
          "java.vm.name",
          "java.vm.specification.name",
          "java.vm.specification.vendor",
          "java.vm.specification.version",
          "java.vm.vendor",
          "java.vm.version",
          "jdk.debug",
          "line.separator",
          "native.encoding",
          "os.arch",
          "os.name",
          "os.version",
          "path.separator",
          "sun.arch.data.model",
          "sun.boot.library.path",
          "sun.cpu.endian",
          "sun.io.unicode.encoding",
          "sun.java.command",
          "sun.java.launcher",
          "sun.jnu.encoding",
          "sun.management.compiler",
          // JNA (Java Native Access) properties
          "jna.loaded",
          "jna.platform.library.path",
          "jnidispatch.path",
          // Java CRaC (Coordinated Restore at Checkpoint) properties
          "sun.java.crac_command",
          "user.country",
          "user.dir",
          "user.home",
          "user.language",
          "user.name",
          "user.timezone",
          "user.variant",
          // Spring and application configuration
          "baseUrl",
          "bootstrap.policies.file",
          "bootstrap.servlets.waitTimeout",
          "configEntityRegistry.path",
          "configEntityRegistry.resource",
          "spring.application.name",
          "spring.application.pid",
          "spring.error.include-exception",
          "spring.error.include-message",
          "spring.error.include-stacktrace",
          "spring.error.whitelabel.enabled",
          "spring.jmx.enabled",
          "spring.mvc.servlet.path",
          "spring.mvc.throw-exception-if-no-handler-found",
          "spring.web.resources.add-mappings",
          "server.server-header",
          // DataHub configuration
          "datahub.gms.async.request-timeout-ms",
          "datahub.gms.host",
          "datahub.gms.port",
          "datahub.gms.sslContext.protocol",
          "datahub.gms.truststore.path",
          "datahub.gms.truststore.type",
          "datahub.gms.uri",
          "datahub.gms.useSSL",
          "datahub.metrics.hookLatency.maxExpectedValue",
          "datahub.metrics.hookLatency.percentiles",
          "datahub.metrics.hookLatency.slo",
          "datahub.plugin.auth.path",
          "datahub.plugin.entityRegistry.loadDelaySeconds",
          "datahub.plugin.entityRegistry.ignoreFailureWhenLoadingRegistry",
          "datahub.plugin.entityRegistry.path",
          "datahub.plugin.pluginSecurityMode",
          "datahub.plugin.retention.path",
          "datahub.serverEnv",
          "datahub.serverType",
          "datahub.s3.bucketName",
          "datahub.s3.roleArn",
          "datahub.s3.presignedUploadUrlExpirationSeconds",
          "datahub.s3.presignedDownloadUrlExpirationSeconds",
          "datahub.s3.assetPathPrefix",
          "datahub.readOnly",
          // Feature flags
          "featureFlags.alwaysEmitChangeLog",
          "featureFlags.alternateMCPValidation",
          "featureFlags.assetSummaryPageV1",
          "featureFlags.datasetSummaryPageV1",
          "featureFlags.businessAttributeEntityEnabled",
          "featureFlags.cdcModeChangeLog",
          "featureFlags.dataContractsEnabled",
          "featureFlags.documentationFileUploadV1",
          "featureFlags.editableDatasetNameEnabled",
          "featureFlags.entityVersioning",
          "featureFlags.erModelRelationshipFeatureEnabled",
          "featureFlags.fineGrainedLineageNotAllowedForPlatforms",
          "featureFlags.graphServiceDiffModeEnabled",
          "featureFlags.hideDbtSourceInLineage",
          "featureFlags.lineageGraphV2",
          "featureFlags.lineageGraphV3",
          "featureFlags.lineageSearchCacheEnabled",
          "featureFlags.logicalModelsEnabled",
          "featureFlags.nestedDomainsEnabled",
          "featureFlags.platformBrowseV2",
          "elasticsearch.search.pointInTimeCreationEnabled",
          "featureFlags.preProcessHooks.reprocessEnabled",
          "featureFlags.preProcessHooks.uiEnabled",
          "featureFlags.readOnlyModeEnabled",
          "featureFlags.schemaFieldCLLEnabled",
          "featureFlags.schemaFieldEntityFetchEnabled",
          "featureFlags.schemaFieldLineageIgnoreStatus",
          "featureFlags.searchServiceDiffModeEnabled",
          "featureFlags.showAccessManagement",
          "featureFlags.showAcrylInfo",
          "featureFlags.showAutoCompleteResults",
          "featureFlags.showBrowseV2",
          "featureFlags.showHasSiblingsFilter",
          "featureFlags.showHomePageRedesign",
          "featureFlags.showHomepageUserRole",
          "featureFlags.showIngestionPageRedesign",
          "featureFlags.ingestionOnboardingRedesignV1",
          "featureFlags.showIntroducePage",
          "featureFlags.showLineageExpandMore",
          "featureFlags.showManageStructuredProperties",
          "featureFlags.showManageTags",
          "featureFlags.showNavBarRedesign",
          "featureFlags.showProductUpdates",
          "featureFlags.productUpdatesJsonUrl",
          "featureFlags.productUpdatesJsonFallbackResource",
          "featureFlags.showStatsTabRedesign",
          "featureFlags.showSearchBarAutocompleteRedesign",
          "featureFlags.showSearchFiltersV2",
          "featureFlags.showSeparateSiblings",
          "featureFlags.showSimplifiedHomepageByDefault",
          "featureFlags.themeV2Default",
          "featureFlags.themeV2Enabled",
          "featureFlags.themeV2Toggleable",
          // Database configuration (non-sensitive settings)
          "cassandra.datacenter",
          "cassandra.datasourceUsername",
          "cassandra.hosts",
          "cassandra.port",
          "cassandra.useSsl",
          "ebean.autoCreateDdl",
          "ebean.batchGetMethod",
          "ebean.cloudProvider",
          "ebean.driver",
          "ebean.leakTimeMinutes",
          "ebean.maxAgeMinutes",
          "ebean.maxConnections",
          "ebean.maxInactiveTimeSeconds",
          "ebean.minConnections",
          "ebean.url",
          "ebean.useIamAuth",
          "ebean.username",
          "ebean.waitTimeoutMillis",
          "neo4j.connectionLivenessCheckTimeout",
          "neo4j.database",
          "neo4j.maxConnectionAcquisitionTimeout",
          "neo4j.maxConnectionLifetimeInSeconds",
          "neo4j.maxConnectionPoolSize",
          "neo4j.maxTransactionRetryTime",
          "neo4j.uri",
          "neo4j.username",
          // Elasticsearch configuration
          "elasticsearch.buildIndices.allowDocCountMismatch",
          "elasticsearch.buildIndices.cloneIndices",
          "elasticsearch.buildIndices.reindexOptimizationEnabled",
          "elasticsearch.buildIndices.retentionUnit",
          "elasticsearch.buildIndices.retentionValue",
          "elasticsearch.bulkDelete.batchSize",
          "elasticsearch.bulkDelete.numRetries",
          "elasticsearch.bulkDelete.pollInterval",
          "elasticsearch.bulkDelete.pollIntervalUnit",
          "elasticsearch.bulkDelete.slices",
          "elasticsearch.bulkDelete.timeout",
          "elasticsearch.bulkDelete.timeoutUnit",
          "elasticsearch.bulkProcessor.async",
          "elasticsearch.bulkProcessor.enableBatchDelete",
          "elasticsearch.bulkProcessor.flushPeriod",
          "elasticsearch.bulkProcessor.numRetries",
          "elasticsearch.bulkProcessor.refreshPolicy",
          "elasticsearch.bulkProcessor.requestsLimit",
          "elasticsearch.bulkProcessor.sizeLimit",
          "elasticsearch.bulkProcessor.threadCount",
          "elasticsearch.dataNodeCount",
          "elasticsearch.bulkProcessor.retryInterval",
          "elasticsearch.connectionRequestTimeout",
          "elasticsearch.host",
          "elasticsearch.idHashAlgo",
          "elasticsearch.implementation",
          "elasticsearch.index.docIds.schemaField.hashIdEnabled",
          "elasticsearch.index.enableMappingsReindex",
          "elasticsearch.index.enableSettingsReindex",
          "elasticsearch.index.entitySettingsOverrides",
          "elasticsearch.index.maxArrayLength",
          "elasticsearch.index.maxReindexHours",
          "elasticsearch.index.maxValueLength",
          "elasticsearch.index.minSearchFilterLength",
          "elasticsearch.index.numReplicas",
          "elasticsearch.index.numRetries",
          "elasticsearch.index.numShards",
          "elasticsearch.index.prefix",
          "elasticsearch.index.refreshIntervalSeconds",
          "elasticsearch.index.settingsOverrides",
          "elasticsearch.pathPrefix",
          "elasticsearch.port",
          "elasticsearch.region",
          "elasticsearch.search.custom.autoCompleteFieldConfigDefault",
          "elasticsearch.search.custom.enabled",
          "elasticsearch.search.custom.file",
          "elasticsearch.search.custom.searchFieldConfigDefault",
          "elasticsearch.search.exactMatch.caseSensitivityFactor",
          "elasticsearch.search.exactMatch.enableStructured",
          "elasticsearch.search.exactMatch.exactFactor",
          "elasticsearch.search.exactMatch.exclusive",
          "elasticsearch.search.exactMatch.prefixFactor",
          "elasticsearch.search.exactMatch.withPrefix",
          "elasticsearch.search.graph.batchSize",
          "elasticsearch.search.graph.boostViaNodes",
          "elasticsearch.search.graph.enableMultiPathSearch",
          "elasticsearch.search.graph.graphStatusEnabled",
          "elasticsearch.search.graph.impact.keepAlive",
          "elasticsearch.search.graph.impact.maxHops",
          "elasticsearch.search.graph.impact.maxRelations",
          "elasticsearch.search.graph.impact.partialResults",
          "elasticsearch.search.graph.impact.searchQueryTimeReservation",
          "elasticsearch.search.graph.impact.slices",
          "elasticsearch.search.graph.lineageMaxHops",
          "elasticsearch.search.graph.maxThreads",
          "elasticsearch.search.graph.pointInTimeCreationEnabled",
          "elasticsearch.search.graph.queryOptimization",
          "elasticsearch.search.graph.timeoutSeconds",
          "elasticsearch.search.maxTermBucketSize",
          "elasticsearch.search.partial.factor",
          "elasticsearch.search.partial.urnFactor",
          "elasticsearch.search.wordGram.fourGramFactor",
          "elasticsearch.search.wordGram.threeGramFactor",
          "elasticsearch.search.wordGram.twoGramFactor",
          "elasticsearch.sslContext.keyStoreFile",
          "elasticsearch.sslContext.keyStoreType",
          "elasticsearch.sslContext.protocol",
          "elasticsearch.sslContext.secureRandomImplementation",
          "elasticsearch.sslContext.trustStoreFile",
          "elasticsearch.sslContext.trustStoreType",
          "elasticsearch.threadCount",
          "elasticsearch.useSSL",
          "elasticsearch.username",
          "elasticsearch.search.validation.maxQueryLength",
          "elasticsearch.search.validation.regex",
          "elasticsearch.search.validation.maxLengthEnabled",
          "elasticsearch.search.validation.enabled",
          // Additional DataHub services and features
          "businessAttribute.fetchRelatedEntitiesBatchSize",
          "businessAttribute.fetchRelatedEntitiesCount",
          "businessAttribute.keepAliveTime",
          "businessAttribute.threadCount",
          "chromeExtension.enabled",
          "chromeExtension.lineageEnabled",
          "entityChangeEvents.consumerGroupSuffix",
          "entityChangeEvents.enabled",
          "entityChangeEvents.entityExclusions",
          "entityClient.java.get.batchSize",
          "entityClient.java.ingest.batchSize",
          "entityClient.numRetries",
          "entityClient.restli.get.batchConcurrency",
          "entityClient.restli.get.batchQueueSize",
          "entityClient.restli.get.batchSize",
          "entityClient.restli.get.batchThreadKeepAlive",
          "entityClient.restli.ingest.batchConcurrency",
          "entityClient.restli.ingest.batchQueueSize",
          "entityClient.restli.ingest.batchSize",
          "entityClient.restli.ingest.batchThreadKeepAlive",
          "entityClient.retryInterval",
          "entityService.impl",
          "entityService.retention.applyOnBootstrap",
          "entityService.retention.enabled",
          "eventsApi.enabled",
          "forms.hook.consumerGroupSuffix",
          "forms.hook.enabled",
          "graphQL.concurrency.corePoolSize",
          "graphQL.concurrency.keepAlive",
          "graphQL.concurrency.maxPoolSize",
          "graphQL.concurrency.separateThreadPool",
          "graphQL.concurrency.stackSize",
          "graphQL.metrics.enabled",
          "graphQL.metrics.fieldLevelEnabled",
          "graphQL.metrics.fieldLevelOperations",
          "graphQL.metrics.fieldLevelPathEnabled",
          "graphQL.metrics.fieldLevelPaths",
          "graphQL.metrics.percentiles",
          "graphQL.metrics.trivialDataFetchersEnabled",
          "graphQL.query.complexityLimit",
          "graphQL.query.depthLimit",
          "graphQL.query.introspectionEnabled",
          "graphService.limit.results.apiDefault",
          "graphService.limit.results.max",
          "graphService.limit.results.strict",
          "graphService.type",
          "healthCheck.cacheDurationSeconds",
          "homePage.firstInPersonalSidebar",
          "icebergCatalog.enablePublicRead",
          "icebergCatalog.publiclyReadableTag",
          "incidents.hook.consumerGroupSuffix",
          "incidents.hook.enabled",
          "incidents.hook.maxIncidentHistory",
          "ingestion.batchRefreshCount",
          "ingestion.defaultCliVersion",
          "ingestion.enabled",
          "ingestion.maxSerializedStringLength",
          "ingestionScheduler.consumerGroupSuffix",
          "ingestionScheduler.enabled",
          "ingestion.scheduler.refreshIntervalSeconds",
          "path-mappings./",
          // Management and monitoring
          "management.defaults.metrics.export.enabled",
          "management.endpoints.jmx.enabled",
          "management.endpoints.web.exposure.include",
          "management.health.defaults.enabled",
          "management.metrics.cache.enabled",
          "management.metrics.export.jmx.enabled",
          "management.metrics.export.prometheus.enabled",
          "management.metrics.tags.application",
          "management.otlp.tracing.export.enabled",
          "management.simple.metrics.export.enabled",
          "management.tracing.enabled",
          "management.tracing.propagation.type",
          // System metadata and updates
          "systemMetadataService.limit.results.apiDefault",
          "systemMetadataService.limit.results.max",
          "systemMetadataService.limit.results.strict",
          "systemUpdate.backOffFactor",
          "systemUpdate.bootstrap.mcpConfig",
          "systemUpdate.cdcMode",
          "systemUpdate.browsePathsV2.batchSize",
          "systemUpdate.browsePathsV2.enabled",
          "systemUpdate.browsePathsV2.reprocess.enabled",
          "systemUpdate.dashboardInfo.batchSize",
          "systemUpdate.dashboardInfo.delayMs",
          "systemUpdate.dashboardInfo.enabled",
          "systemUpdate.dashboardInfo.limit",
          "systemUpdate.dataJobNodeCLL.batchSize",
          "systemUpdate.dataJobNodeCLL.delayMs",
          "systemUpdate.dataJobNodeCLL.enabled",
          "systemUpdate.dataJobNodeCLL.limit",
          "systemUpdate.domainDescription.batchSize",
          "systemUpdate.domainDescription.delayMs",
          "systemUpdate.domainDescription.enabled",
          "systemUpdate.domainDescription.limit",
          "systemUpdate.edgeStatus.batchSize",
          "systemUpdate.edgeStatus.delayMs",
          "systemUpdate.edgeStatus.enabled",
          "systemUpdate.edgeStatus.limit",
          "systemUpdate.ingestionIndices.batchSize",
          "systemUpdate.ingestionIndices.delayMs",
          "systemUpdate.ingestionIndices.enabled",
          "systemUpdate.ingestionIndices.limit",
          "systemUpdate.initialBackOffMs",
          "systemUpdate.maxBackOffs",
          "systemUpdate.ownershipTypes.batchSize",
          "systemUpdate.ownershipTypes.enabled",
          "systemUpdate.ownershipTypes.reprocess.enabled",
          "systemUpdate.policyFields.batchSize",
          "systemUpdate.policyFields.enabled",
          "systemUpdate.policyFields.reprocess.enabled",
          "systemUpdate.processInstanceHasRunEvents.batchSize",
          "systemUpdate.processInstanceHasRunEvents.delayMs",
          "systemUpdate.processInstanceHasRunEvents.enabled",
          "systemUpdate.processInstanceHasRunEvents.reprocess.enabled",
          "systemUpdate.processInstanceHasRunEvents.totalDays",
          "systemUpdate.processInstanceHasRunEvents.windowDays",
          "systemUpdate.propertyDefinitions.batchSize",
          "systemUpdate.propertyDefinitions.delayMs",
          "systemUpdate.propertyDefinitions.enabled",
          "systemUpdate.propertyDefinitions.limit",
          "systemUpdate.removeQueryEdges.enabled",
          "systemUpdate.removeQueryEdges.numRetries",
          "systemUpdate.schemaFieldsDocIds.batchSize",
          "systemUpdate.schemaFieldsDocIds.delayMs",
          "systemUpdate.schemaFieldsDocIds.enabled",
          "systemUpdate.schemaFieldsDocIds.limit",
          "systemUpdate.schemaFieldsFromSchemaMetadata.batchSize",
          "systemUpdate.schemaFieldsFromSchemaMetadata.delayMs",
          "systemUpdate.schemaFieldsFromSchemaMetadata.enabled",
          "systemUpdate.schemaFieldsFromSchemaMetadata.limit",
          "systemUpdate.waitForSystemUpdate",
          "systemUpdate.entityConsistency.checkIds",
          "systemUpdate.entityConsistency.dryRun",
          "systemUpdate.entityConsistency.entityTypes",
          "systemUpdate.entityConsistency.gracePeriodSeconds",
          "systemUpdate.entityConsistency.systemMetadataFilterConfig.aspectFilters",
          "systemUpdate.entityConsistency.systemMetadataFilterConfig.gePitEpochMs",
          "systemUpdate.entityConsistency.systemMetadataFilterConfig.includeSoftDeleted",
          "systemUpdate.entityConsistency.systemMetadataFilterConfig.lePitEpochMs",
          // Additional configuration
          "metadataChangeProposal.consumer.batch.enabled",
          "metadataChangeProposal.consumer.batch.size",
          "metadataChangeProposal.sideEffects.dataProductUnset.enabled",
          "metadataChangeProposal.sideEffects.schemaField.enabled",
          "metadataChangeProposal.throttle.components.apiRequests.enabled",
          "metadataChangeProposal.throttle.components.mceConsumer.enabled",
          "metadataChangeProposal.throttle.timeseries.enabled",
          "metadataChangeProposal.throttle.timeseries.initialIntervalMs",
          "metadataChangeProposal.throttle.timeseries.maxAttempts",
          "metadataChangeProposal.throttle.timeseries.maxIntervalMs",
          "metadataChangeProposal.throttle.timeseries.multiplier",
          "metadataChangeProposal.throttle.timeseries.threshold",
          "metadataChangeProposal.throttle.updateIntervalMs",
          "metadataChangeProposal.throttle.versioned.enabled",
          "metadataChangeProposal.throttle.versioned.initialIntervalMs",
          "metadataChangeProposal.throttle.versioned.maxAttempts",
          "metadataChangeProposal.throttle.versioned.maxIntervalMs",
          "metadataChangeProposal.throttle.versioned.multiplier",
          "metadataChangeProposal.throttle.versioned.threshold",
          "metadataChangeProposal.validation.extensions.enabled",
          "metadataChangeProposal.validation.ignoreUnknown",
          "metadataChangeProposal.validation.privilegeConstraints.enabled",
          "metadataTests.enabled",
          "platformAnalytics.enabled",
          "platformAnalytics.usageExport.aspectTypes",
          "platformAnalytics.usageExport.enabled",
          "platformAnalytics.usageExport.usageEventTypes",
          "platformAnalytics.usageExport.userFilters",
          "searchBar.apiVariant",
          "searchCard.showDescription",
          "searchFlags.defaultSkipHighlighting",
          "searchService.cache.hazelcast.serviceName",
          "searchService.cache.hazelcast.service-dns-timeout",
          "searchService.cache.hazelcast.kubernetes-api-retries",
          "searchService.cache.hazelcast.resolve-not-ready-addresses",
          "searchService.cacheImplementation",
          "searchService.enableCache",
          "searchService.enableEviction",
          "searchService.limit.results.apiDefault",
          "searchService.limit.results.max",
          "searchService.limit.results.strict",
          "searchService.queryFilterRewriter.containerExpansion.enabled",
          "searchService.queryFilterRewriter.containerExpansion.limit",
          "searchService.queryFilterRewriter.containerExpansion.pageSize",
          "searchService.queryFilterRewriter.domainExpansion.enabled",
          "searchService.queryFilterRewriter.domainExpansion.limit",
          "searchService.queryFilterRewriter.domainExpansion.pageSize",
          "searchService.resultBatchSize",
          "siblings.consumerGroupSuffix",
          "siblings.enabled",
          "springdoc.api-docs.path",
          "springdoc.api-docs.version",
          "springdoc.cache.disabled",
          "springdoc.groups.enabled",
          "springdoc.swagger-ui.disable-swagger-default-url",
          "springdoc.swagger-ui.path",
          "springdoc.swagger-ui.urls-primary-name",
          "structuredProperties.enabled",
          "structuredProperties.systemUpdateEnabled",
          "structuredProperties.writeEnabled",
          "telemetry.enabledCli",
          "telemetry.enabledIngestion",
          "telemetry.enabledServer",
          "telemetry.enableThirdPartyLogging",
          "timeseriesAspectService.limit.results.apiDefault",
          "timeseriesAspectService.limit.results.max",
          "timeseriesAspectService.limit.results.strict",
          "timeseriesAspectService.query.concurrency",
          "timeseriesAspectService.query.queueSize",
          "timeseriesAspectService.query.threadKeepAlive",
          "updateIndices.consumerGroupSuffix",
          "updateIndices.enabled",
          "usageClient.numRetries",
          "usageClient.retryInterval",
          "usageClient.timeoutMs",
          "views.enabled",
          "visualConfig.application.showSidebarSectionWhenEmpty",
          "visualConfig.appTitle",
          "visualConfig.assets.faviconUrl",
          "visualConfig.assets.logoUrl",
          "visualConfig.entityProfile.domainDefaultTab",
          "visualConfig.hideGlossary",
          "visualConfig.queriesTab.queriesTabResultSize",
          "visualConfig.searchResult.enableNameHighlight",
          "visualConfig.showFullTitleInLineage",
          "visualConfig.theme.themeId",
          // Gradle and test-specific properties
          "org.gradle.internal.worker.tmpdir",
          "org.springframework.boot.test.context.SpringBootTestContextBootstrapper",
          "datahub.policies.systemPolicyUrnList",
          // Base Path
          "datahub.basePath",
          "server.servlet.context-path",
          "datahub.gms.basePath",
          "datahub.gms.basePathEnabled",
          // CDC (Change Data Capture) configuration properties
          "kafka.serde.cdc.key.serializer",
          "kafka.serde.cdc.key.deserializer",
          "kafka.serde.cdc.key.delegateDeserializer",
          "kafka.serde.cdc.value.serializer",
          "kafka.serde.cdc.value.deserializer",
          "kafka.serde.cdc.value.delegateDeserializer",
          "mclProcessing.cdcSource.enabled",
          "mclProcessing.cdcSource.configureSource",
          "mclProcessing.cdcSource.type",
          "mclProcessing.cdcSource.debeziumConfig.type",
          "mclProcessing.cdcSource.debeziumConfig.name",
          "mclProcessing.cdcSource.debeziumConfig.url",
          "mclProcessing.cdcSource.debeziumConfig.requestTimeoutMillis",
          "mclProcessing.cdcSource.debeziumConfig.config.tasks.max",
          "mclProcessing.cdcSource.debeziumConfig.config.topic.prefix",
          "mclProcessing.cdcSource.debeziumConfig.config.database.user",
          "mclProcessing.cdcSource.debeziumConfig.config.database.dbname",
          "mclProcessing.cdcSource.debeziumConfig.config.schema.history.internal.kafka.topic",
          "mclProcessing.cdcSource.debeziumConfig.config.value.converter.schemas.enable",
          "mclProcessing.cdcSource.debeziumConfig.config.key.converter",
          "mclProcessing.cdcSource.debeziumConfig.config.value.converter",
          "mclProcessing.cdcSource.debeziumConfig.config.message.key.columns",
          "mclProcessing.cdcSource.debeziumConfig.config.transforms",
          "mclProcessing.cdcSource.debeziumConfig.config.transforms.route.type",
          "mclProcessing.cdcSource.debeziumConfig.config.transforms.route.regex",
          "mclProcessing.cdcSource.debeziumConfig.config.transforms.route.replacement",
          "mclProcessing.cdcSource.debeziumConfig.postgresConfig.connector.class",
          "mclProcessing.cdcSource.debeziumConfig.postgresConfig.plugin.name",
          "mclProcessing.cdcSource.debeziumConfig.postgresConfig.table.include.list",
          "mclProcessing.cdcSource.debeziumConfig.postgresConfig.schema.include.list",
          "mclProcessing.cdcSource.debeziumConfig.postgresConfig.publication.autocreate.mode",
          "mclProcessing.cdcSource.debeziumConfig.postgresConfig.publication.name",
          "mclProcessing.cdcSource.debeziumConfig.postgresConfig.message.key.columns",
          "mclProcessing.cdcSource.debeziumConfig.postgresConfig.slot.name",
          "mclProcessing.cdcSource.debeziumConfig.postgresConfig.database.server.name",
          "mclProcessing.cdcSource.debeziumConfig.mysqlConfig.connector.class",
          "mclProcessing.cdcSource.debeziumConfig.mysqlConfig.plugin.name",
          "mclProcessing.cdcSource.debeziumConfig.mysqlConfig.table.include.list",
          "mclProcessing.cdcSource.debeziumConfig.mysqlConfig.database.server.id",
          "mclProcessing.cdcSource.debeziumConfig.mysqlConfig.database.include.list",
          "elasticsearch.entityIndex.v2.cleanup",
          "elasticsearch.entityIndex.v3.analyzerConfig",
          "elasticsearch.entityIndex.v3.mappingConfig",
          "elasticsearch.entityIndex.v3.cleanup",
          "elasticsearch.entityIndex.v3.maxFieldsLimit",
          // Semantic search configuration
          "elasticsearch.entityIndex.semanticSearch.enabled",
          "elasticsearch.entityIndex.semanticSearch.enabledEntities",
          "elasticsearch.entityIndex.semanticSearch.models.cohere_embed_v3.vectorDimension",
          "elasticsearch.entityIndex.semanticSearch.models.cohere_embed_v3.knnEngine",
          "elasticsearch.entityIndex.semanticSearch.models.cohere_embed_v3.spaceType",
          "elasticsearch.entityIndex.semanticSearch.models.cohere_embed_v3.efConstruction",
          "elasticsearch.entityIndex.semanticSearch.models.cohere_embed_v3.m",
          "elasticsearch.entityIndex.semanticSearch.embeddingsUpdate.batchSize",
          "elasticsearch.entityIndex.semanticSearch.embeddingsUpdate.maxTextLength",
          "elasticsearch.entityIndex.semanticSearch.embeddingProvider.type",
          "elasticsearch.entityIndex.semanticSearch.embeddingProvider.awsRegion",
          "elasticsearch.entityIndex.semanticSearch.embeddingProvider.modelId",
          "elasticsearch.entityIndex.semanticSearch.embeddingProvider.maxCharacterLength",
          // Metadata Change Log configuration
          "metadataChangeLog.consumer.batch.enabled",
          "metadataChangeLog.consumer.batch.size"

          // TODO: Add more properties as they are discovered during testing
          // When this test fails due to unclassified properties, add them to
          // either SENSITIVE_PROPERTIES or NON_SENSITIVE_PROPERTIES
          );

  /**
   * Convert a template pattern to a regex pattern. Templates use [*] for numeric indices and * for
   * single property segments.
   */
  private String templateToRegex(String template) {
    return template
        .replace(".", "\\.") // Escape dots: . → \.
        .replace("[*]", "\\[\\d+\\]") // Array indices: [*] → [\d+]
        .replace("*", "[^.]*"); // Segments: * → [^.]* (zero or more non-dot chars)
  }

  /** Check if a property key matches any of the sensitive templates */
  private boolean matchesSensitiveTemplate(String key) {
    return SENSITIVE_PROPERTY_TEMPLATES.stream()
        .anyMatch(template -> key.matches(templateToRegex(template)));
  }

  /** Check if a property key matches any of the non-sensitive templates */
  private boolean matchesNonSensitiveTemplate(String key) {
    return NON_SENSITIVE_PROPERTY_TEMPLATES.stream()
        .anyMatch(template -> key.matches(templateToRegex(template)));
  }

  /**
   * Main test that ensures every configuration property is explicitly classified.
   *
   * <p>FAIL CONDITIONS: - Property is redacted but not in SENSITIVE_PROPERTIES - Property is not
   * redacted but not in NON_SENSITIVE_PROPERTIES - Property is in both lists - Property is not in
   * either list
   */
  @Test
  public void testAllPropertiesAreExplicitlyClassified() {
    SystemPropertiesInfo systemInfo = propertiesCollector.collect();
    Map<String, PropertyInfo> properties = systemInfo.getProperties();

    // Separate properties by source type
    Map<String, PropertyInfo> configProperties = new HashMap<>();
    Map<String, PropertyInfo> environmentVariables = new HashMap<>();

    for (Map.Entry<String, PropertyInfo> entry : properties.entrySet()) {
      if ("OriginAwareSystemEnvironmentPropertySource".equals(entry.getValue().getSourceType())) {
        environmentVariables.put(entry.getKey(), entry.getValue());
      } else {
        configProperties.put(entry.getKey(), entry.getValue());
      }
    }

    log.info(
        "📊 Property Classification: {} configuration properties, {} environment variables",
        configProperties.size(),
        environmentVariables.size());

    // Test explicit classification for configuration properties only
    testConfigurationPropertiesClassification(configProperties);

    // Handle environment variables with a separate policy
    handleEnvironmentVariables(environmentVariables);
  }

  /**
   * Test explicit classification for configuration properties (application.yaml, etc.) These must
   * be explicitly classified as sensitive or non-sensitive.
   */
  private void testConfigurationPropertiesClassification(
      Map<String, PropertyInfo> configProperties) {
    Set<String> unclassifiedProperties = new HashSet<>();
    Set<String> misclassifiedProperties = new HashSet<>();
    int sensitiveCount = 0;
    int nonSensitiveCount = 0;

    for (Map.Entry<String, PropertyInfo> entry : configProperties.entrySet()) {
      String key = entry.getKey();
      PropertyInfo info = entry.getValue();
      boolean isRedacted = "***REDACTED***".equals(info.getValue());

      // Check if property is in both lists (should never happen)
      if (SENSITIVE_PROPERTIES.contains(key) && NON_SENSITIVE_PROPERTIES.contains(key)) {
        fail(
            "Property '"
                + key
                + "' is in both SENSITIVE_PROPERTIES and NON_SENSITIVE_PROPERTIES lists");
      }

      // Classify the property
      if (isRedacted) {
        sensitiveCount++;
        if (!SENSITIVE_PROPERTIES.contains(key) && !matchesSensitiveTemplate(key)) {
          misclassifiedProperties.add(
              key + " (is redacted but not in SENSITIVE_PROPERTIES or templates)");
        }
      } else {
        nonSensitiveCount++;
        if (!NON_SENSITIVE_PROPERTIES.contains(key) && !matchesNonSensitiveTemplate(key)) {
          unclassifiedProperties.add(
              key + " (not redacted, needs to be added to NON_SENSITIVE_PROPERTIES or templates)");
        }
      }

      // Also check that properties in SENSITIVE_PROPERTIES are actually redacted
      if ((SENSITIVE_PROPERTIES.contains(key) || matchesSensitiveTemplate(key)) && !isRedacted) {
        misclassifiedProperties.add(
            key
                + " (in SENSITIVE_PROPERTIES/templates but not redacted, value: "
                + info.getValue()
                + ")");
      }
    }

    // Report any issues
    if (!unclassifiedProperties.isEmpty()) {
      log.error("Unclassified configuration properties found: {}", unclassifiedProperties);
      fail(
          "Found unclassified configuration properties. Add them to NON_SENSITIVE_PROPERTIES: "
              + unclassifiedProperties);
    }

    if (!misclassifiedProperties.isEmpty()) {
      log.error("Misclassified configuration properties found: {}", misclassifiedProperties);
      fail("Found misclassified configuration properties: " + misclassifiedProperties);
    }

    // Success logging
    log.info("✅ All {} configuration properties are properly classified:", configProperties.size());
    log.info("  - {} sensitive properties (redacted)", sensitiveCount);
    log.info("  - {} non-sensitive properties", nonSensitiveCount);
    log.info("  - {} properties in SENSITIVE_PROPERTIES list", SENSITIVE_PROPERTIES.size());
    log.info("  - {} properties in NON_SENSITIVE_PROPERTIES list", NON_SENSITIVE_PROPERTIES.size());

    // Sanity checks
    assertTrue(
        sensitiveCount > 0, "Expected to find at least some sensitive configuration properties");
    assertTrue(
        nonSensitiveCount > 0,
        "Expected to find at least some non-sensitive configuration properties");
  }

  /**
   * Handle environment variables with a separate, more permissive policy. Environment variables are
   * inherently unpredictable across different environments.
   */
  private void handleEnvironmentVariables(Map<String, PropertyInfo> environmentVariables) {
    Set<String> sensitiveEnvVars = new HashSet<>();
    Set<String> allowedEnvVars = new HashSet<>();

    for (Map.Entry<String, PropertyInfo> entry : environmentVariables.entrySet()) {
      String key = entry.getKey();
      PropertyInfo info = entry.getValue();
      boolean isRedacted = "***REDACTED***".equals(info.getValue());

      if (isRedacted) {
        sensitiveEnvVars.add(key);
      } else {
        allowedEnvVars.add(key);
      }
    }

    log.info("🔒 Environment Variables Summary:");
    log.info("  - {} total environment variables", environmentVariables.size());
    log.info(
        "  - {} sensitive (redacted): {}",
        sensitiveEnvVars.size(),
        sensitiveEnvVars.size() > 10
            ? sensitiveEnvVars.stream().limit(10).toArray() + "..."
            : sensitiveEnvVars);
    log.info(
        "  - {} allowed (visible): {}",
        allowedEnvVars.size(),
        allowedEnvVars.size() > 10
            ? allowedEnvVars.stream().limit(10).toArray() + "..."
            : allowedEnvVars);

    // Environment variables are handled permissively - we just log them for awareness
    // The PropertiesCollector's existing isAllowedProperty() method handles the security
  }

  /**
   * Test to validate that our template-to-regex conversion works correctly. This ensures the
   * wildcard matching is functioning as expected.
   */
  @Test
  public void testTemplateToRegexConversion() {
    // Test array index wildcards
    String template1 = "authentication.authenticators[*].type";
    String regex1 = templateToRegex(template1);
    assertEquals(regex1, "authentication\\.authenticators\\[\\d+\\]\\.type");

    // Verify the regex matches actual property names
    assertTrue("authentication.authenticators[0].type".matches(regex1));
    assertTrue("authentication.authenticators[15].type".matches(regex1));
    assertFalse("authentication.authenticators[abc].type".matches(regex1));

    // Test segment wildcards
    String template2 = "cache.client.entityClient.entityAspectTTLSeconds.*.*";
    String regex2 = templateToRegex(template2);
    assertEquals(regex2, "cache\\.client\\.entityClient\\.entityAspectTTLSeconds\\.[^.]*\\.[^.]*");

    // Verify the regex matches actual cache properties
    assertTrue(
        "cache.client.entityClient.entityAspectTTLSeconds.corpuser.corpUserKey".matches(regex2));
    assertTrue(
        "cache.client.entityClient.entityAspectTTLSeconds.dataset.usageFeatures".matches(regex2));
    assertFalse(
        "cache.client.entityClient.entityAspectTTLSeconds.corpuser"
            .matches(regex2)); // Too few segments

    log.info("✅ Template-to-regex conversion working correctly");
  }
}

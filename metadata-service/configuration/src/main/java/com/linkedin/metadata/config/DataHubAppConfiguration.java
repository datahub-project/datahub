package com.linkedin.metadata.config;

import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.metadata.config.cache.CacheConfiguration;
import com.linkedin.metadata.config.events.EventSinksConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.notification.NotificationConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.telemetry.TelemetryConfiguration;
import lombok.Data;

@Data
public class DataHubAppConfiguration {

  /** Ingestion related configs */
  private IngestionConfiguration ingestion;

  /** Telemetry related configs */
  private TelemetryConfiguration telemetry;

  /** Viz related configs */
  private VisualConfiguration visualConfig;

  /** Tests related configs */
  private TestsConfiguration metadataTests;

  /** DataHub top-level server configurations */
  private DataHubConfiguration datahub;

  /** Views feature related configs */
  private ViewsConfiguration views;

  /** Search bar related configs */
  private SearchBarConfiguration searchBar;

  /** Feature flags indicating what is turned on vs turned off */
  private FeatureFlags featureFlags;

  /** Kafka related configs. */
  private KafkaConfiguration kafka;

  /** ElasticSearch configurations */
  private ElasticSearchConfiguration elasticSearch;

  /* Search Service configurations */
  private SearchServiceConfiguration searchService;

  /** System Update configurations */
  private SystemUpdateConfiguration systemUpdate;

  /** The base URL where DataHub is hosted. */
  private String baseUrl;

  // fork related configs go below this line
  /** Event mirroring related configs */
  private EventSinksConfiguration eventSinks;

  /** Notification related configs */
  private NotificationConfiguration notifications;

  /** Configuration for caching */
  private CacheConfiguration cache;

  /** Configuration for the chrome extension */
  private ChromeExtensionConfiguration chromeExtension;

  /** Ebean related configuration */
  private EbeanConfiguration ebean;

  /** GraphQL Configurations */
  private GraphQLConfiguration graphQL;

  /** Configuration for the integrations service. */
  private IntegrationsServiceConfiguration integrationsService;

  /** Configuration for the monitor service. */
  private MonitorServiceConfiguration monitorService;

  /** Configuration related to datahub executors */
  private ExecutorConfiguration executors;

  /** Configuration related to classifications and automations */
  private ClassificationConfiguration classificationConfig;

  /** Configuration for the execution of assertions */
  private AssertionMonitorsConfiguration assertionMonitors;

  /** MCP throttling configuration */
  private MetadataChangeProposalConfig metadataChangeProposal;

  /** Timeseries Aspect Service configuration */
  private TimeseriesAspectServiceConfig timeseriesAspectService;

  /** EntityService configuration */
  private EntityServiceConfiguration entityService;
}

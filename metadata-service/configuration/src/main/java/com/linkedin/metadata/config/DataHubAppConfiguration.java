package com.linkedin.metadata.config;

import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.metadata.config.cache.CacheConfiguration;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.telemetry.TelemetryConfiguration;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties
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

  /** Search card related configs */
  private SearchCardConfiguration searchCard;

  /** Search flags related configs */
  private SearchFlagsConfiguration searchFlags;

  /** Home page related configs */
  private HomePageConfiguration homePage;

  /** Feature flags indicating what is turned on vs turned off */
  private FeatureFlags featureFlags;

  /** Kafka related configs. */
  private KafkaConfiguration kafka;

  /** ElasticSearch configurations */
  private ElasticSearchConfiguration elasticSearch;

  /** Search Service configurations */
  private SearchServiceConfiguration searchService;

  /** Graph Service configurations */
  private GraphServiceConfiguration graphService;

  /** System Update configurations */
  private SystemUpdateConfiguration systemUpdate;

  /** The base URL where DataHub is hosted. */
  private String baseUrl;

  /** Configuration for caching */
  private CacheConfiguration cache;

  /** Configuration for the chrome extension */
  private ChromeExtensionConfiguration chromeExtension;

  /** Ebean related configuration */
  private EbeanConfiguration ebean;

  /** GraphQL Configurations */
  private GraphQLConfiguration graphQL;

  /** MCP throttling configuration */
  private MetadataChangeProposalConfig metadataChangeProposal;

  /** MCL Processing configurations */
  private MetadataChangeLogConfig metadataChangeLog;

  /** Timeseries Aspect Service configuration */
  private TimeseriesAspectServiceConfig timeseriesAspectService;

  /** SystemMetadata Service configuration */
  private SystemMetadataServiceConfig systemMetadataService;

  /* Secret service configuration */
  private SecretServiceConfiguration secretService;

  /** MCL Processing configurations */
  private MCLProcessingConfiguration mclProcessing;

  /** Structured properties related configurations */
  private StructuredPropertiesConfiguration structuredProperties;

  /** Consistency checks configuration */
  private ConsistencyChecksConfiguration consistencyChecks;
}

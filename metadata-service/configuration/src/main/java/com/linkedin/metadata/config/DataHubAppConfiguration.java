package com.linkedin.metadata.config;

import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.metadata.config.cache.CacheConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
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

  /** Configuration for caching */
  private CacheConfiguration cache;

  /** Ebean related configuration */
  private EbeanConfiguration ebean;

  /** GraphQL Configurations */
  private GraphQLConfiguration graphQL;

  /** MCP throttling configuration */
  private MetadataChangeProposalConfig metadataChangeProposal;

  /** Timeseries Aspect Service configuration */
  private TimeseriesAspectServiceConfig timeseriesAspectService;
}

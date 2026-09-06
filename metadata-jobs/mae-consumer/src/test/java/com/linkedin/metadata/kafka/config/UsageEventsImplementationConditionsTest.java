package com.linkedin.metadata.kafka.config;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import javax.annotation.Nullable;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Exactly one {@code dataHubUsageEventIndexer} bean must be registered in every configuration, so
 * these two conditions have to stay mutually exclusive.
 */
public class UsageEventsImplementationConditionsTest {

  @Mock private ConditionContext context;
  @Mock private AnnotatedTypeMetadata metadata;
  @Mock private Environment env;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    when(context.getEnvironment()).thenReturn(env);
  }

  private void configure(@Nullable String implementation, boolean elasticsearchEnabled) {
    when(env.getProperty("platformAnalytics.usage-events.implementation"))
        .thenReturn(implementation);
    when(env.getProperty("DATAHUB_USAGE_EVENTS_IMPLEMENTATION")).thenReturn(null);
    when(env.getProperty("elasticsearch.enabled", Boolean.class)).thenReturn(elasticsearchEnabled);
  }

  private boolean esMatches() {
    return new ElasticsearchUsageEventsImplementationCondition().matches(context, metadata);
  }

  private boolean pgMatches() {
    return new PostgresUsageEventsImplementationCondition().matches(context, metadata);
  }

  @Test
  public void unsetImplementationDefaultsToElasticsearchWhenClusterEnabled() {
    configure(null, true);
    assertTrue(esMatches());
    assertFalse(pgMatches());
  }

  /**
   * Without a cluster to write to, pgAnalytics becomes the default rather than a required opt-in.
   */
  @Test
  public void unsetImplementationDefaultsToPostgresWhenElasticsearchDisabled() {
    configure(null, false);
    assertFalse(esMatches());
    assertTrue(pgMatches());
  }

  @Test
  public void explicitPostgresWins() {
    configure("postgres", true);
    assertFalse(esMatches());
    assertTrue(pgMatches());
  }

  /** Postgres takes over rather than leaving the indexer bean undefined. */
  @Test
  public void elasticsearchImplementationFallsBackToPostgresWhenClusterDisabled() {
    configure("elasticsearch", false);
    assertFalse(esMatches());
    assertTrue(pgMatches());
  }
}

package com.linkedin.metadata.usage.report;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.usage.loader.UsageMetricRegistryLoader;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import com.linkedin.metadata.usage.UsageDimensions;
import com.linkedin.metadata.usage.flush.AdditiveUsageRow;
import com.linkedin.metadata.usage.flush.FlushTrigger;
import com.linkedin.metadata.usage.flush.RecordingUsageFlushSink;
import com.linkedin.metadata.usage.identity.AspectCorpUserFlagsProvider;
import com.linkedin.metadata.usage.identity.UsageActorClassResolver;
import com.linkedin.metadata.usage.registry.metrics.UsageMetricRegistry;
import com.linkedin.metadata.usage.registry.operations.UsageOperationsRegistry;
import com.linkedin.metadata.usage.store.InMemoryUsageAggregationStore;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.AuthChannel;
import io.datahubproject.metadata.context.usage.UsageActorClass;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ReportedUsageTest {

  @Test
  public void record_mcpQuery_incrementsMcpQueriesWithoutApiCalls() {
    RecordingUsageFlushSink recordingSink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store = newStore(recordingSink);
    OperationContext systemContext = TestOperationContexts.systemContextNoValidate();

    boolean recorded =
        ReportedUsage.record(
            store,
            systemContext,
            3L,
            Map.of(
                UsageDimensions.USAGE_OPERATION,
                "mcp_query",
                ReportedUsage.PROP_ACTOR_URN,
                Constants.METATDATA_TEST_ACTOR,
                ReportedUsage.PROP_USAGE_IDENTITY,
                Constants.METATDATA_TEST_ACTOR,
                ReportedUsage.PROP_USER_AGENT,
                "python-requests/2.31.0",
                UsageDimensions.AUTH_CHANNEL,
                "pat",
                UsageDimensions.REQUEST_API,
                "mcp",
                "operation",
                "tools/call",
                "tool_name",
                "search",
                "mcp_server",
                "datahub"));
    Assert.assertTrue(recorded);

    store.recordRequest(httpSession(Constants.METATDATA_TEST_ACTOR, "metadata_ingest"));
    store.flush(FlushTrigger.SCHEDULED);

    var batch = recordingSink.batches().get(0);
    long mcpQueries =
        batch.additiveRows().stream()
            .filter(row -> row.metricName().equals("mcp_queries"))
            .mapToLong(AdditiveUsageRow::valueSum)
            .sum();
    long apiCalls =
        batch.additiveRows().stream()
            .filter(row -> row.metricName().equals("api_calls"))
            .mapToLong(AdditiveUsageRow::valueSum)
            .sum();
    Assert.assertEquals(mcpQueries, 3L);
    Assert.assertEquals(apiCalls, 1L);

    AdditiveUsageRow mcpRow =
        batch.additiveRows().stream()
            .filter(row -> row.metricName().equals("mcp_queries"))
            .findFirst()
            .orElseThrow();
    Assert.assertEquals(mcpRow.dimensions().get(UsageDimensions.USAGE_OPERATION), "mcp_query");
    Assert.assertEquals(mcpRow.dimensions().get(UsageDimensions.REQUEST_API), "mcp");
    Assert.assertEquals(mcpRow.dimensions().get(UsageDimensions.AUTH_CHANNEL), "pat");
    Assert.assertNotNull(mcpRow.dimensions().get(UsageDimensions.AGENT_CLASS));
    Assert.assertNotNull(mcpRow.dimensions().get(UsageDimensions.ACTOR_CLASS));
  }

  @Test
  public void record_rejectsZeroQuantityAndMissingTaxonomyProps() {
    RecordingUsageFlushSink recordingSink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store = newStore(recordingSink);
    OperationContext systemContext = TestOperationContexts.systemContextNoValidate();
    Assert.assertFalse(
        ReportedUsage.record(
            store,
            systemContext,
            0L,
            Map.of(
                UsageDimensions.USAGE_OPERATION, "mcp_query", UsageDimensions.REQUEST_API, "mcp")));
    Assert.assertFalse(ReportedUsage.record(store, systemContext, 1L, Map.of()));
    Assert.assertFalse(
        ReportedUsage.record(
            store, systemContext, 1L, Map.of(UsageDimensions.USAGE_OPERATION, "mcp_query")));
  }

  @Test
  public void record_rejectsUnknownUsageOperation() {
    RecordingUsageFlushSink recordingSink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store = newStore(recordingSink);
    OperationContext systemContext = TestOperationContexts.systemContextNoValidate();
    Assert.assertFalse(
        ReportedUsage.record(
            store,
            systemContext,
            1L,
            Map.of(
                UsageDimensions.USAGE_OPERATION,
                "not_a_real_operation",
                UsageDimensions.REQUEST_API,
                "mcp")));
  }

  @Test
  public void record_rejectsRequestApiNotAllowedForOperation() {
    RecordingUsageFlushSink recordingSink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store = newStore(recordingSink);
    OperationContext systemContext = TestOperationContexts.systemContextNoValidate();
    // mcp_query only allows request_apis: [mcp]
    Assert.assertFalse(
        ReportedUsage.record(
            store,
            systemContext,
            1L,
            Map.of(
                UsageDimensions.USAGE_OPERATION,
                "mcp_query",
                UsageDimensions.REQUEST_API,
                "openapi")));
  }

  @Test
  public void record_acceptsUnprovisionedActorWithoutAccessChecks() {
    RecordingUsageFlushSink recordingSink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store = newStore(recordingSink);
    OperationContext systemContext = TestOperationContexts.systemContextNoValidate();

    boolean recorded =
        ReportedUsage.record(
            store,
            systemContext,
            2L,
            Map.of(
                UsageDimensions.USAGE_OPERATION,
                "mcp_query",
                ReportedUsage.PROP_ACTOR_URN,
                "urn:li:corpuser:never-provisioned-metering-actor",
                UsageDimensions.REQUEST_API,
                "mcp"));
    Assert.assertTrue(recorded);

    store.flush(FlushTrigger.SCHEDULED);
    long mcpQueries =
        recordingSink.batches().get(0).additiveRows().stream()
            .filter(row -> row.metricName().equals("mcp_queries"))
            .mapToLong(AdditiveUsageRow::valueSum)
            .sum();
    Assert.assertEquals(mcpQueries, 2L);
  }

  @Test
  public void record_missingActorFallsBackToUnknownNotSystem() {
    RecordingUsageFlushSink recordingSink = new RecordingUsageFlushSink();
    InMemoryUsageAggregationStore store = newStore(recordingSink);
    OperationContext systemContext = TestOperationContexts.systemContextNoValidate();

    Assert.assertTrue(
        ReportedUsage.record(
            store,
            systemContext,
            1L,
            Map.of(
                UsageDimensions.USAGE_OPERATION, "mcp_query", UsageDimensions.REQUEST_API, "mcp")));

    store.flush(FlushTrigger.SCHEDULED);
    AdditiveUsageRow mcpRow =
        recordingSink.batches().get(0).additiveRows().stream()
            .filter(row -> row.metricName().equals("mcp_queries"))
            .findFirst()
            .orElseThrow();
    Assert.assertEquals(
        mcpRow.dimensions().get(UsageDimensions.ACTOR_CLASS),
        UsageActorClass.REGULAR.dimensionValue());
  }

  private static InMemoryUsageAggregationStore newStore(RecordingUsageFlushSink recordingSink) {
    YAMLMapper yamlMapper = new YAMLMapper();
    yamlMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    UsageOperationsRegistry usageRegistry =
        UsageOperationsRegistry.loadOssOnly(new UsageOperationsLoader(yamlMapper));
    UsageMetricRegistry metricRegistry =
        UsageMetricRegistry.loadBundled(
            new UsageMetricRegistryLoader(yamlMapper), java.util.List.of());
    return new InMemoryUsageAggregationStore(
        usageRegistry,
        metricRegistry,
        new UsageActorClassResolver(new AspectCorpUserFlagsProvider()),
        recordingSink,
        10_000,
        300);
  }

  private static OperationContext httpSession(String actorUrn, String usageOperation) {
    RequestContext requestContext =
        RequestContext.builder()
            .actorUrn(actorUrn)
            .sourceIP("127.0.0.1")
            .requestAPI(RequestContext.RequestAPI.OPENAPI)
            .requestID("testAction")
            .userAgent("test-agent")
            .usageOperation(usageOperation)
            .usageIdentity(actorUrn)
            .authChannel(AuthChannel.PAT)
            .build();
    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, actorUrn), "Basic test");
    try {
      return TestOperationContexts.systemContextNoValidate().toBuilder()
          .requestContext(requestContext)
          .build(authentication, true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

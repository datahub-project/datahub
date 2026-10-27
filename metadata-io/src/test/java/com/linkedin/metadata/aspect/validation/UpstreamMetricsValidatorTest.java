package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.METRIC_UPSTREAMS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Edge;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.UpstreamMetrics;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metric.MetricUpstreams;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpstreamMetricsValidatorTest {

  private static final Urn CHART_URN = UrnUtils.getUrn("urn:li:chart:(looker,revenue)");
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sch.orders,PROD)");
  private static final Urn METRIC_URN =
      UrnUtils.getUrn("urn:li:metric:(urn:li:dataPlatform:snowflake,db.sch.sales,revenue)");

  private final EntityRegistry registry =
      TestOperationContexts.systemContextNoSearchAuthorization().getEntityRegistry();

  private UpstreamMetricsValidator validator;
  private CachingAspectRetriever mockAspectRetriever;
  private RetrieverContext retrieverContext;
  private final Map<Urn, Map<String, Aspect>> currentAspects = new HashMap<>();

  @BeforeMethod
  public void setup() {
    currentAspects.clear();
    validator =
        new UpstreamMetricsValidator()
            .setConfig(
                AspectPluginConfig.builder()
                    .className("test")
                    .enabled(true)
                    .supportedOperations(List.of("UPSERT", "PATCH"))
                    .supportedEntityAspectNames(List.of())
                    .build());
    mockAspectRetriever = Mockito.mock(CachingAspectRetriever.class);
    Mockito.when(mockAspectRetriever.getEntityRegistry()).thenReturn(registry);
    Mockito.doAnswer(
            invocation -> {
              Set<Urn> requestedUrns = invocation.getArgument(1);
              Set<String> requestedAspects = invocation.getArgument(2);
              Map<Urn, Map<String, Aspect>> result = new HashMap<>();
              requestedUrns.forEach(
                  urn -> {
                    Map<String, Aspect> byName = new HashMap<>();
                    currentAspects
                        .getOrDefault(urn, Map.of())
                        .forEach(
                            (aspectName, aspect) -> {
                              if (requestedAspects.contains(aspectName)) {
                                byName.put(aspectName, aspect);
                              }
                            });
                    if (!byName.isEmpty()) {
                      result.put(urn, byName);
                    }
                  });
              return result;
            })
        .when(mockAspectRetriever)
        .getLatestAspectObjects(any(OperationFingerprint.class), anySet(), anySet());

    retrieverContext =
        io.datahubproject.metadata.context.RetrieverContext.builder()
            .searchRetriever(Mockito.mock(SearchRetriever.class))
            .graphRetriever(Mockito.mock(GraphRetriever.class))
            .cachingAspectRetriever(mockAspectRetriever)
            .build();
  }

  @Test
  public void testAcceptsMetricDestinationOnChart() {
    Assert.assertTrue(validateUpsert(CHART_URN, upstreamMetrics(METRIC_URN)).isEmpty());
  }

  @Test
  public void testAcceptsMetricDestinationOnDatasetWithoutCycle() {
    Assert.assertTrue(validateUpsert(DATASET_URN, upstreamMetrics(METRIC_URN)).isEmpty());
  }

  @Test
  public void testRejectsNonMetricDestination() {
    Assert.assertFalse(validateUpsert(CHART_URN, upstreamMetrics(DATASET_URN)).isEmpty());
  }

  @Test
  public void testRejectsDatasetMetricTwoCycle() {
    MetricUpstreams metricUpstreams =
        new MetricUpstreams().setDatasetUpstreams(new EdgeArray(edge(DATASET_URN)));
    currentAspects
        .computeIfAbsent(METRIC_URN, u -> new HashMap<>())
        .put(METRIC_UPSTREAMS_ASPECT_NAME, new Aspect(metricUpstreams.data()));

    Assert.assertFalse(validateUpsert(DATASET_URN, upstreamMetrics(METRIC_URN)).isEmpty());
  }

  private List<AspectValidationException> validateUpsert(Urn consumerUrn, UpstreamMetrics aspect) {
    BatchItem item =
        TestMCP.ofOneUpsertItem(consumerUrn, aspect, registry).stream().findFirst().get();
    return validator
        .validateProposedAspects(OperationFingerprint.EMPTY, List.of(item), retrieverContext)
        .toList();
  }

  private static UpstreamMetrics upstreamMetrics(Urn destinationUrn) {
    return new UpstreamMetrics().setMetrics(new EdgeArray(edge(destinationUrn)));
  }

  private static Edge edge(Urn destinationUrn) {
    return new Edge().setDestinationUrn(destinationUrn);
  }
}

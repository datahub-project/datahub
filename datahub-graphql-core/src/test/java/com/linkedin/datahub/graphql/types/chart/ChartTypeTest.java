package com.linkedin.datahub.graphql.types.chart;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.AspectLoadContext;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.execution.DataFetcherResult;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ChartTypeTest {

  private static final String TEST_CHART_URN = "urn:li:chart:(looker,test.chart)";

  @Test
  public void testBatchLoadWithOptimizedAspects() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    QueryContext mockContext = getMockAllowContext();

    Urn chartUrn = Urn.createFromString(TEST_CHART_URN);
    Set<String> optimizedAspects = ImmutableSet.of("chartKey", "chartInfo");

    Mockito.when(mockContext.getAspectLoadContext("Chart"))
        .thenReturn(AspectLoadContext.of(optimizedAspects));

    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.CHART_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(chartUrn))),
                Mockito.eq(ImmutableSet.of("chartKey", "chartInfo"))))
        .thenReturn(
            ImmutableMap.of(
                chartUrn,
                new EntityResponse()
                    .setEntityName(Constants.CHART_ENTITY_NAME)
                    .setUrn(chartUrn)
                    .setAspects(new EnvelopedAspectMap())));

    ChartType chartType = new ChartType(mockClient);
    List<DataFetcherResult<Chart>> results =
        chartType.batchLoad(Collections.singletonList(TEST_CHART_URN), mockContext);

    assertEquals(results.size(), 1);
    assertNotNull(results.get(0));
    assertNotNull(results.get(0).getData());
    assertEquals(results.get(0).getData().getUrn(), TEST_CHART_URN);

    ArgumentCaptor<Set<String>> aspectsCaptor = ArgumentCaptor.forClass(Set.class);
    Mockito.verify(mockClient)
        .batchGetV2(
            any(),
            Mockito.eq(Constants.CHART_ENTITY_NAME),
            any(HashSet.class),
            aspectsCaptor.capture());
    assertEquals(aspectsCaptor.getValue(), ImmutableSet.of("chartKey", "chartInfo"));
  }

  @Test
  public void testBatchLoadFallsBackWhenRegistryReturnsNull() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    QueryContext mockContext = getMockAllowContext();

    Urn chartUrn = Urn.createFromString(TEST_CHART_URN);

    Mockito.when(mockContext.getAspectLoadContext("Chart"))
        .thenReturn(AspectLoadContext.fetchAll());

    Mockito.when(mockClient.batchGetV2(any(), any(), any(), any()))
        .thenReturn(
            ImmutableMap.of(
                chartUrn,
                new EntityResponse()
                    .setEntityName(Constants.CHART_ENTITY_NAME)
                    .setUrn(chartUrn)
                    .setAspects(new EnvelopedAspectMap())));

    ChartType chartType = new ChartType(mockClient);
    chartType.batchLoad(Collections.singletonList(TEST_CHART_URN), mockContext);

    ArgumentCaptor<Set<String>> aspectsCaptor = ArgumentCaptor.forClass(Set.class);
    Mockito.verify(mockClient)
        .batchGetV2(
            any(),
            Mockito.eq(Constants.CHART_ENTITY_NAME),
            any(HashSet.class),
            aspectsCaptor.capture());
    assertEquals(aspectsCaptor.getValue(), ChartType.ASPECTS_TO_RESOLVE);
  }
}

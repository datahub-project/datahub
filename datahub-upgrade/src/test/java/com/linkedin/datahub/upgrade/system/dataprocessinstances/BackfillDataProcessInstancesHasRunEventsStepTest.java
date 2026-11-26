package com.linkedin.datahub.upgrade.system.dataprocessinstances;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.aggregations.Aggregations;
import org.testng.annotations.Test;

public class BackfillDataProcessInstancesHasRunEventsStepTest {

  @Test
  public void queriesTheTimeseriesClientNotPrimary() throws Exception {
    SearchClientShim<?> timeseries = mock(SearchClientShim.class);
    SearchClientShim<?> other = mock(SearchClientShim.class);
    SearchResponse response = mock(SearchResponse.class);
    Aggregations aggregations = mock(Aggregations.class);
    when(aggregations.asList()).thenReturn(List.of());
    when(response.getAggregations()).thenReturn(aggregations);
    when(timeseries.search(any(), any(), any())).thenReturn(response);

    SearchClusterAccess access =
        component -> component == SearchComponent.TIMESERIES ? timeseries : other;
    OperationContext opContext =
        TestOperationContexts.withSearchClusterAccess(
            TestOperationContexts.systemContextNoSearchAuthorization(), access);

    EntityService<?> entityService = mock(EntityService.class);
    ElasticSearchService elasticSearchService = mock(ElasticSearchService.class);

    BackfillDataProcessInstancesHasRunEventsStep step =
        new BackfillDataProcessInstancesHasRunEventsStep(
            opContext, entityService, elasticSearchService, true, 10, 0, 1, 1);

    UpgradeContext context = mock(UpgradeContext.class);
    when(context.opContext()).thenReturn(opContext);

    UpgradeStepResult result = step.executable().apply(context);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(timeseries).search(any(), any(), any());
    verify(other, never()).search(any(), any(), any());
  }
}

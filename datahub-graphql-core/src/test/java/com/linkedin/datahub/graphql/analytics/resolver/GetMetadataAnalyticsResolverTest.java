package com.linkedin.datahub.graphql.analytics.resolver;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.MetadataAnalyticsInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class GetMetadataAnalyticsResolverTest {

  private static boolean hasFieldWithCondition(
      ConjunctiveCriterion clause, String field, Condition condition) {
    return clause.getAnd().stream()
        .anyMatch(c -> field.equals(c.getField()) && condition.equals(c.getCondition()));
  }

  @Test
  public void testGetCharts_recordsWhetherTheCallerSetAFilter() throws Exception {
    // A vector store that cannot filter reads this to tell the caller's domain filter from the
    // published-document default this resolver adds
    EntityClient entityClient = mock(EntityClient.class);
    when(entityClient.searchAcrossEntities(
            any(OperationContext.class), anyList(), anyString(), any(), anyInt(), any(), anyList()))
        .thenReturn(
            new SearchResult()
                .setMetadata(
                    new SearchResultMetadata().setAggregations(new AggregationMetadataArray())));
    GetMetadataAnalyticsResolver resolver = new GetMetadataAnalyticsResolver(entityClient);

    for (String domain : Arrays.asList(null, "urn:li:domain:sales")) {
      DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
      QueryContext context = getMockAllowContext();
      when(env.getContext()).thenReturn(context);
      when(env.getArgument("input"))
          .thenReturn(MetadataAnalyticsInput.builder().setQuery(null).setDomain(domain).build());
      resolver.get(env);
    }

    ArgumentCaptor<OperationContext> contexts = ArgumentCaptor.forClass(OperationContext.class);
    verify(entityClient, times(2))
        .searchAcrossEntities(
            contexts.capture(), anyList(), anyString(), any(), anyInt(), any(), anyList());
    assertEquals(
        contexts.getAllValues().stream()
            .map(opContext -> opContext.getSearchContext().getCallerFiltered())
            .collect(Collectors.toList()),
        Arrays.asList(false, true));
  }

  @Test
  public void testGetCharts_appliesDefaultDocumentFilters() throws Exception {
    // Bridge documents (a Document subtype) are only excluded from cross-entity aggregations via
    // the default entity filters (lifecycleStage/showInGlobalContext); this resolver queries
    // searchAcrossEntities directly, so it must apply them itself rather than relying on
    // SearchAcrossEntitiesResolver/AggregateAcrossEntitiesResolver to have done so upstream.
    EntityClient entityClient = mock(EntityClient.class);
    when(entityClient.searchAcrossEntities(
            any(OperationContext.class), anyList(), anyString(), any(), anyInt(), any(), anyList()))
        .thenReturn(
            new SearchResult()
                .setMetadata(
                    new SearchResultMetadata().setAggregations(new AggregationMetadataArray())));

    DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    QueryContext context = getMockAllowContext();
    when(env.getContext()).thenReturn(context);
    when(env.getArgument("input"))
        .thenReturn(MetadataAnalyticsInput.builder().setQuery(null).setDomain(null).build());

    GetMetadataAnalyticsResolver resolver = new GetMetadataAnalyticsResolver(entityClient);
    resolver.get(env);

    ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    verify(entityClient)
        .searchAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            filterCaptor.capture(),
            anyInt(),
            any(),
            anyList());

    Filter filter = filterCaptor.getValue();
    assertNotNull(filter, "Expected default document filters to be applied");
    assertNotNull(filter.getOr());
    assertEquals(filter.getOr().size(), 2);
    assertTrue(hasFieldWithCondition(filter.getOr().get(0), "lifecycleStage", Condition.EQUAL));
    assertTrue(hasFieldWithCondition(filter.getOr().get(1), "lifecycleStage", Condition.IS_NULL));
  }
}

package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.LineageDirection;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageCounts;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageCountsInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.LineageSearchEntityArray;
import com.linkedin.metadata.search.LineageSearchResult;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SearchAcrossLineageCountsResolverTest {

  private static final String SOURCE_URN =
      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.orders,PROD),order_id)";

  private EntityClient _entityClient;
  private DataFetchingEnvironment _environment;
  private SearchAcrossLineageCountsResolver _resolver;

  @BeforeMethod
  public void setUp() {
    _entityClient = mock(EntityClient.class);
    _environment = mock(DataFetchingEnvironment.class);
    _resolver = new SearchAcrossLineageCountsResolver(_entityClient);
  }

  private SearchAcrossLineageCountsInput input(Boolean includeGhostEntities) {
    final SearchAcrossLineageCountsInput input = new SearchAcrossLineageCountsInput();
    input.setUrn(SOURCE_URN);
    input.setDirection(LineageDirection.DOWNSTREAM);
    input.setTypes(Collections.singletonList(EntityType.SCHEMA_FIELD));
    input.setIncludeGhostEntities(includeGhostEntities);
    return input;
  }

  private OperationContext captureContext(Boolean includeGhostEntities) throws Exception {
    final QueryContext mockContext = getMockAllowContext();
    when(_environment.getContext()).thenReturn(mockContext);
    when(_environment.getArgument(eq("input"))).thenReturn(input(includeGhostEntities));
    when(_entityClient.searchAcrossLineage(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), anyInt()))
        .thenReturn(
            new LineageSearchResult()
                .setEntities(new LineageSearchEntityArray())
                .setNumEntities(7));

    final SearchAcrossLineageCounts counts = _resolver.get(_environment).join();
    assertEquals(counts.getTotal(), 7);

    ArgumentCaptor<OperationContext> captor = ArgumentCaptor.forClass(OperationContext.class);
    verify(_entityClient)
        .searchAcrossLineage(
            captor.capture(), any(), any(), any(), any(), any(), any(), any(), anyInt(), anyInt());
    return captor.getValue();
  }

  @Test
  public void testCountsReadOffTheGraphWhenGhostEntitiesWanted() throws Exception {
    // Ghost entities have nothing in the entity index to count, so the request has to opt the
    // search
    // service into reading the count off the graph
    assertTrue(
        captureContext(true).getSearchContext().getLineageFlags().isUseLightningMode(),
        "asking for ghost entities should switch the search service to the graph");
  }

  @Test
  public void testCountsComeFromTheIndexByDefault() throws Exception {
    assertFalse(
        Boolean.TRUE.equals(
            captureContext(null).getSearchContext().getLineageFlags().isUseLightningMode()),
        "the graph-only path carries caveats, so it should not be the default");
  }

  @Test
  public void testNothingIsFetchedToCount() throws Exception {
    final QueryContext mockContext = getMockAllowContext();
    when(_environment.getContext()).thenReturn(mockContext);
    when(_environment.getArgument(eq("input"))).thenReturn(input(true));
    when(_entityClient.searchAcrossLineage(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), anyInt()))
        .thenReturn(
            new LineageSearchResult()
                .setEntities(new LineageSearchEntityArray())
                .setNumEntities(3));

    _resolver.get(_environment).join();

    // Entities are never returned, which is what keeps existence checks from stripping the very
    // entities being counted back out of the result
    verify(_entityClient)
        .searchAcrossLineage(any(), any(), any(), any(), any(), any(), any(), any(), eq(0), eq(0));
  }

  @Test
  public void testInvalidUrnRejected() {
    final QueryContext mockContext = getMockAllowContext();
    when(_environment.getContext()).thenReturn(mockContext);
    final SearchAcrossLineageCountsInput bad = input(true);
    bad.setUrn("not-a-valid-urn");
    when(_environment.getArgument(eq("input"))).thenReturn(bad);

    assertThrows(IllegalArgumentException.class, () -> _resolver.get(_environment).join());
  }
}

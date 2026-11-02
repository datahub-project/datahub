package com.linkedin.datahub.graphql.resolvers.ingest.source;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ListIngestionSourcesInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.FilterValue;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.MergedField;
import graphql.language.Field;
import graphql.language.SelectionSet;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ListIngestionSourceResolverTest {

  private static final ListIngestionSourcesInput TEST_INPUT =
      new ListIngestionSourcesInput(0, 20, null, null, null);

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.searchAcrossEntities(
                any(),
                Mockito.eq(List.of(Constants.INGESTION_SOURCE_ENTITY_NAME)),
                Mockito.eq(""),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(20),
                Mockito.any(),
                Mockito.eq(List.of("type"))))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(new SearchEntity().setEntity(TEST_INGESTION_SOURCE_URN))))
                .setMetadata(
                    new SearchResultMetadata()
                        .setAggregations(
                            new AggregationMetadataArray(
                                new AggregationMetadata()
                                    .setName("type")
                                    .setFilterValues(
                                        new FilterValueArray(
                                            new FilterValue()
                                                .setValue("snowflake")
                                                .setFacetCount(1)))))));

    ListIngestionSourcesResolver resolver = new ListIngestionSourcesResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getMergedField())
        .thenReturn(
            mergedListIngestionSourcesField(
                "start", "count", "total", "ingestionSources", "facets"));
    var result = resolver.get(mockEnv).get();

    // Data Assertions
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 1);
    assertEquals(result.getTotal(), 1);
    assertEquals(result.getIngestionSources().size(), 1);

    assertEquals(
        result.getIngestionSources().get(0).getUrn(), TEST_INGESTION_SOURCE_URN.toString());

    // Facet assertions
    assertEquals(result.getFacets().size(), 1);
    assertEquals(result.getFacets().get(0).getField(), "type");
    assertEquals(result.getFacets().get(0).getAggregations().size(), 1);
    assertEquals(result.getFacets().get(0).getAggregations().get(0).getValue(), "snowflake");
    assertEquals(result.getFacets().get(0).getAggregations().get(0).getCount(), Long.valueOf(1));
  }

  @Test
  public void testGetSuccessSkipsFacetsWhenNotSelected() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.searchAcrossEntities(
                any(),
                Mockito.eq(List.of(Constants.INGESTION_SOURCE_ENTITY_NAME)),
                Mockito.eq(""),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(20),
                Mockito.any(),
                Mockito.eq(List.of())))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(new SearchEntity().setEntity(TEST_INGESTION_SOURCE_URN))))
                .setMetadata(
                    new SearchResultMetadata().setAggregations(new AggregationMetadataArray())));

    ListIngestionSourcesResolver resolver = new ListIngestionSourcesResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getMergedField())
        .thenReturn(mergedListIngestionSourcesField("start", "count", "total"));
    var result = resolver.get(mockEnv).get();

    assertEquals(result.getIngestionSources().size(), 1);
    assertEquals(result.getFacets().size(), 0);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    ListIngestionSourcesResolver resolver = new ListIngestionSourcesResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .batchGetV2(any(), Mockito.any(), Mockito.anySet(), Mockito.anySet());
    Mockito.verify(mockClient, Mockito.times(0))
        .searchAcrossEntities(
            any(),
            Mockito.any(),
            Mockito.eq(""),
            Mockito.any(),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.any(),
            Mockito.any());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.any(), Mockito.anySet(), Mockito.anySet());
    ListIngestionSourcesResolver resolver = new ListIngestionSourcesResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getMergedField())
        .thenReturn(mergedListIngestionSourcesField("start", "count", "total"));

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }

  /**
   * Builds a real query AST for {@code listIngestionSources { <selectedFields> }} so the resolver
   * reads the selection the same way it does at runtime.
   */
  private static MergedField mergedListIngestionSourcesField(final String... selectedFields) {
    final SelectionSet.Builder selectionSet = SelectionSet.newSelectionSet();
    for (String name : selectedFields) {
      selectionSet.selection(Field.newField(name).build());
    }
    return MergedField.newMergedField(
            Field.newField("listIngestionSources").selectionSet(selectionSet.build()).build())
        .build();
  }

  @Test
  void testDefaultBuildSortCriteria() {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    ListIngestionSourcesResolver resolver = new ListIngestionSourcesResolver(mockClient);

    List<SortCriterion> result = resolver.buildSortCriteria(null);

    assertEquals(0, result.size());
  }

  @Test
  void testBuildSortCriteriaForNameField() {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    ListIngestionSourcesResolver resolver = new ListIngestionSourcesResolver(mockClient);

    com.linkedin.datahub.graphql.generated.SortCriterion input =
        new com.linkedin.datahub.graphql.generated.SortCriterion();
    input.setField("name");
    input.setSortOrder(com.linkedin.datahub.graphql.generated.SortOrder.DESCENDING);

    List<SortCriterion> result = resolver.buildSortCriteria(input);

    assertEquals(1, result.size());
    assertEquals("name", result.get(0).getField());
    assertEquals(SortOrder.DESCENDING, result.get(0).getOrder());
  }
}

package com.linkedin.datahub.graphql.resolvers.ingest.source;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
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
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Set;
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

    ListIngestionSourcesResolver resolver =
        new ListIngestionSourcesResolver(mockClient, new FeatureFlags());

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

    ListIngestionSourcesResolver resolver =
        new ListIngestionSourcesResolver(mockClient, new FeatureFlags());

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
    ListIngestionSourcesResolver resolver =
        new ListIngestionSourcesResolver(mockClient, new FeatureFlags());

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
    ListIngestionSourcesResolver resolver =
        new ListIngestionSourcesResolver(mockClient, new FeatureFlags());

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
  private static final Urn OTHER_INGESTION_SOURCE_URN =
      UrnUtils.getUrn("urn:li:dataHubIngestionSource:other");

  /**
   * Actor without MANAGE_INGESTION (type-level checks are denied) who may read only the given
   * ingestion source URNs.
   */
  private static QueryContext getMockViewerContext(Set<Urn> readableUrns) {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    OperationContext opContext = Mockito.mock(OperationContext.class);
    Mockito.when(mockContext.getOperationContext()).thenReturn(opContext);
    Mockito.when(opContext.withSearchFlags(any())).thenReturn(opContext);
    Mockito.when(opContext.authorize(any(), Mockito.nullable(EntitySpec.class), any()))
        .thenAnswer(
            invocation -> {
              EntitySpec spec = invocation.getArgument(1);
              boolean allow =
                  spec != null
                      && !spec.getEntity().isEmpty()
                      && readableUrns.contains(UrnUtils.getUrn(spec.getEntity()));
              return new AuthorizationResult(
                  null, allow ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY, "");
            });
    return mockContext;
  }

  private static FeatureFlags viewPrivilegesEnabled() {
    FeatureFlags featureFlags = new FeatureFlags();
    featureFlags.setViewIngestionSourcePrivilegesEnabled(true);
    return featureFlags;
  }

  private static SearchResult searchResultOf(Urn... urns) {
    SearchEntityArray entities = new SearchEntityArray();
    for (Urn urn : urns) {
      entities.add(new SearchEntity().setEntity(urn));
    }
    return new SearchResult()
        .setFrom(0)
        .setPageSize(urns.length)
        .setNumEntities(urns.length)
        .setEntities(entities)
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()));
  }

  @Test
  public void testViewerSeesOnlyReadableSourcesWhenFlagEnabled() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.searchAcrossEntities(
                any(),
                Mockito.eq(List.of(Constants.INGESTION_SOURCE_ENTITY_NAME)),
                Mockito.eq(""),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(ListIngestionSourcesResolver.AUTHORIZED_SOURCES_SCAN_LIMIT),
                Mockito.any(),
                Mockito.eq(List.of())))
        .thenReturn(searchResultOf(OTHER_INGESTION_SOURCE_URN, TEST_INGESTION_SOURCE_URN));

    ListIngestionSourcesResolver resolver =
        new ListIngestionSourcesResolver(mockClient, viewPrivilegesEnabled());

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    QueryContext viewerContext = getMockViewerContext(Set.of(TEST_INGESTION_SOURCE_URN));
    Mockito.when(mockEnv.getContext()).thenReturn(viewerContext);
    Mockito.when(mockEnv.getMergedField())
        .thenReturn(
            mergedListIngestionSourcesField(
                "start", "count", "total", "ingestionSources", "facets"));
    var result = resolver.get(mockEnv).get();

    assertEquals(result.getTotal(), 1);
    assertEquals(result.getIngestionSources().size(), 1);
    assertEquals(
        result.getIngestionSources().get(0).getUrn(), TEST_INGESTION_SOURCE_URN.toString());
    // Facets are aggregated over every matching source, so viewers never get them.
    assertEquals(result.getFacets().size(), 0);
  }

  @Test
  public void testViewerPaginationAppliesAfterAuthorizationFilter() throws Exception {
    Urn first = UrnUtils.getUrn("urn:li:dataHubIngestionSource:first");
    Urn hidden = UrnUtils.getUrn("urn:li:dataHubIngestionSource:hidden");
    Urn second = UrnUtils.getUrn("urn:li:dataHubIngestionSource:second");
    Urn third = UrnUtils.getUrn("urn:li:dataHubIngestionSource:third");

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.searchAcrossEntities(
                any(),
                Mockito.eq(List.of(Constants.INGESTION_SOURCE_ENTITY_NAME)),
                Mockito.eq(""),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(ListIngestionSourcesResolver.AUTHORIZED_SOURCES_SCAN_LIMIT),
                Mockito.any(),
                Mockito.eq(List.of())))
        .thenReturn(searchResultOf(first, hidden, second, third));

    ListIngestionSourcesResolver resolver =
        new ListIngestionSourcesResolver(mockClient, viewPrivilegesEnabled());

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input")))
        .thenReturn(new ListIngestionSourcesInput(1, 1, null, null, null));
    QueryContext viewerContext = getMockViewerContext(Set.of(first, second, third));
    Mockito.when(mockEnv.getContext()).thenReturn(viewerContext);
    Mockito.when(mockEnv.getMergedField())
        .thenReturn(mergedListIngestionSourcesField("start", "count", "total"));
    var result = resolver.get(mockEnv).get();

    assertEquals(result.getStart(), 1);
    assertEquals(result.getCount(), 1);
    assertEquals(result.getTotal(), 3);
    assertEquals(result.getIngestionSources().size(), 1);
    assertEquals(result.getIngestionSources().get(0).getUrn(), second.toString());
  }

  @Test
  public void testViewerUnauthorizedWhenFlagDisabled() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    ListIngestionSourcesResolver resolver =
        new ListIngestionSourcesResolver(mockClient, new FeatureFlags());

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    QueryContext viewerContext = getMockViewerContext(Set.of(TEST_INGESTION_SOURCE_URN));
    Mockito.when(mockEnv.getContext()).thenReturn(viewerContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verifyNoInteractions(mockClient);
  }

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
    ListIngestionSourcesResolver resolver =
        new ListIngestionSourcesResolver(mockClient, new FeatureFlags());

    List<SortCriterion> result = resolver.buildSortCriteria(null);

    assertEquals(0, result.size());
  }

  @Test
  void testBuildSortCriteriaForNameField() {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    ListIngestionSourcesResolver resolver =
        new ListIngestionSourcesResolver(mockClient, new FeatureFlags());

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

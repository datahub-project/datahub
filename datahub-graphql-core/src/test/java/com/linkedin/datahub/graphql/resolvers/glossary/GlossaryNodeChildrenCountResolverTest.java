package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.GlossaryNodeChildrenCount;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.FilterValue;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GlossaryNodeChildrenCountResolverTest {
  private static final String TEST_GLOSSARY_NODE_URN = "urn:li:glossaryNode:test-id";

  private EntityClient _entityClient;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private GlossaryNodeChildrenCountResolver _resolver;
  private Entity _entity;

  @BeforeMethod
  public void setupTest() {
    _entityClient = Mockito.mock(EntityClient.class);
    _dataFetchingEnvironment = Mockito.mock(DataFetchingEnvironment.class);
    _entity = Mockito.mock(Entity.class);
    Mockito.when(_entity.getUrn()).thenReturn(TEST_GLOSSARY_NODE_URN);
    Mockito.when(_dataFetchingEnvironment.getSource()).thenReturn(_entity);

    _resolver = new GlossaryNodeChildrenCountResolver(_entityClient);
  }

  @Test
  public void testGetSuccess() throws Exception {
    // Setup mock search result with both terms and nodes
    SearchResult mockResult = new SearchResult();
    AggregationMetadata entityTypeAgg = new AggregationMetadata();
    entityTypeAgg.setName("_entityType");
    FilterValueArray filterValues = new FilterValueArray();
    filterValues.add(new FilterValue().setValue("glossaryterm").setFacetCount(5L));
    filterValues.add(new FilterValue().setValue("glossarynode").setFacetCount(3L));
    entityTypeAgg.setFilterValues(filterValues);

    AggregationMetadataArray aggregations = new AggregationMetadataArray();
    aggregations.add(entityTypeAgg);
    mockResult.setMetadata(
        new com.linkedin.metadata.search.SearchResultMetadata().setAggregations(aggregations));

    Mockito.when(
            _entityClient.searchAcrossEntities(
                any(),
                eq(
                    ImmutableList.of(
                        Constants.GLOSSARY_TERM_ENTITY_NAME, Constants.GLOSSARY_NODE_ENTITY_NAME)),
                eq("*"),
                any(Filter.class),
                eq(0),
                eq(0),
                eq(Collections.emptyList()),
                eq(ImmutableList.of("_entityType"))))
        .thenReturn(mockResult);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    GlossaryNodeChildrenCount result = _resolver.get(_dataFetchingEnvironment).get();

    // Verify results
    assertEquals(result.getTermsCount(), 5);
    assertEquals(result.getNodesCount(), 3);
  }

  @Test
  public void testGetNoChildren() throws Exception {
    // Setup mock search result with no children
    SearchResult mockResult = new SearchResult();
    AggregationMetadata entityTypeAgg = new AggregationMetadata();
    entityTypeAgg.setName("_entityType");
    entityTypeAgg.setFilterValues(new FilterValueArray());

    AggregationMetadataArray aggregations = new AggregationMetadataArray();
    aggregations.add(entityTypeAgg);
    mockResult.setMetadata(
        new com.linkedin.metadata.search.SearchResultMetadata().setAggregations(aggregations));

    Mockito.when(
            _entityClient.searchAcrossEntities(
                any(),
                eq(
                    ImmutableList.of(
                        Constants.GLOSSARY_TERM_ENTITY_NAME, Constants.GLOSSARY_NODE_ENTITY_NAME)),
                eq("*"),
                any(Filter.class),
                eq(0),
                eq(0),
                eq(Collections.emptyList()),
                eq(ImmutableList.of("_entityType"))))
        .thenReturn(mockResult);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    GlossaryNodeChildrenCount result = _resolver.get(_dataFetchingEnvironment).get();

    // Verify results
    assertEquals(result.getTermsCount(), 0);
    assertEquals(result.getNodesCount(), 0);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Execute resolver with unauthorized context
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Setup mock to throw exception
    Mockito.when(
            _entityClient.searchAcrossEntities(
                any(),
                eq(
                    ImmutableList.of(
                        Constants.GLOSSARY_TERM_ENTITY_NAME, Constants.GLOSSARY_NODE_ENTITY_NAME)),
                eq("*"),
                any(Filter.class),
                eq(0),
                eq(0),
                eq(Collections.emptyList()),
                eq(ImmutableList.of("_entityType"))))
        .thenThrow(new RemoteInvocationException("Failed to search"));

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testGetInvalidUrn() throws Exception {
    // Setup entity with invalid URN
    Mockito.when(_entity.getUrn()).thenReturn("invalid-urn");

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(URISyntaxException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
  }
}

package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.LineageDirection;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageInput;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageResult;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageResults;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.LineageSearchEntity;
import com.linkedin.metadata.search.LineageSearchEntityArray;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.MatchedFieldArray;
import com.linkedin.metadata.search.SearchResultMetadata;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

// Initialize this class in the style of SearchAcrossEntitiesResolverTest.java
public class SearchAcrossLineageResolverTest {
  private static final String SOURCE_URN_STRING =
      "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)";
  private static final String TARGET_URN_STRING =
      "urn:li:dataset:(urn:li:dataPlatform:foo,baz,PROD)";
  private static final String QUERY = "";
  private static final int START = 0;
  private static final int COUNT = 10;
  private static final Long START_TIMESTAMP_MILLIS = 0L;
  private static final Long END_TIMESTAMP_MILLIS = 1000L;
  private EntityClient _entityClient;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;
  private SearchAcrossLineageResolver _resolver;

  private EntityRegistry _entityRegistry;

  @BeforeMethod
  public void setupTest() {
    _entityClient = mock(EntityClient.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    _entityRegistry = mock(EntityRegistry.class);
    _resolver = new SearchAcrossLineageResolver(_entityClient, _entityRegistry);
  }

  @Test
  public void testAllEntitiesInitialization() {
    InputStream inputStream = ClassLoader.getSystemResourceAsStream("entity-registry.yml");
    EntityRegistry entityRegistry = new ConfigEntityRegistry(inputStream);
    SearchAcrossLineageResolver resolver =
        new SearchAcrossLineageResolver(_entityClient, entityRegistry);
    assertTrue(resolver._allEntities.contains("dataset"));
    assertTrue(resolver._allEntities.contains("dataFlow"));
    // Test for case sensitivity
    assertFalse(resolver._allEntities.contains("dataflow"));
  }

  @Test
  public void testSearchAcrossLineage() throws Exception {
    final QueryContext mockContext = getMockAllowContext();
    when(mockContext.getAuthentication()).thenReturn(_authentication);

    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    final SearchAcrossLineageInput input = new SearchAcrossLineageInput();
    input.setCount(COUNT);
    input.setDirection(LineageDirection.DOWNSTREAM);
    input.setOrFilters(Collections.emptyList());
    input.setQuery(QUERY);
    input.setStart(START);
    input.setTypes(Collections.emptyList());
    input.setStartTimeMillis(START_TIMESTAMP_MILLIS);
    input.setEndTimeMillis(END_TIMESTAMP_MILLIS);
    input.setUrn(SOURCE_URN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    final LineageSearchResult lineageSearchResult = new LineageSearchResult();
    lineageSearchResult.setNumEntities(1);
    lineageSearchResult.setFrom(0);
    lineageSearchResult.setPageSize(10);

    final SearchResultMetadata searchResultMetadata = new SearchResultMetadata();
    searchResultMetadata.setAggregations(new AggregationMetadataArray());
    lineageSearchResult.setMetadata(searchResultMetadata);

    final LineageSearchEntity lineageSearchEntity = new LineageSearchEntity();
    lineageSearchEntity.setEntity(UrnUtils.getUrn(TARGET_URN_STRING));
    lineageSearchEntity.setScore(15.0);
    lineageSearchEntity.setDegree(1);
    lineageSearchEntity.setMatchedFields(new MatchedFieldArray());
    lineageSearchEntity.setPaths(new UrnArrayArray());
    lineageSearchResult.setEntities(new LineageSearchEntityArray(lineageSearchEntity));
    ArgumentCaptor<OperationContext> opContext = ArgumentCaptor.forClass(OperationContext.class);

    when(_entityClient.searchAcrossLineage(
            opContext.capture(),
            eq(UrnUtils.getUrn(SOURCE_URN_STRING)),
            eq(com.linkedin.metadata.graph.LineageDirection.DOWNSTREAM),
            anyList(),
            eq(QUERY),
            eq(null),
            any(),
            eq(null),
            eq(START),
            eq(COUNT)))
        .thenReturn(lineageSearchResult);

    final SearchAcrossLineageResults results = _resolver.get(_dataFetchingEnvironment).join();
    assertEquals(results.getCount(), 10);
    assertEquals(results.getTotal(), 1);
    assertEquals(
        opContext.getValue().getSearchContext().getLineageFlags().getStartTimeMillis(),
        START_TIMESTAMP_MILLIS);
    assertEquals(
        opContext.getValue().getSearchContext().getLineageFlags().getEndTimeMillis(),
        END_TIMESTAMP_MILLIS);

    final List<SearchAcrossLineageResult> entities = results.getSearchResults();
    assertEquals(entities.size(), 1);
    final SearchAcrossLineageResult entity = entities.get(0);
    assertEquals(entity.getEntity().getUrn(), TARGET_URN_STRING);
    assertEquals(entity.getEntity().getType(), EntityType.DATASET);
  }
}

package com.linkedin.datahub.graphql.resolvers.organization;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ListOrganizationsInput;
import com.linkedin.datahub.graphql.generated.ListOrganizationsResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ListOrganizationsResolverTest {

  private EntityClient _entityClient;
  private ListOrganizationsResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;

  @BeforeMethod
  public void setupTest() {
    _entityClient = Mockito.mock(EntityClient.class);
    _dataFetchingEnvironment = Mockito.mock(DataFetchingEnvironment.class);
    _resolver = new ListOrganizationsResolver(_entityClient);
  }

  @Test
  public void testListOrganizationsSuccess() throws Exception {
    ListOrganizationsInput input = new ListOrganizationsInput();
    input.setStart(0);
    input.setCount(10);

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    SearchResult searchResult = new SearchResult();
    searchResult.setFrom(0);
    searchResult.setPageSize(10);
    searchResult.setNumEntities(1);
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(Urn.createFromString("urn:li:organization:test-org"));
    searchResult.setEntities(new SearchEntityArray(searchEntity));

    Mockito.when(
            _entityClient.search(
                any(), anyString(), anyString(), any(), anyList(), anyInt(), anyInt()))
        .thenReturn(searchResult);

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(new EnvelopedAspectMap());
    Map<Urn, EntityResponse> batchResponse = new HashMap<>();
    batchResponse.put(Urn.createFromString("urn:li:organization:test-org"), entityResponse);

    Mockito.when(_entityClient.batchGetV2(any(), anyString(), any(), any()))
        .thenReturn(batchResponse);

    ListOrganizationsResult result = _resolver.get(_dataFetchingEnvironment).get();

    assertNotNull(result);
    assertEquals(result.getTotal(), Integer.valueOf(1));
    assertEquals(result.getStart(), Integer.valueOf(0));
    assertEquals(result.getCount(), Integer.valueOf(10));
  }

  @Test
  public void testListOrganizationsWithDefaults() throws Exception {
    ListOrganizationsInput input = new ListOrganizationsInput();

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    SearchResult searchResult = new SearchResult();
    searchResult.setFrom(0);
    searchResult.setPageSize(20);
    searchResult.setNumEntities(0);
    searchResult.setEntities(new SearchEntityArray());

    Mockito.when(
            _entityClient.search(
                any(), anyString(), anyString(), any(), anyList(), anyInt(), anyInt()))
        .thenReturn(searchResult);
    Mockito.when(_entityClient.batchGetV2(any(), anyString(), any(), any()))
        .thenReturn(Collections.emptyMap());

    ListOrganizationsResult result = _resolver.get(_dataFetchingEnvironment).get();

    assertNotNull(result);
    assertEquals(result.getTotal(), Integer.valueOf(0));
  }

  @Test
  public void testListOrganizationsWithNegativeStart() throws Exception {
    ListOrganizationsInput input = new ListOrganizationsInput();
    input.setStart(-1);
    input.setCount(10);

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    SearchResult searchResult = new SearchResult();
    searchResult.setFrom(0);
    searchResult.setPageSize(10);
    searchResult.setNumEntities(0);
    searchResult.setEntities(new SearchEntityArray());

    Mockito.when(
            _entityClient.search(
                any(), anyString(), anyString(), any(), anyList(), anyInt(), anyInt()))
        .thenReturn(searchResult);
    Mockito.when(_entityClient.batchGetV2(any(), anyString(), any(), any()))
        .thenReturn(Collections.emptyMap());

    ListOrganizationsResult result = _resolver.get(_dataFetchingEnvironment).get();

    assertNotNull(result);
    assertEquals(result.getStart(), Integer.valueOf(0));
  }

  @Test
  public void testListOrganizationsWithZeroCount() throws Exception {
    ListOrganizationsInput input = new ListOrganizationsInput();
    input.setStart(0);
    input.setCount(0);

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    SearchResult searchResult = new SearchResult();
    searchResult.setFrom(0);
    searchResult.setPageSize(20);
    searchResult.setNumEntities(0);
    searchResult.setEntities(new SearchEntityArray());

    Mockito.when(
            _entityClient.search(
                any(), anyString(), anyString(), any(), anyList(), anyInt(), anyInt()))
        .thenReturn(searchResult);
    Mockito.when(_entityClient.batchGetV2(any(), anyString(), any(), any()))
        .thenReturn(Collections.emptyMap());

    ListOrganizationsResult result = _resolver.get(_dataFetchingEnvironment).get();

    assertNotNull(result);
    assertEquals(result.getCount(), Integer.valueOf(20));
  }
}

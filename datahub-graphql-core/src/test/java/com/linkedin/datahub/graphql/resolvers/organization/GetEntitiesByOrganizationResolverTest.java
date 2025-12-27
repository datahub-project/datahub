package com.linkedin.datahub.graphql.resolvers.organization;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GetEntitiesByOrganizationResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import java.util.Arrays;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GetEntitiesByOrganizationResolverTest {

  private static final String ORGANIZATION_URN = "urn:li:organization:test-org";
  private EntityClient _entityClient;
  private GetEntitiesByOrganizationResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;

  @BeforeMethod
  public void setupTest() {
    _entityClient = Mockito.mock(EntityClient.class);
    _dataFetchingEnvironment = Mockito.mock(DataFetchingEnvironment.class);
    _resolver = new GetEntitiesByOrganizationResolver(_entityClient);
  }

  @Test
  public void testGetEntitiesSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("organizationUrn")))
        .thenReturn(ORGANIZATION_URN);
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("entityTypes"))).thenReturn(null);
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("start"))).thenReturn(0);
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("count"))).thenReturn(10);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    SearchResult searchResult = new SearchResult();
    searchResult.setFrom(0);
    searchResult.setPageSize(10);
    searchResult.setNumEntities(1);
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(Urn.createFromString("urn:li:dataset:(test,test,PROD)"));
    searchResult.setEntities(new SearchEntityArray(searchEntity));

    Mockito.when(
            _entityClient.searchAcrossEntities(
                any(), anyList(), anyString(), any(), anyInt(), anyInt(), anyList()))
        .thenReturn(searchResult);

    GetEntitiesByOrganizationResult result = _resolver.get(_dataFetchingEnvironment).get();

    assertNotNull(result);
    assertEquals(result.getTotal(), Integer.valueOf(1));
    assertEquals(result.getStart(), Integer.valueOf(0));
    assertEquals(result.getCount(), Integer.valueOf(10));
  }

  @Test
  public void testGetEntitiesWithEntityTypes() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("organizationUrn")))
        .thenReturn(ORGANIZATION_URN);
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("entityTypes")))
        .thenReturn(Arrays.asList("DATASET", "CHART"));
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("start"))).thenReturn(0);
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("count"))).thenReturn(10);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    SearchResult searchResult = new SearchResult();
    searchResult.setFrom(0);
    searchResult.setPageSize(10);
    searchResult.setNumEntities(0);
    searchResult.setEntities(new SearchEntityArray());

    Mockito.when(
            _entityClient.searchAcrossEntities(
                any(), anyList(), anyString(), any(), anyInt(), anyInt(), anyList()))
        .thenReturn(searchResult);

    GetEntitiesByOrganizationResult result = _resolver.get(_dataFetchingEnvironment).get();

    assertNotNull(result);
    assertEquals(result.getTotal(), Integer.valueOf(0));
  }

  @Test
  public void testGetEntitiesUnauthorized() {
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("organizationUrn")))
        .thenReturn(ORGANIZATION_URN);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testGetEntitiesWithInvalidEntityType() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("organizationUrn")))
        .thenReturn(ORGANIZATION_URN);
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("entityTypes")))
        .thenReturn(Arrays.asList("DATASET", "INVALID_TYPE"));
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("start"))).thenReturn(0);
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("count"))).thenReturn(10);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    SearchResult searchResult = new SearchResult();
    searchResult.setFrom(0);
    searchResult.setPageSize(10);
    searchResult.setNumEntities(0);
    searchResult.setEntities(new SearchEntityArray());

    Mockito.when(
            _entityClient.searchAcrossEntities(
                any(), anyList(), anyString(), any(), anyInt(), anyInt(), anyList()))
        .thenReturn(searchResult);

    GetEntitiesByOrganizationResult result = _resolver.get(_dataFetchingEnvironment).get();

    assertNotNull(result);
  }

  @Test
  public void testGetEntitiesWithNullSearchResult() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("organizationUrn")))
        .thenReturn(ORGANIZATION_URN);
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("entityTypes"))).thenReturn(null);
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("start"))).thenReturn(0);
    Mockito.when(_dataFetchingEnvironment.getArgument(eq("count"))).thenReturn(10);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    SearchResult searchResult = new SearchResult();
    searchResult.setFrom(0);
    searchResult.setPageSize(10);
    searchResult.setNumEntities(0);
    searchResult.setEntities(new SearchEntityArray());

    Mockito.when(
            _entityClient.searchAcrossEntities(
                any(), anyList(), anyString(), any(), anyInt(), anyInt(), anyList()))
        .thenReturn(searchResult);

    GetEntitiesByOrganizationResult result = _resolver.get(_dataFetchingEnvironment).get();

    assertNotNull(result);
    assertEquals(result.getEntities().size(), 0);
  }
}

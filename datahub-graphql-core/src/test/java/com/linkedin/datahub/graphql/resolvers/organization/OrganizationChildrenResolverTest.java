package com.linkedin.datahub.graphql.resolvers.organization;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Organization;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OrganizationChildrenResolverTest {

  private static final String PARENT_ORG_URN = "urn:li:organization:parent-org";

  private EntityClient _entityClient;
  private OrganizationChildrenResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;

  @BeforeMethod
  public void setupTest() {
    _entityClient = Mockito.mock(EntityClient.class);
    _dataFetchingEnvironment = Mockito.mock(DataFetchingEnvironment.class);
    _resolver = new OrganizationChildrenResolver(_entityClient);
  }

  @Test
  public void testGetChildrenSuccess() throws Exception {
    Organization parentOrg = new Organization();
    parentOrg.setUrn(PARENT_ORG_URN);

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getSource()).thenReturn(parentOrg);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(1);
    SearchEntity searchEntity = new SearchEntity();
    searchEntity.setEntity(Urn.createFromString("urn:li:organization:child-org"));
    searchResult.setEntities(new com.linkedin.metadata.search.SearchEntityArray(searchEntity));

    Mockito.when(_entityClient.filter(any(), anyString(), any(), any(), anyInt(), anyInt()))
        .thenReturn(searchResult);

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(new EnvelopedAspectMap());
    Map<Urn, EntityResponse> batchResponse = new HashMap<>();
    batchResponse.put(Urn.createFromString("urn:li:organization:child-org"), entityResponse);

    Mockito.when(_entityClient.batchGetV2(any(), anyString(), any(), any()))
        .thenReturn(batchResponse);

    List<Organization> result = _resolver.get(_dataFetchingEnvironment).get();

    assertNotNull(result);
    assertEquals(result.size(), 1);
  }

  @Test
  public void testGetChildrenUnauthorized() throws Exception {
    Organization parentOrg = new Organization();
    parentOrg.setUrn(PARENT_ORG_URN);

    QueryContext mockContext = getMockDenyContext();
    Mockito.when(_dataFetchingEnvironment.getSource()).thenReturn(parentOrg);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    List<Organization> result = _resolver.get(_dataFetchingEnvironment).get();

    assertNotNull(result);
    assertEquals(result.size(), 0);
  }

  @Test
  public void testGetChildrenEmpty() throws Exception {
    Organization parentOrg = new Organization();
    parentOrg.setUrn(PARENT_ORG_URN);

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getSource()).thenReturn(parentOrg);
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(0);
    searchResult.setEntities(new com.linkedin.metadata.search.SearchEntityArray());

    Mockito.when(_entityClient.filter(any(), anyString(), any(), any(), anyInt(), anyInt()))
        .thenReturn(searchResult);

    List<Organization> result = _resolver.get(_dataFetchingEnvironment).get();

    assertNotNull(result);
    assertEquals(result.size(), 0);
  }
}

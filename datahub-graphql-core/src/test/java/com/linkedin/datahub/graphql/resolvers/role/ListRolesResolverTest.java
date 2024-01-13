package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.anyInt;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ListRolesInput;
import com.linkedin.datahub.graphql.generated.ListRolesResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.policy.DataHubRoleInfo;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ListRolesResolverTest {
  private static final String ADMIN_ROLE_URN_STRING = "urn:li:dataHubRole:Admin";
  private static final String EDITOR_ROLE_URN_STRING = "urn:li:dataHubRole:Editor";
  private static Map<Urn, EntityResponse> _entityResponseMap;

  private EntityClient _entityClient;
  private ListRolesResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  private EntityResponse getMockRoleEntityResponse(Urn roleUrn) {
    EntityResponse entityResponse = new EntityResponse().setUrn(roleUrn);
    DataHubRoleInfo dataHubRoleInfo = new DataHubRoleInfo();
    dataHubRoleInfo.setDescription(roleUrn.toString());
    dataHubRoleInfo.setName(roleUrn.toString());
    entityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                DATAHUB_ROLE_INFO_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(dataHubRoleInfo.data())))));

    return entityResponse;
  }

  @BeforeMethod
  public void setupTest() throws Exception {
    Urn adminRoleUrn = Urn.createFromString(ADMIN_ROLE_URN_STRING);
    Urn editorRoleUrn = Urn.createFromString(EDITOR_ROLE_URN_STRING);
    _entityResponseMap =
        ImmutableMap.of(
            adminRoleUrn,
            getMockRoleEntityResponse(adminRoleUrn),
            editorRoleUrn,
            getMockRoleEntityResponse(editorRoleUrn));

    _entityClient = mock(EntityClient.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    _resolver = new ListRolesResolver(_entityClient);
  }

  @Test
  public void testNotAuthorizedFails() {
    QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testListRoles() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);

    ListRolesInput input = new ListRolesInput();
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);
    final SearchResult roleSearchResult =
        new SearchResult()
            .setMetadata(new SearchResultMetadata())
            .setFrom(0)
            .setPageSize(10)
            .setNumEntities(2);
    roleSearchResult.setEntities(
        new SearchEntityArray(
            ImmutableList.of(
                new SearchEntity().setEntity(Urn.createFromString(ADMIN_ROLE_URN_STRING)),
                new SearchEntity().setEntity(Urn.createFromString(EDITOR_ROLE_URN_STRING)))));

    when(_entityClient.search(
            eq(DATAHUB_ROLE_ENTITY_NAME),
            any(),
            any(),
            anyInt(),
            anyInt(),
            any(),
            Mockito.eq(new SearchFlags().setFulltext(true))))
        .thenReturn(roleSearchResult);
    when(_entityClient.batchGetV2(eq(DATAHUB_ROLE_ENTITY_NAME), any(), any(), any()))
        .thenReturn(_entityResponseMap);

    ListRolesResult result = _resolver.get(_dataFetchingEnvironment).join();
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 10);
    assertEquals(result.getTotal(), 2);
    assertEquals(result.getRoles().size(), 2);
  }
}

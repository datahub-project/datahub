package com.linkedin.datahub.graphql.resolvers.user;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ListUsersInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ListUsersResolverTest {

  private static final Urn TEST_USER_URN = Urn.createFromTuple("corpuser", "test");

  private static final ListUsersInput TEST_INPUT = new ListUsersInput();

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Mock search result
    SearchResult searchResult =
        new SearchResult()
            .setFrom(0)
            .setPageSize(1)
            .setNumEntities(1)
            .setEntities(
                new SearchEntityArray(
                    ImmutableSet.of(new SearchEntity().setEntity(TEST_USER_URN))));

    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(CORP_USER_ENTITY_NAME),
                Mockito.eq(""),
                Mockito.eq(Collections.emptyMap()),
                Mockito.eq(0),
                Mockito.eq(20)))
        .thenReturn(searchResult);

    // Mock batchGetV2 result
    EntityResponse entityResponse = createMockEntityResponse(TEST_USER_URN);
    Map<Urn, EntityResponse> entityMap = new HashMap<>();
    entityMap.put(TEST_USER_URN, entityResponse);

    Mockito.when(
            mockClient.batchGetV2(
                any(), Mockito.eq(CORP_USER_ENTITY_NAME), any(), Mockito.isNull()))
        .thenReturn(entityMap);

    ListUsersResolver resolver = new ListUsersResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Data Assertions
    var result = resolver.get(mockEnv).get();
    assertEquals((int) result.getStart(), 0);
    assertEquals((int) result.getCount(), 1);
    assertEquals((int) result.getTotal(), 1);
    assertNotNull(result.getUsers());
    assertEquals(result.getUsers().size(), 1);
    assertEquals(result.getUsers().get(0).getUrn(), TEST_USER_URN.toString());
  }

  @Test
  public void testGetWithCustomInput() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Create custom input
    ListUsersInput customInput = new ListUsersInput();
    customInput.setStart(10);
    customInput.setCount(5);
    customInput.setQuery("test query");

    // Mock search result
    SearchResult searchResult =
        new SearchResult()
            .setFrom(10)
            .setPageSize(5)
            .setNumEntities(0)
            .setEntities(new SearchEntityArray(ImmutableSet.of()));

    Mockito.when(
            mockClient.search(
                any(),
                Mockito.eq(CORP_USER_ENTITY_NAME),
                Mockito.eq("test query"),
                Mockito.eq(Collections.emptyMap()),
                Mockito.eq(10),
                Mockito.eq(5)))
        .thenReturn(searchResult);

    // Mock batchGetV2 result (empty)
    Mockito.when(
            mockClient.batchGetV2(
                any(), Mockito.eq(CORP_USER_ENTITY_NAME), any(), Mockito.isNull()))
        .thenReturn(Collections.emptyMap());

    ListUsersResolver resolver = new ListUsersResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(customInput);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Data Assertions
    var result = resolver.get(mockEnv).get();
    assertEquals((int) result.getStart(), 10);
    assertEquals((int) result.getCount(), 5);
    assertEquals((int) result.getTotal(), 0);
    assertNotNull(result.getUsers());
    assertEquals(result.getUsers().size(), 0);
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .search(any(), any(), Mockito.eq(""), Mockito.anyMap(), Mockito.anyInt(), Mockito.anyInt());
    ListUsersResolver resolver = new ListUsersResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private EntityResponse createMockEntityResponse(Urn userUrn) {
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(userUrn);

    // Create CorpUserKey aspect
    CorpUserKey corpUserKey = new CorpUserKey();
    corpUserKey.setUsername(userUrn.getId());

    EnvelopedAspect keyAspect = new EnvelopedAspect();
    keyAspect.setValue(new Aspect(corpUserKey.data()));

    // Create CorpUserInfo aspect
    CorpUserInfo corpUserInfo = new CorpUserInfo();
    corpUserInfo.setActive(true);
    corpUserInfo.setFullName("Test User");
    corpUserInfo.setEmail("test@example.com");

    EnvelopedAspect infoAspect = new EnvelopedAspect();
    infoAspect.setValue(new Aspect(corpUserInfo.data()));

    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(Constants.CORP_USER_KEY_ASPECT_NAME, keyAspect);
    aspects.put(Constants.CORP_USER_INFO_ASPECT_NAME, infoAspect);

    entityResponse.setAspects(new EnvelopedAspectMap(aspects));
    return entityResponse;
  }
}

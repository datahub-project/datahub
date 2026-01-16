package com.linkedin.datahub.graphql.resolvers.auth;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ListServiceAccountsInput;
import com.linkedin.datahub.graphql.generated.ListServiceAccountsResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ListServiceAccountsResolverTest {

  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";

  private EntityClient mockClient;
  private DataFetchingEnvironment mockEnv;
  private ListServiceAccountsResolver resolver;

  @BeforeMethod
  public void setup() {
    mockClient = mock(EntityClient.class);
    mockEnv = mock(DataFetchingEnvironment.class);
    resolver = new ListServiceAccountsResolver(mockClient);
  }

  @Test
  public void testListServiceAccountsSuccess() throws Exception {
    // Setup
    QueryContext mockContext = TestUtils.getMockAllowContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);

    ListServiceAccountsInput input = new ListServiceAccountsInput();
    input.setStart(0);
    input.setCount(10);
    when(mockEnv.getArgument("input")).thenReturn(input);

    // Create mock search results
    SearchEntityArray entities = new SearchEntityArray();
    entities.add(
        new SearchEntity()
            .setEntity(Urn.createFromString("urn:li:corpuser:service_ingestion-pipeline")));
    entities.add(
        new SearchEntity()
            .setEntity(Urn.createFromString("urn:li:corpuser:service_monitoring-service")));

    SearchResult searchResult =
        new SearchResult().setFrom(0).setPageSize(10).setNumEntities(2).setEntities(entities);

    when(mockClient.search(
            any(),
            eq(Constants.CORP_USER_ENTITY_NAME),
            anyString(),
            any(),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(searchResult);

    // Execute
    ListServiceAccountsResult result = resolver.get(mockEnv).get();

    // Verify
    assertNotNull(result);
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 10);
    assertEquals(result.getTotal(), 2);
    assertEquals(result.getServiceAccounts().size(), 2);

    // Verify names are extracted correctly
    assertEquals(result.getServiceAccounts().get(0).getName(), "ingestion-pipeline");
    assertEquals(result.getServiceAccounts().get(1).getName(), "monitoring-service");
  }

  @Test
  public void testListServiceAccountsWithQuery() throws Exception {
    // Setup
    QueryContext mockContext = TestUtils.getMockAllowContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);

    ListServiceAccountsInput input = new ListServiceAccountsInput();
    input.setStart(0);
    input.setCount(10);
    input.setQuery("ingestion");
    when(mockEnv.getArgument("input")).thenReturn(input);

    SearchEntityArray entities = new SearchEntityArray();
    entities.add(
        new SearchEntity()
            .setEntity(Urn.createFromString("urn:li:corpuser:service_ingestion-pipeline")));

    SearchResult searchResult =
        new SearchResult().setFrom(0).setPageSize(10).setNumEntities(1).setEntities(entities);

    when(mockClient.search(
            any(),
            eq(Constants.CORP_USER_ENTITY_NAME),
            eq("ingestion"),
            any(),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(searchResult);

    // Execute
    ListServiceAccountsResult result = resolver.get(mockEnv).get();

    // Verify
    assertNotNull(result);
    assertEquals(result.getTotal(), 1);
    assertEquals(result.getServiceAccounts().get(0).getName(), "ingestion-pipeline");
  }

  @Test
  public void testListServiceAccountsEmpty() throws Exception {
    // Setup
    QueryContext mockContext = TestUtils.getMockAllowContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);

    ListServiceAccountsInput input = new ListServiceAccountsInput();
    input.setStart(0);
    input.setCount(10);
    when(mockEnv.getArgument("input")).thenReturn(input);

    SearchResult searchResult =
        new SearchResult()
            .setFrom(0)
            .setPageSize(10)
            .setNumEntities(0)
            .setEntities(new SearchEntityArray());

    when(mockClient.search(
            any(),
            eq(Constants.CORP_USER_ENTITY_NAME),
            anyString(),
            any(),
            any(),
            anyInt(),
            anyInt()))
        .thenReturn(searchResult);

    // Execute
    ListServiceAccountsResult result = resolver.get(mockEnv).get();

    // Verify
    assertNotNull(result);
    assertEquals(result.getTotal(), 0);
    assertTrue(result.getServiceAccounts().isEmpty());
  }

  @Test
  public void testListServiceAccountsUnauthorized() throws Exception {
    // Setup
    QueryContext mockContext = TestUtils.getMockDenyContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);

    ListServiceAccountsInput input = new ListServiceAccountsInput();
    input.setStart(0);
    input.setCount(10);
    when(mockEnv.getArgument("input")).thenReturn(input);

    // Execute & Verify
    try {
      resolver.get(mockEnv).get();
      fail("Expected ExecutionException");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof AuthorizationException);
    }
  }

  @Test
  public void testListServiceAccountsWithDefaultPagination() throws Exception {
    // Setup
    QueryContext mockContext = TestUtils.getMockAllowContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);

    // Input with null start and count - should use defaults
    ListServiceAccountsInput input = new ListServiceAccountsInput();
    when(mockEnv.getArgument("input")).thenReturn(input);

    SearchResult searchResult =
        new SearchResult()
            .setFrom(0)
            .setPageSize(10)
            .setNumEntities(0)
            .setEntities(new SearchEntityArray());

    // Verify defaults are used: start=0, count=10
    when(mockClient.search(
            any(), eq(Constants.CORP_USER_ENTITY_NAME), eq("*"), any(), any(), eq(0), eq(10)))
        .thenReturn(searchResult);

    // Execute
    ListServiceAccountsResult result = resolver.get(mockEnv).get();

    // Verify
    assertNotNull(result);
  }
}

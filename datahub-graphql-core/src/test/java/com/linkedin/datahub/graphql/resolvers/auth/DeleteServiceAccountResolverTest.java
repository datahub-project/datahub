package com.linkedin.datahub.graphql.resolvers.auth;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeleteServiceAccountResolverTest {

  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";
  private static final String TEST_SERVICE_ACCOUNT_URN =
      "urn:li:corpuser:service_ingestion-pipeline";

  private EntityClient mockClient;
  private DataFetchingEnvironment mockEnv;
  private DeleteServiceAccountResolver resolver;

  @BeforeMethod
  public void setup() {
    mockClient = mock(EntityClient.class);
    mockEnv = mock(DataFetchingEnvironment.class);
    resolver = new DeleteServiceAccountResolver(mockClient);
  }

  @Test
  public void testDeleteServiceAccountSuccess() throws Exception {
    // Setup
    QueryContext mockContext = TestUtils.getMockAllowContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument("urn")).thenReturn(TEST_SERVICE_ACCOUNT_URN);

    EntityResponse entityResponse = createMockServiceAccountResponse(true);
    Map<Urn, EntityResponse> responseMap = new HashMap<>();
    responseMap.put(Urn.createFromString(TEST_SERVICE_ACCOUNT_URN), entityResponse);

    when(mockClient.batchGetV2(any(), eq(Constants.CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(responseMap);

    // Execute
    Boolean result = resolver.get(mockEnv).get();

    // Verify
    assertTrue(result);
    verify(mockClient).deleteEntity(any(), eq(Urn.createFromString(TEST_SERVICE_ACCOUNT_URN)));
  }

  @Test
  public void testDeleteServiceAccountNotFound() throws Exception {
    // Setup
    QueryContext mockContext = TestUtils.getMockAllowContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument("urn")).thenReturn(TEST_SERVICE_ACCOUNT_URN);

    // Return empty map (entity not found)
    when(mockClient.batchGetV2(any(), eq(Constants.CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(Collections.emptyMap());

    // Execute & Verify
    try {
      resolver.get(mockEnv).get();
      fail("Expected ExecutionException");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
      assertTrue(e.getCause().getMessage().contains("not found"));
    }

    // Verify delete was not called
    verify(mockClient, never()).deleteEntity(any(), any());
  }

  @Test
  public void testDeleteServiceAccountNotAServiceAccount() throws Exception {
    // Setup
    QueryContext mockContext = TestUtils.getMockAllowContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument("urn")).thenReturn("urn:li:corpuser:regular-user");

    // Return a regular user (not a service account)
    EntityResponse entityResponse = createMockServiceAccountResponse(false);
    Map<Urn, EntityResponse> responseMap = new HashMap<>();
    responseMap.put(Urn.createFromString("urn:li:corpuser:regular-user"), entityResponse);

    when(mockClient.batchGetV2(any(), eq(Constants.CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(responseMap);

    // Execute & Verify
    try {
      resolver.get(mockEnv).get();
      fail("Expected ExecutionException");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
      assertTrue(e.getCause().getMessage().contains("not a service account"));
    }

    // Verify delete was not called
    verify(mockClient, never()).deleteEntity(any(), any());
  }

  @Test
  public void testDeleteServiceAccountUnauthorized() throws Exception {
    // Setup
    QueryContext mockContext = TestUtils.getMockDenyContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument("urn")).thenReturn(TEST_SERVICE_ACCOUNT_URN);

    // Execute & Verify
    try {
      resolver.get(mockEnv).get();
      fail("Expected ExecutionException");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof AuthorizationException);
    }

    // Verify delete was not called
    verify(mockClient, never()).deleteEntity(any(), any());
  }

  private EntityResponse createMockServiceAccountResponse(boolean isServiceAccount)
      throws Exception {
    EntityResponse response = mock(EntityResponse.class);
    when(response.getUrn()).thenReturn(Urn.createFromString(TEST_SERVICE_ACCOUNT_URN));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    if (isServiceAccount) {
      SubTypes subTypes = new SubTypes();
      subTypes.setTypeNames(new StringArray(ServiceAccountUtils.SERVICE_ACCOUNT_SUB_TYPE));
      EnvelopedAspect subTypesAspect = new EnvelopedAspect();
      subTypesAspect.setValue(new Aspect(subTypes.data()));
      aspectMap.put(Constants.SUB_TYPES_ASPECT_NAME, subTypesAspect);
    }

    when(response.getAspects()).thenReturn(aspectMap);
    return response;
  }
}

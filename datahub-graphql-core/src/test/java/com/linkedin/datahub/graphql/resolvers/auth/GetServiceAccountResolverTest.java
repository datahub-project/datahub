package com.linkedin.datahub.graphql.resolvers.auth;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ServiceAccount;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GetServiceAccountResolverTest {

  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";
  private static final String TEST_SERVICE_ACCOUNT_URN =
      "urn:li:corpuser:service_ingestion-pipeline";
  private static final String TEST_SERVICE_ACCOUNT_NAME = "ingestion-pipeline";

  private EntityClient mockClient;
  private DataFetchingEnvironment mockEnv;
  private GetServiceAccountResolver resolver;

  @BeforeMethod
  public void setup() {
    mockClient = mock(EntityClient.class);
    mockEnv = mock(DataFetchingEnvironment.class);
    resolver = new GetServiceAccountResolver(mockClient);
  }

  @Test
  public void testGetServiceAccountSuccess() throws Exception {
    // Setup
    QueryContext mockContext = TestUtils.getMockAllowContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument("urn")).thenReturn(TEST_SERVICE_ACCOUNT_URN);

    EntityResponse entityResponse = createMockServiceAccountResponse(true, true);
    Map<Urn, EntityResponse> responseMap = new HashMap<>();
    responseMap.put(Urn.createFromString(TEST_SERVICE_ACCOUNT_URN), entityResponse);

    when(mockClient.batchGetV2(any(), eq(Constants.CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(responseMap);

    // Execute
    ServiceAccount result = resolver.get(mockEnv).get();

    // Verify
    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_SERVICE_ACCOUNT_URN);
    assertEquals(result.getName(), TEST_SERVICE_ACCOUNT_NAME);
    assertEquals(result.getType(), EntityType.CORP_USER);
    assertEquals(result.getDisplayName(), "Ingestion Pipeline");
    assertEquals(result.getDescription(), "Test description");
  }

  @Test
  public void testGetServiceAccountNotFound() throws Exception {
    // Setup
    QueryContext mockContext = TestUtils.getMockAllowContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument("urn")).thenReturn(TEST_SERVICE_ACCOUNT_URN);

    // Return empty map (entity not found)
    when(mockClient.batchGetV2(any(), eq(Constants.CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(Collections.emptyMap());

    // Execute
    ServiceAccount result = resolver.get(mockEnv).get();

    // Verify - should return null for not found
    assertNull(result);
  }

  @Test
  public void testGetServiceAccountNotAServiceAccount() throws Exception {
    // Setup
    QueryContext mockContext = TestUtils.getMockAllowContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument("urn")).thenReturn("urn:li:corpuser:regular-user");

    // Return a regular user (not a service account)
    EntityResponse entityResponse = createMockServiceAccountResponse(false, true);
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
  }

  @Test
  public void testGetServiceAccountUnauthorized() throws Exception {
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
  }

  @Test
  public void testGetServiceAccountWithoutCorpUserInfo() throws Exception {
    // Setup
    QueryContext mockContext = TestUtils.getMockAllowContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument("urn")).thenReturn(TEST_SERVICE_ACCOUNT_URN);

    EntityResponse entityResponse = createMockServiceAccountResponse(true, false);
    Map<Urn, EntityResponse> responseMap = new HashMap<>();
    responseMap.put(Urn.createFromString(TEST_SERVICE_ACCOUNT_URN), entityResponse);

    when(mockClient.batchGetV2(any(), eq(Constants.CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(responseMap);

    // Execute
    ServiceAccount result = resolver.get(mockEnv).get();

    // Verify - should work, just without optional fields
    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_SERVICE_ACCOUNT_URN);
    assertEquals(result.getName(), TEST_SERVICE_ACCOUNT_NAME);
    assertNull(result.getDisplayName());
    assertNull(result.getDescription());
  }

  private EntityResponse createMockServiceAccountResponse(
      boolean isServiceAccount, boolean includeCorpUserInfo) throws Exception {
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

    if (includeCorpUserInfo) {
      CorpUserInfo info = new CorpUserInfo();
      info.setActive(true);
      info.setDisplayName("Ingestion Pipeline");
      info.setTitle("Test description");
      EnvelopedAspect infoAspect = new EnvelopedAspect();
      infoAspect.setValue(new Aspect(info.data()));
      aspectMap.put(Constants.CORP_USER_INFO_ASPECT_NAME, infoAspect);
    }

    when(response.getAspects()).thenReturn(aspectMap);
    return response;
  }
}

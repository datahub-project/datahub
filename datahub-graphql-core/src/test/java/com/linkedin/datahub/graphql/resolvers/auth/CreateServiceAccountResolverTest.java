package com.linkedin.datahub.graphql.resolvers.auth;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateServiceAccountInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ServiceAccount;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CreateServiceAccountResolverTest {

  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";
  private static final String TEST_DISPLAY_NAME = "Ingestion Pipeline";
  private static final String TEST_DESCRIPTION = "Service account for data ingestion";

  private EntityClient mockClient;
  private DataFetchingEnvironment mockEnv;
  private CreateServiceAccountResolver resolver;

  @BeforeMethod
  public void setup() {
    mockClient = mock(EntityClient.class);
    mockEnv = mock(DataFetchingEnvironment.class);
    resolver = new CreateServiceAccountResolver(mockClient);
  }

  @Test
  public void testCreateServiceAccountSuccess() throws Exception {
    // Setup
    QueryContext mockContext = TestUtils.getMockAllowContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);

    CreateServiceAccountInput input = new CreateServiceAccountInput();
    input.setDisplayName(TEST_DISPLAY_NAME);
    input.setDescription(TEST_DESCRIPTION);
    when(mockEnv.getArgument("input")).thenReturn(input);

    // Execute
    ServiceAccount result = resolver.get(mockEnv).get();

    // Verify
    assertNotNull(result);
    // Name should be auto-generated with service_ prefix and UUID
    assertTrue(result.getName().startsWith(ServiceAccountUtils.SERVICE_ACCOUNT_PREFIX));
    assertEquals(result.getDisplayName(), TEST_DISPLAY_NAME);
    assertEquals(result.getDescription(), TEST_DESCRIPTION);
    assertEquals(result.getType(), EntityType.CORP_USER);
    assertEquals(result.getCreatedBy(), TEST_ACTOR_URN);
    assertTrue(result.getUrn().contains(ServiceAccountUtils.SERVICE_ACCOUNT_PREFIX));

    // Verify batch ingest was called
    verify(mockClient).batchIngestProposals(any(), anyList(), eq(false));
  }

  @Test
  public void testCreateServiceAccountWithNoOptionalFields() throws Exception {
    // Setup
    QueryContext mockContext = TestUtils.getMockAllowContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);

    CreateServiceAccountInput input = new CreateServiceAccountInput();
    // No display name or description - all fields are optional now
    when(mockEnv.getArgument("input")).thenReturn(input);

    // Execute
    ServiceAccount result = resolver.get(mockEnv).get();

    // Verify - name should be auto-generated UUID
    assertNotNull(result);
    assertTrue(result.getName().startsWith(ServiceAccountUtils.SERVICE_ACCOUNT_PREFIX));
    assertNull(result.getDisplayName());
    assertNull(result.getDescription());

    // Verify batch ingest was called
    verify(mockClient).batchIngestProposals(any(), anyList(), eq(false));
  }

  @Test
  public void testCreateServiceAccountUnauthorized() throws Exception {
    // Setup
    QueryContext mockContext = TestUtils.getMockDenyContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);

    CreateServiceAccountInput input = new CreateServiceAccountInput();
    when(mockEnv.getArgument("input")).thenReturn(input);

    // Execute & Verify
    try {
      resolver.get(mockEnv).get();
      fail("Expected ExecutionException");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof AuthorizationException);
    }

    // Verify no ingestion happened
    verify(mockClient, never()).batchIngestProposals(any(), anyList(), anyBoolean());
  }
}

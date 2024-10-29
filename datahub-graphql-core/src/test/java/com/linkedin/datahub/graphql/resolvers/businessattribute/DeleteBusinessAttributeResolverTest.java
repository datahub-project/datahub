package com.linkedin.datahub.graphql.resolvers.businessattribute;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DeleteBusinessAttributeResolverTest {
  private static final String TEST_BUSINESS_ATTRIBUTE_URN =
      "urn:li:businessAttribute:7d0c4283-de02-4043-aaf2-698b04274658";
  private EntityClient mockClient;
  private QueryContext mockContext;
  private DataFetchingEnvironment mockEnv;
  private Authentication mockAuthentication;

  private void init() {
    mockClient = Mockito.mock(EntityClient.class);
    mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    mockAuthentication = Mockito.mock(Authentication.class);
  }

  private void setupAllowContext() {
    mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(mockAuthentication);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
  }

  private void setupDenyContext() {
    mockContext = getMockDenyContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(mockAuthentication);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
  }

  @Test
  public void testSuccess() throws Exception {
    init();
    setupAllowContext();
    Mockito.when(mockEnv.getArgument("urn")).thenReturn(TEST_BUSINESS_ATTRIBUTE_URN);
    Mockito.when(
            mockClient.exists(
                any(OperationContext.class), eq(Urn.createFromString(TEST_BUSINESS_ATTRIBUTE_URN))))
        .thenReturn(true);

    DeleteBusinessAttributeResolver resolver = new DeleteBusinessAttributeResolver(mockClient);
    resolver.get(mockEnv).get();

    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(
            any(OperationContext.class),
            Mockito.eq(Urn.createFromString(TEST_BUSINESS_ATTRIBUTE_URN)));
  }

  @Test
  public void testUnauthorized() throws Exception {
    init();
    setupDenyContext();
    Mockito.when(mockEnv.getArgument("urn")).thenReturn(TEST_BUSINESS_ATTRIBUTE_URN);

    DeleteBusinessAttributeResolver resolver = new DeleteBusinessAttributeResolver(mockClient);
    AuthorizationException actualException =
        expectThrows(AuthorizationException.class, () -> resolver.get(mockEnv).get());
    assertTrue(
        actualException
            .getMessage()
            .equals(
                "Unauthorized to perform this action. Please contact your DataHub administrator."));

    Mockito.verify(mockClient, Mockito.times(0))
        .deleteEntity(
            any(OperationContext.class),
            Mockito.eq(Urn.createFromString(TEST_BUSINESS_ATTRIBUTE_URN)));
  }

  @Test
  public void testEntityNotExists() throws Exception {
    init();
    setupAllowContext();
    Mockito.when(mockEnv.getArgument("urn")).thenReturn(TEST_BUSINESS_ATTRIBUTE_URN);
    Mockito.when(
            mockClient.exists(
                any(OperationContext.class), eq(Urn.createFromString(TEST_BUSINESS_ATTRIBUTE_URN))))
        .thenReturn(false);

    DeleteBusinessAttributeResolver resolver = new DeleteBusinessAttributeResolver(mockClient);
    RuntimeException actualException =
        expectThrows(RuntimeException.class, () -> resolver.get(mockEnv).get());
    assertTrue(
        actualException
            .getMessage()
            .equals(String.format("This urn does not exist: %s", TEST_BUSINESS_ATTRIBUTE_URN)));

    Mockito.verify(mockClient, Mockito.times(0))
        .deleteEntity(
            any(OperationContext.class),
            Mockito.eq(Urn.createFromString(TEST_BUSINESS_ATTRIBUTE_URN)));
  }
}

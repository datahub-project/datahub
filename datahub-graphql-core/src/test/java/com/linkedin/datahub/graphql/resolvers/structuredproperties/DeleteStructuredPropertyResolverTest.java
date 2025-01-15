package com.linkedin.datahub.graphql.resolvers.structuredproperties;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DeleteStructuredPropertyInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DeleteStructuredPropertyResolverTest {
  private static final String TEST_PROP_URN = "urn:li:structuredProperty:test";

  private static final DeleteStructuredPropertyInput TEST_INPUT =
      new DeleteStructuredPropertyInput(TEST_PROP_URN);

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient(true);
    DeleteStructuredPropertyResolver resolver =
        new DeleteStructuredPropertyResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Boolean success = resolver.get(mockEnv).get();
    assertTrue(success);

    // Validate that we called delete
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .deleteEntity(any(), Mockito.eq(UrnUtils.getUrn(TEST_PROP_URN)));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient(true);
    DeleteStructuredPropertyResolver resolver =
        new DeleteStructuredPropertyResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that we did NOT call delete
    Mockito.verify(mockEntityClient, Mockito.times(0))
        .deleteEntity(any(), Mockito.eq(UrnUtils.getUrn(TEST_PROP_URN)));
  }

  @Test
  public void testGetFailure() throws Exception {
    EntityClient mockEntityClient = initMockEntityClient(false);
    DeleteStructuredPropertyResolver resolver =
        new DeleteStructuredPropertyResolver(mockEntityClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that deleteEntity was called, but since it's the thing that failed it was called
    // once still
    Mockito.verify(mockEntityClient, Mockito.times(1))
        .deleteEntity(any(), Mockito.eq(UrnUtils.getUrn(TEST_PROP_URN)));
  }

  private EntityClient initMockEntityClient(boolean shouldSucceed) throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    if (!shouldSucceed) {
      Mockito.doThrow(new RemoteInvocationException()).when(client).deleteEntity(any(), any());
    }
    return client;
  }
}

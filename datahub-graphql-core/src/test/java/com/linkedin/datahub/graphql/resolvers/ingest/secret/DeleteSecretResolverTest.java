package com.linkedin.datahub.graphql.resolvers.ingest.secret;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DeleteSecretResolverTest {

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DeleteSecretResolver resolver = new DeleteSecretResolver(mockClient);

    // execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_SECRET_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertEquals(resolver.get(mockEnv).get(), TEST_SECRET_URN.toString());
    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(TEST_SECRET_URN, mockContext.getAuthentication());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DeleteSecretResolver resolver = new DeleteSecretResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_SECRET_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .deleteEntity(TEST_SECRET_URN, mockContext.getAuthentication());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .deleteEntity(Mockito.eq(TEST_SECRET_URN), Mockito.any(Authentication.class));
    DeleteSecretResolver resolver = new DeleteSecretResolver(mockClient);

    // Execute Resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_SECRET_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }
}

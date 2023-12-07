package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.RollbackIngestionInput;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class RollbackIngestionResolverTest {

  private static final String RUN_ID = "testRunId";
  private static final RollbackIngestionInput TEST_INPUT = new RollbackIngestionInput(RUN_ID);

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    RollbackIngestionResolver resolver = new RollbackIngestionResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Boolean result = resolver.get(mockEnv).get();
    assertTrue(result);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    RollbackIngestionResolver resolver = new RollbackIngestionResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .rollbackIngestion(Mockito.eq(RUN_ID), Mockito.any(Authentication.class));
  }

  @Test
  public void testRollbackIngestionMethod() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    RollbackIngestionResolver resolver = new RollbackIngestionResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    resolver.rollbackIngestion(RUN_ID, mockContext).get();

    Mockito.verify(mockClient, Mockito.times(1))
        .rollbackIngestion(Mockito.eq(RUN_ID), Mockito.any(Authentication.class));
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RuntimeException.class)
        .when(mockClient)
        .rollbackIngestion(Mockito.any(), Mockito.any(Authentication.class));

    RollbackIngestionResolver resolver = new RollbackIngestionResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();

    assertThrows(
        RuntimeException.class, () -> resolver.rollbackIngestion(RUN_ID, mockContext).join());
  }
}

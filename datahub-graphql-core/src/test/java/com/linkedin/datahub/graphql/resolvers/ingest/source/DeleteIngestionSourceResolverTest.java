package com.linkedin.datahub.graphql.resolvers.ingest.source;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DeleteIngestionSourceResolverTest {

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DeleteIngestionSourceResolver resolver = new DeleteIngestionSourceResolver(mockClient);

    // execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn")))
        .thenReturn(TEST_INGESTION_SOURCE_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertEquals(resolver.get(mockEnv).get(), TEST_INGESTION_SOURCE_URN.toString());
    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(mockContext.getOperationContext(), TEST_INGESTION_SOURCE_URN);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DeleteIngestionSourceResolver resolver = new DeleteIngestionSourceResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn")))
        .thenReturn(TEST_INGESTION_SOURCE_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).deleteEntity(any(), eq(TEST_INGESTION_SOURCE_URN));
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .deleteEntity(any(), Mockito.eq(TEST_INGESTION_SOURCE_URN));

    // Execute Resolver
    QueryContext mockContext = getMockAllowContext();
    DeleteIngestionSourceResolver resolver = new DeleteIngestionSourceResolver(mockClient);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn")))
        .thenReturn(TEST_INGESTION_SOURCE_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }
}

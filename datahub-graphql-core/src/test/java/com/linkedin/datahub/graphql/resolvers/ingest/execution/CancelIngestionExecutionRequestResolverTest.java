package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CancelIngestionExecutionRequestInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CancelIngestionExecutionRequestResolverTest {

  private static final CancelIngestionExecutionRequestInput TEST_INPUT =
      new CancelIngestionExecutionRequestInput(
          TEST_INGESTION_SOURCE_URN.toString(), TEST_EXECUTION_REQUEST_URN.toString());

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(TEST_INGESTION_SOURCE_URN))),
                Mockito.eq(ImmutableSet.of(Constants.INGESTION_INFO_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                TEST_INGESTION_SOURCE_URN,
                new EntityResponse()
                    .setEntityName(Constants.INGESTION_SOURCE_ENTITY_NAME)
                    .setUrn(TEST_INGESTION_SOURCE_URN)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.INGESTION_INFO_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(getTestIngestionSourceInfo().data())))))));
    CancelIngestionExecutionRequestResolver resolver =
        new CancelIngestionExecutionRequestResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    // Verify ingest proposal has been called to create a Signal request.
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(any(), Mockito.any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    CancelIngestionExecutionRequestResolver resolver =
        new CancelIngestionExecutionRequestResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any(), anyBoolean());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .ingestProposal(any(), Mockito.any(), anyBoolean());
    CancelIngestionExecutionRequestResolver resolver =
        new CancelIngestionExecutionRequestResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }
}

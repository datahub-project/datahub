package com.linkedin.datahub.graphql.resolvers.operation;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.linkedin.common.Operation;
import com.linkedin.common.OperationSourceType;
import com.linkedin.common.OperationType;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ReportOperationInput;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ReportOperationResolverTest {

  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Operation expectedOperation =
        new Operation()
            .setTimestampMillis(0L)
            .setLastUpdatedTimestamp(0L)
            .setOperationType(OperationType.INSERT)
            .setSourceType(OperationSourceType.DATA_PLATFORM)
            .setActor(UrnUtils.getUrn("urn:li:corpuser:test"))
            .setCustomOperationType(null, SetMode.IGNORE_NULL)
            .setNumAffectedRows(1L);

    MetadataChangeProposal expectedProposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            UrnUtils.getUrn(TEST_ENTITY_URN), OPERATION_ASPECT_NAME, expectedOperation);

    // Test setting the domain
    Mockito.when(mockClient.ingestProposal(any(), Mockito.eq(expectedProposal)))
        .thenReturn(TEST_ENTITY_URN);

    ReportOperationResolver resolver = new ReportOperationResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(getTestInput());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).get();

    verifyIngestProposal(mockClient, 1, expectedProposal);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    ReportOperationResolver resolver = new ReportOperationResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(getTestInput());
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any());
  }

  private ReportOperationInput getTestInput() {
    ReportOperationInput input = new ReportOperationInput();
    input.setUrn(TEST_ENTITY_URN);
    input.setOperationType(com.linkedin.datahub.graphql.generated.OperationType.INSERT);
    input.setNumAffectedRows(1L);
    input.setTimestampMillis(0L);
    input.setSourceType(com.linkedin.datahub.graphql.generated.OperationSourceType.DATA_PLATFORM);
    return input;
  }
}

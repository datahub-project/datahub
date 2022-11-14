package com.linkedin.datahub.graphql.resolvers.operation;

import com.datahub.authentication.Authentication;
import com.linkedin.common.Operation;
import com.linkedin.common.OperationSourceType;
import com.linkedin.common.OperationType;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ReportOperationInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;


public class ReportOperationResolverTest {

  private static final String TEST_ENTITY_URN = "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Operation expectedOperation = new Operation()
        .setTimestampMillis(0L)
        .setLastUpdatedTimestamp(0L)
        .setOperationType(OperationType.INSERT)
        .setSourceType(OperationSourceType.DATA_PLATFORM)
        .setActor(UrnUtils.getUrn("urn:li:corpuser:test"))
        .setCustomOperationType(null, SetMode.IGNORE_NULL)
        .setNumAffectedRows(1L);

    MetadataChangeProposal expectedProposal = new MetadataChangeProposal()
        .setAspectName(Constants.OPERATION_ASPECT_NAME)
        .setChangeType(ChangeType.UPSERT)
        .setEntityUrn(UrnUtils.getUrn(TEST_ENTITY_URN))
        .setEntityType(Constants.DATASET_ENTITY_NAME)
        .setAspect(GenericRecordUtils.serializeAspect(expectedOperation));

    // Test setting the domain
    Mockito.when(mockClient.ingestProposal(
        Mockito.eq(expectedProposal),
        Mockito.any(Authentication.class)))
      .thenReturn(TEST_ENTITY_URN);

    ReportOperationResolver resolver = new ReportOperationResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(getTestInput());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).get();

    Mockito.verify(mockClient, Mockito.times(1)).ingestProposal(
        Mockito.eq(expectedProposal),
        Mockito.any(Authentication.class)
    );
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
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(
        Mockito.any(),
        Mockito.any(Authentication.class));
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
package com.linkedin.datahub.graphql.resolvers.logical;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.testng.Assert.*;

import com.linkedin.common.Edge;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.SetLogicalParentInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.ActorContext;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SetLogicalParentResolverTest {

  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_PARENT_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-parent,PROD)";
  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  @Test
  public void testGetSuccessSetParent() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SetLogicalParentResolver resolver = new SetLogicalParentResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    mockActor(mockContext);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    SetLogicalParentInput input = new SetLogicalParentInput();
    input.setResourceUrn(TEST_ENTITY_URN);
    input.setParentUrn(TEST_PARENT_URN);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
  }

  @Test
  public void testGetSuccessRemoveParent() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SetLogicalParentResolver resolver = new SetLogicalParentResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    mockActor(mockContext);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    SetLogicalParentInput input = new SetLogicalParentInput();
    input.setResourceUrn(TEST_ENTITY_URN);
    input.setParentUrn(null);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());

    SetLogicalParentResolver resolver = new SetLogicalParentResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    mockActor(mockContext);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    SetLogicalParentInput input = new SetLogicalParentInput();
    input.setResourceUrn(TEST_ENTITY_URN);
    input.setParentUrn(TEST_PARENT_URN);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testCreateLogicalParentWithParent() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SetLogicalParentResolver resolver = new SetLogicalParentResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    mockActor(mockContext);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    SetLogicalParentInput input = new SetLogicalParentInput();
    input.setResourceUrn(TEST_ENTITY_URN);
    input.setParentUrn(TEST_PARENT_URN);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(),
            Mockito.argThat(
                proposal -> {
                  try {
                    LogicalParent logicalParent =
                        GenericRecordUtils.deserializeAspect(
                            proposal.getAspect().getValue(),
                            proposal.getAspect().getContentType(),
                            LogicalParent.class);
                    if (logicalParent.getParent() == null) {
                      return false;
                    }
                    Edge edge = logicalParent.getParent();
                    return edge.getDestinationUrn().toString().equals(TEST_PARENT_URN)
                        && edge.getCreated() != null
                        && edge.getLastModified() != null
                        && edge.getCreated().getActor().toString().equals(TEST_ACTOR_URN.toString())
                        && edge.getLastModified()
                            .getActor()
                            .toString()
                            .equals(TEST_ACTOR_URN.toString());
                  } catch (Exception e) {
                    return false;
                  }
                }),
            anyBoolean());
  }

  @Test
  public void testCreateLogicalParentWithoutParent() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    SetLogicalParentResolver resolver = new SetLogicalParentResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    mockActor(mockContext);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    SetLogicalParentInput input = new SetLogicalParentInput();
    input.setResourceUrn(TEST_ENTITY_URN);
    input.setParentUrn(null);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(),
            Mockito.argThat(
                proposal -> {
                  try {
                    LogicalParent logicalParent =
                        GenericRecordUtils.deserializeAspect(
                            proposal.getAspect().getValue(),
                            proposal.getAspect().getContentType(),
                            LogicalParent.class);
                    return logicalParent.getParent() == null;
                  } catch (Exception e) {
                    return false;
                  }
                }),
            anyBoolean());
  }

  private void mockActor(QueryContext mockContext) {
    OperationContext mockOperationContext = Mockito.mock(OperationContext.class);
    ActorContext mockActorContext = Mockito.mock(ActorContext.class);
    Mockito.when(mockContext.getOperationContext()).thenReturn(mockOperationContext);
    Mockito.when(mockOperationContext.getActorContext()).thenReturn(mockActorContext);
    Mockito.when(mockActorContext.getActorUrn()).thenReturn(TEST_ACTOR_URN);
  }
}

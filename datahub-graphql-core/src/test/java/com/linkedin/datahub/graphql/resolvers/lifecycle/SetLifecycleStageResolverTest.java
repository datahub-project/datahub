package com.linkedin.datahub.graphql.resolvers.lifecycle;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class SetLifecycleStageResolverTest {

  private static final String ENTITY_URN = "urn:li:dataset:(urn:li:dataPlatform:hdfs,mydata,PROD)";
  private static final String STAGE_URN = "urn:li:lifecycleStageType:DRAFT";
  private static final String ACTOR_URN = "urn:li:corpuser:testUser";

  @Test
  public void testSetLifecycleStage() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));
    when(mockContext.getActorUrn()).thenReturn(ACTOR_URN);

    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument("urn")).thenReturn(ENTITY_URN);
    when(mockEnv.getArgument("lifecycleStageUrn")).thenReturn(STAGE_URN);

    // Existing Status with removed=true to verify we preserve it
    Status existingStatus = new Status();
    existingStatus.setRemoved(true);
    when(mockClient.getLatestAspectObject(
            any(), eq(UrnUtils.getUrn(ENTITY_URN)), eq(STATUS_ASPECT_NAME), eq(false)))
        .thenReturn(new Aspect(existingStatus.data()));

    SetLifecycleStageResolver resolver = new SetLifecycleStageResolver(mockClient);
    boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockClient).ingestProposal(any(), mcpCaptor.capture(), eq(false));

    MetadataChangeProposal mcp = mcpCaptor.getValue();
    Status written = deserializeStatus(mcp);
    assertTrue(written.isRemoved(), "Should preserve existing removed=true");
    assertTrue(written.hasLifecycleStage());
    assertTrue(written.getLifecycleStage().toString().contains("DRAFT"));
    assertNotNull(written.getLifecycleLastUpdated(), "Should set attribution");
    assertEquals(written.getLifecycleLastUpdated().getActor().toString(), ACTOR_URN);
  }

  @Test
  public void testClearLifecycleStage() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));
    when(mockContext.getActorUrn()).thenReturn(ACTOR_URN);

    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument("urn")).thenReturn(ENTITY_URN);
    when(mockEnv.getArgument("lifecycleStageUrn")).thenReturn(null);

    Status existingStatus = new Status();
    existingStatus.setRemoved(false);
    existingStatus.setLifecycleStage(UrnUtils.getUrn(STAGE_URN));
    when(mockClient.getLatestAspectObject(
            any(), eq(UrnUtils.getUrn(ENTITY_URN)), eq(STATUS_ASPECT_NAME), eq(false)))
        .thenReturn(new Aspect(existingStatus.data()));

    SetLifecycleStageResolver resolver = new SetLifecycleStageResolver(mockClient);
    boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockClient).ingestProposal(any(), mcpCaptor.capture(), eq(false));

    MetadataChangeProposal mcp = mcpCaptor.getValue();
    Status written = deserializeStatus(mcp);
    assertFalse(written.hasLifecycleStage(), "lifecycleStage should be cleared");
    assertNotNull(written.getLifecycleLastUpdated(), "Should set attribution even when clearing");
    assertEquals(written.getLifecycleLastUpdated().getActor().toString(), ACTOR_URN);
  }

  @Test
  public void testSetLifecycleStageNoExistingStatus() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));
    when(mockContext.getActorUrn()).thenReturn(ACTOR_URN);

    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument("urn")).thenReturn(ENTITY_URN);
    when(mockEnv.getArgument("lifecycleStageUrn")).thenReturn(STAGE_URN);

    when(mockClient.getLatestAspectObject(any(), any(Urn.class), eq(STATUS_ASPECT_NAME), eq(false)))
        .thenReturn(null);

    SetLifecycleStageResolver resolver = new SetLifecycleStageResolver(mockClient);
    boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockClient).ingestProposal(any(), mcpCaptor.capture(), eq(false));

    MetadataChangeProposal mcp = mcpCaptor.getValue();
    Status written = deserializeStatus(mcp);
    assertFalse(written.isRemoved(), "New status should default to removed=false");
    assertTrue(written.hasLifecycleStage());
    assertNotNull(written.getLifecycleLastUpdated(), "Should set attribution");
    assertEquals(written.getLifecycleLastUpdated().getActor().toString(), ACTOR_URN);
  }

  @Test
  public void testClearLifecycleStageWithBlankString() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));
    when(mockContext.getActorUrn()).thenReturn(ACTOR_URN);

    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument("urn")).thenReturn(ENTITY_URN);
    when(mockEnv.getArgument("lifecycleStageUrn")).thenReturn("   ");

    Status existingStatus = new Status();
    existingStatus.setLifecycleStage(UrnUtils.getUrn(STAGE_URN));
    when(mockClient.getLatestAspectObject(
            any(), eq(UrnUtils.getUrn(ENTITY_URN)), eq(STATUS_ASPECT_NAME), eq(false)))
        .thenReturn(new Aspect(existingStatus.data()));

    SetLifecycleStageResolver resolver = new SetLifecycleStageResolver(mockClient);
    boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockClient).ingestProposal(any(), mcpCaptor.capture(), eq(false));

    Status written = deserializeStatus(mcpCaptor.getValue());
    assertFalse(written.hasLifecycleStage(), "Blank string should clear lifecycleStage");
    assertNotNull(written.getLifecycleLastUpdated());
  }

  private static Status deserializeStatus(MetadataChangeProposal mcp) {
    return GenericRecordUtils.deserializeAspect(
        mcp.getAspect().getValue(), mcp.getAspect().getContentType(), Status.class);
  }
}

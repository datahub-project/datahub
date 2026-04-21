package com.linkedin.datahub.graphql.resolvers.lifecycle;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.LifecycleStageType;
import com.linkedin.datahub.graphql.generated.Status;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import org.testng.annotations.Test;

public class StatusLifecycleStageResolverTest {

  private static final String STAGE_URN = "urn:li:lifecycleStageType:DRAFT";

  @Test
  public void testReturnsNullWhenNoLifecycleStage() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);

    Status status = new Status();
    status.setRemoved(false);
    when(mockEnv.getSource()).thenReturn(status);

    StatusLifecycleStageResolver resolver = new StatusLifecycleStageResolver(mockClient);
    LifecycleStageType result = resolver.get(mockEnv).get();

    assertNull(result);
    verify(mockClient, never()).getV2(any(), any(), any(Urn.class), any());
  }

  @Test
  public void testResolvesStageDetails() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(mockContext);

    LifecycleStageType skeleton = new LifecycleStageType();
    skeleton.setUrn(STAGE_URN);

    Status status = new Status();
    status.setRemoved(false);
    status.setLifecycleStage(skeleton);
    when(mockEnv.getSource()).thenReturn(status);

    EntityResponse response =
        LifecycleStageTestUtils.makeStageResponse(UrnUtils.getUrn(STAGE_URN), "Draft", true);
    when(mockClient.getV2(any(), eq("lifecycleStageType"), eq(UrnUtils.getUrn(STAGE_URN)), any()))
        .thenReturn(response);

    StatusLifecycleStageResolver resolver = new StatusLifecycleStageResolver(mockClient);
    LifecycleStageType result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.getUrn(), STAGE_URN);
    assertEquals(result.getName(), "Draft");
    assertTrue(result.getHideInSearch());
  }

  @Test
  public void testReturnsNullWhenEntityNotFound() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(mockContext);

    LifecycleStageType skeleton = new LifecycleStageType();
    skeleton.setUrn(STAGE_URN);

    Status status = new Status();
    status.setLifecycleStage(skeleton);
    when(mockEnv.getSource()).thenReturn(status);

    when(mockClient.getV2(any(), any(), any(Urn.class), any())).thenReturn(null);

    StatusLifecycleStageResolver resolver = new StatusLifecycleStageResolver(mockClient);
    LifecycleStageType result = resolver.get(mockEnv).get();

    assertNull(result);
  }

  @Test
  public void testReturnsNullWhenSkeletonUrnIsBlank() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);

    LifecycleStageType skeleton = new LifecycleStageType();
    skeleton.setUrn("   ");

    Status status = new Status();
    status.setLifecycleStage(skeleton);
    when(mockEnv.getSource()).thenReturn(status);

    StatusLifecycleStageResolver resolver = new StatusLifecycleStageResolver(mockClient);
    LifecycleStageType result = resolver.get(mockEnv).get();

    assertNull(result);
    verify(mockClient, never()).getV2(any(), any(), any(Urn.class), any());
  }

  @Test
  public void testReturnsNullWhenResponseMissingInfoAspect() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(mockContext);

    LifecycleStageType skeleton = new LifecycleStageType();
    skeleton.setUrn(STAGE_URN);

    Status status = new Status();
    status.setLifecycleStage(skeleton);
    when(mockEnv.getSource()).thenReturn(status);

    EntityResponse emptyResponse = new EntityResponse();
    emptyResponse.setUrn(UrnUtils.getUrn(STAGE_URN));
    emptyResponse.setEntityName("lifecycleStageType");
    emptyResponse.setAspects(new EnvelopedAspectMap());

    when(mockClient.getV2(any(), eq("lifecycleStageType"), eq(UrnUtils.getUrn(STAGE_URN)), any()))
        .thenReturn(emptyResponse);

    StatusLifecycleStageResolver resolver = new StatusLifecycleStageResolver(mockClient);
    LifecycleStageType result = resolver.get(mockEnv).get();

    assertNull(result);
  }
}

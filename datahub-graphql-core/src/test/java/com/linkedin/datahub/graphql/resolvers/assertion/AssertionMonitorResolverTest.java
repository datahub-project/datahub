package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.resolvers.assertion.AssertionMonitorResolver.ASSERTION_MONITOR_RELATIONSHIP_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.datahub.graphql.types.monitor.MonitorType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class AssertionMonitorResolverTest {

  private final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test-guid");
  private final Urn TEST_MONITOR_URN = UrnUtils.getUrn("urn:li:monitor:test-guid");

  @Test
  public void testGetSuccess() throws Exception {
    // Mock dependencies
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    MonitorType mockMonitorType = Mockito.mock(MonitorType.class);

    // Mock GraphClient response
    Mockito.when(
            mockGraphClient.getRelatedEntities(
                eq(TEST_ASSERTION_URN.toString()),
                eq(ImmutableSet.of(ASSERTION_MONITOR_RELATIONSHIP_NAME)),
                eq(RelationshipDirection.INCOMING),
                eq(0),
                eq(1),
                any()))
        .thenReturn(
            new EntityRelationships()
                .setStart(0)
                .setCount(1)
                .setTotal(1)
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(
                            new EntityRelationship()
                                .setEntity(TEST_MONITOR_URN)
                                .setType(ASSERTION_MONITOR_RELATIONSHIP_NAME)))));

    // Mock entity existence check
    Mockito.when(mockClient.exists(any(), eq(TEST_MONITOR_URN), eq(false))).thenReturn(true);

    // Mock Monitor response from MonitorType
    Monitor mockMonitor = new Monitor();
    mockMonitor.setUrn(TEST_MONITOR_URN.toString());
    mockMonitor.setType(EntityType.MONITOR);

    // Mock the MonitorType batchLoad method
    DataFetcherResult<Monitor> loadResult =
        new DataFetcherResult<>(mockMonitor, Collections.emptyList());
    Mockito.when(mockMonitorType.batchLoad(Mockito.eq(List.of(TEST_MONITOR_URN.toString())), any()))
        .thenReturn(List.of(loadResult));

    // Create resolver with the mocks
    AssertionMonitorResolver resolver =
        new AssertionMonitorResolver(mockClient, mockGraphClient, mockMonitorType);

    // Set up environment for the test
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    Assertion parentAssertion = new Assertion();
    parentAssertion.setUrn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentAssertion);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Execute resolver
    Monitor monitor = resolver.get(mockEnv).get();

    // Verify results
    assertEquals(monitor.getUrn(), TEST_MONITOR_URN.toString());
    assertEquals(monitor.getType(), EntityType.MONITOR);

    // Verify the right methods were called
    Mockito.verify(mockMonitorType)
        .batchLoad(Mockito.eq(List.of(TEST_MONITOR_URN.toString())), any());
  }

  @Test
  public void testGetSuccessMultipleMonitors() throws Exception {
    // THIS CASE SHOULD NOT HAPPEN UNDER NORMAL OPERATION.
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    MonitorType mockMonitorType = Mockito.mock(MonitorType.class);

    // Mock GraphClient response with multiple relationships
    Mockito.when(
            mockGraphClient.getRelatedEntities(
                eq(TEST_ASSERTION_URN.toString()),
                eq(ImmutableSet.of(ASSERTION_MONITOR_RELATIONSHIP_NAME)),
                eq(RelationshipDirection.INCOMING),
                eq(0),
                eq(1),
                any()))
        .thenReturn(
            new EntityRelationships()
                .setStart(0)
                .setCount(2)
                .setTotal(2)
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(
                            new EntityRelationship()
                                .setEntity(TEST_MONITOR_URN)
                                .setType(ASSERTION_MONITOR_RELATIONSHIP_NAME),
                            new EntityRelationship()
                                .setEntity(UrnUtils.getUrn("urn:li:monitor:test2"))
                                .setType(ASSERTION_MONITOR_RELATIONSHIP_NAME)))));

    // Mock entity existence check
    Mockito.when(mockClient.exists(any(), eq(TEST_MONITOR_URN), eq(false))).thenReturn(true);

    // Mock Monitor response
    Monitor mockMonitor = new Monitor();
    mockMonitor.setUrn(TEST_MONITOR_URN.toString());
    mockMonitor.setType(EntityType.MONITOR);

    // Mock the MonitorType batchLoad method
    DataFetcherResult<Monitor> loadResult =
        new DataFetcherResult<>(mockMonitor, Collections.emptyList());
    Mockito.when(mockMonitorType.batchLoad(Mockito.eq(List.of(TEST_MONITOR_URN.toString())), any()))
        .thenReturn(List.of(loadResult));

    // Create resolver
    AssertionMonitorResolver resolver =
        new AssertionMonitorResolver(mockClient, mockGraphClient, mockMonitorType);

    // Set up environment
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    Assertion parentAssertion = new Assertion();
    parentAssertion.setUrn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentAssertion);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Execute resolver
    Monitor monitor = resolver.get(mockEnv).get();

    // Verify results
    assertEquals(monitor.getUrn(), TEST_MONITOR_URN.toString());
    assertEquals(monitor.getType(), EntityType.MONITOR);

    // Verify the right methods were called
    Mockito.verify(mockMonitorType)
        .batchLoad(Mockito.eq(List.of(TEST_MONITOR_URN.toString())), any());
  }

  @Test
  public void testGetSuccessNoRelationships() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);
    MonitorType mockMonitorType = Mockito.mock(MonitorType.class);

    // Mock GraphClient response with no relationships
    Mockito.when(
            mockGraphClient.getRelatedEntities(
                eq(TEST_ASSERTION_URN.toString()),
                eq(ImmutableSet.of(ASSERTION_MONITOR_RELATIONSHIP_NAME)),
                eq(RelationshipDirection.INCOMING),
                eq(0),
                eq(1),
                any()))
        .thenReturn(
            new EntityRelationships()
                .setStart(0)
                .setCount(0)
                .setTotal(0)
                .setRelationships(new EntityRelationshipArray(Collections.emptyList())));

    // Create resolver
    AssertionMonitorResolver resolver =
        new AssertionMonitorResolver(mockClient, mockGraphClient, mockMonitorType);

    // Set up environment
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    Assertion parentAssertion = new Assertion();
    parentAssertion.setUrn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentAssertion);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Execute resolver
    Monitor monitor = resolver.get(mockEnv).get();

    // Verify no monitor is returned
    assertNull(monitor);

    // Verify the batchLoad was never called since there were no relationships
    Mockito.verify(mockMonitorType, Mockito.never()).batchLoad(any(), any());
  }
}

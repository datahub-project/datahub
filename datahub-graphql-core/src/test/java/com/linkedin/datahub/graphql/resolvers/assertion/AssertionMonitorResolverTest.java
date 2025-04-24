package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.resolvers.assertion.AssertionMonitorResolver.ASSERTION_MONITOR_RELATIONSHIP_NAME;
import static org.mockito.ArgumentMatchers.any;
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
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class AssertionMonitorResolverTest {

  private final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test-guid");
  private final Urn TEST_MONITOR_URN = UrnUtils.getUrn("urn:li:monitor:test-guid");

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    GraphClient graphClient = Mockito.mock(GraphClient.class);

    Mockito.when(
            graphClient.getRelatedEntities(
                Mockito.eq(TEST_ASSERTION_URN.toString()),
                Mockito.eq(ImmutableSet.of(ASSERTION_MONITOR_RELATIONSHIP_NAME)),
                Mockito.eq(RelationshipDirection.INCOMING),
                Mockito.eq(0),
                Mockito.eq(1),
                Mockito.any()))
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

    MonitorInfo expectedMonitorInfo =
        new MonitorInfo()
            .setType(MonitorType.ASSERTION)
            .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE))
            .setAssertionMonitor(
                new AssertionMonitor()
                    .setAssertions(new AssertionEvaluationSpecArray(Collections.emptyList())));

    Map<String, com.linkedin.entity.EnvelopedAspect> monitorAspects = new HashMap<>();
    monitorAspects.put(
        Constants.MONITOR_INFO_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(expectedMonitorInfo.data())));
    Mockito.when(
            mockClient.getV2(
                any(),
                Mockito.eq(Constants.MONITOR_ENTITY_NAME),
                Mockito.eq(TEST_MONITOR_URN),
                Mockito.eq(null)))
        .thenReturn(
            new EntityResponse()
                .setEntityName(Constants.MONITOR_ENTITY_NAME)
                .setUrn(TEST_MONITOR_URN)
                .setAspects(new EnvelopedAspectMap(monitorAspects)));

    // Monitor Exists
    Mockito.when(mockClient.exists(any(), Mockito.eq(TEST_MONITOR_URN), Mockito.eq(false)))
        .thenReturn(true);

    AssertionMonitorResolver resolver = new AssertionMonitorResolver(mockClient, graphClient);

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    Assertion parentAssertion = new Assertion();
    parentAssertion.setUrn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentAssertion);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Monitor monitor = resolver.get(mockEnv).get();

    assertEquals(monitor.getUrn(), TEST_MONITOR_URN.toString());
    assertEquals(monitor.getType(), EntityType.MONITOR);
    assertEquals(
        monitor.getInfo().getType(), com.linkedin.datahub.graphql.generated.MonitorType.ASSERTION);
  }

  @Test
  public void testGetSuccessMultipleMonitors() throws Exception {
    // THIS CASE SHOULD NOT HAPPEN UNDER NORMAL OPERATION.
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    GraphClient graphClient = Mockito.mock(GraphClient.class);

    Mockito.when(
            graphClient.getRelatedEntities(
                Mockito.eq(TEST_ASSERTION_URN.toString()),
                Mockito.eq(ImmutableSet.of(ASSERTION_MONITOR_RELATIONSHIP_NAME)),
                Mockito.eq(RelationshipDirection.INCOMING),
                Mockito.eq(0),
                Mockito.eq(1),
                Mockito.any()))
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

    MonitorInfo expectedMonitorInfo =
        new MonitorInfo()
            .setType(MonitorType.ASSERTION)
            .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE))
            .setAssertionMonitor(
                new AssertionMonitor()
                    .setAssertions(new AssertionEvaluationSpecArray(Collections.emptyList())));

    Map<String, com.linkedin.entity.EnvelopedAspect> monitorAspects = new HashMap<>();
    monitorAspects.put(
        Constants.MONITOR_INFO_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(expectedMonitorInfo.data())));
    Mockito.when(
            mockClient.getV2(
                any(),
                Mockito.eq(Constants.MONITOR_ENTITY_NAME),
                Mockito.eq(TEST_MONITOR_URN),
                Mockito.eq(null)))
        .thenReturn(
            new EntityResponse()
                .setEntityName(Constants.MONITOR_ENTITY_NAME)
                .setUrn(TEST_MONITOR_URN)
                .setAspects(new EnvelopedAspectMap(monitorAspects)));

    // Monitor Exists
    Mockito.when(mockClient.exists(any(), Mockito.eq(TEST_MONITOR_URN), Mockito.eq(false)))
        .thenReturn(true);

    AssertionMonitorResolver resolver = new AssertionMonitorResolver(mockClient, graphClient);

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    Assertion parentAssertion = new Assertion();
    parentAssertion.setUrn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentAssertion);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Monitor monitor = resolver.get(mockEnv).get();

    assertEquals(monitor.getUrn(), TEST_MONITOR_URN.toString());
    assertEquals(monitor.getType(), EntityType.MONITOR);
    assertEquals(
        monitor.getInfo().getType(), com.linkedin.datahub.graphql.generated.MonitorType.ASSERTION);
  }

  @Test
  public void testGetSuccessNoRelationships() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    GraphClient graphClient = Mockito.mock(GraphClient.class);

    Mockito.when(
            graphClient.getRelatedEntities(
                Mockito.eq(TEST_ASSERTION_URN.toString()),
                Mockito.eq(ImmutableSet.of(ASSERTION_MONITOR_RELATIONSHIP_NAME)),
                Mockito.eq(RelationshipDirection.INCOMING),
                Mockito.eq(0),
                Mockito.eq(1),
                Mockito.any()))
        .thenReturn(
            new EntityRelationships()
                .setStart(0)
                .setCount(0)
                .setTotal(0)
                .setRelationships(new EntityRelationshipArray(Collections.emptyList())));

    AssertionMonitorResolver resolver = new AssertionMonitorResolver(mockClient, graphClient);

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    Assertion parentAssertion = new Assertion();
    parentAssertion.setUrn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentAssertion);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Monitor monitor = resolver.get(mockEnv).get();

    assertNull(monitor);
  }
}

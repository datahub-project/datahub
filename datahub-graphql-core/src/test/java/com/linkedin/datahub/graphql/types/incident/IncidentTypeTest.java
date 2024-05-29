package com.linkedin.datahub.graphql.types.incident;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Incident;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentSource;
import com.linkedin.incident.IncidentSourceType;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.IncidentKey;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class IncidentTypeTest {

  private static final String TEST_INCIDENT_URN = "urn:li:incident:guid-1";
  private static Urn testAssertionUrn;
  private static Urn testUserUrn;
  private static Urn testDatasetUrn;

  static {
    try {
      testAssertionUrn = Urn.createFromString("urn:li:assertion:test");
      testUserUrn = Urn.createFromString("urn:li:corpuser:test");
      testDatasetUrn = Urn.createFromString("urn:li:dataset:(test,test,test)");
    } catch (Exception ignored) {
      // ignored
    }
  }

  private static final IncidentKey TEST_INCIDENT_KEY = new IncidentKey().setId("guid-1");
  private static final IncidentInfo TEST_INCIDENT_INFO =
      new IncidentInfo()
          .setType(IncidentType.OPERATIONAL)
          .setCustomType("Custom Type")
          .setDescription("Description")
          .setPriority(5)
          .setTitle("Title")
          .setEntities(new UrnArray(ImmutableList.of(testDatasetUrn)))
          .setSource(
              new IncidentSource()
                  .setType(IncidentSourceType.MANUAL)
                  .setSourceUrn(testAssertionUrn))
          .setStatus(
              new IncidentStatus()
                  .setState(IncidentState.ACTIVE)
                  .setMessage("Message")
                  .setLastUpdated(new AuditStamp().setTime(1L).setActor(testUserUrn)))
          .setCreated(new AuditStamp().setTime(0L).setActor(testUserUrn));
  private static final String TEST_INCIDENT_URN_2 = "urn:li:incident:guid-2";

  @Test
  public void testBatchLoad() throws Exception {

    EntityClient client = Mockito.mock(EntityClient.class);

    Urn incidentUrn1 = Urn.createFromString(TEST_INCIDENT_URN);
    Urn incidentUrn2 = Urn.createFromString(TEST_INCIDENT_URN_2);

    Map<String, EnvelopedAspect> incident1Aspects = new HashMap<>();
    incident1Aspects.put(
        Constants.INCIDENT_KEY_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_INCIDENT_KEY.data())));
    incident1Aspects.put(
        Constants.INCIDENT_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_INCIDENT_INFO.data())));
    Mockito.when(
            client.batchGetV2(
                any(),
                Mockito.eq(Constants.INCIDENT_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(incidentUrn1, incidentUrn2))),
                Mockito.eq(
                    com.linkedin.datahub.graphql.types.incident.IncidentType.ASPECTS_TO_FETCH)))
        .thenReturn(
            ImmutableMap.of(
                incidentUrn1,
                new EntityResponse()
                    .setEntityName(Constants.INCIDENT_ENTITY_NAME)
                    .setUrn(incidentUrn1)
                    .setAspects(new EnvelopedAspectMap(incident1Aspects))));

    com.linkedin.datahub.graphql.types.incident.IncidentType type =
        new com.linkedin.datahub.graphql.types.incident.IncidentType(client);

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());

    List<DataFetcherResult<Incident>> result =
        type.batchLoad(ImmutableList.of(TEST_INCIDENT_URN, TEST_INCIDENT_URN_2), mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            any(),
            Mockito.eq(Constants.INCIDENT_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(incidentUrn1, incidentUrn2)),
            Mockito.eq(com.linkedin.datahub.graphql.types.incident.IncidentType.ASPECTS_TO_FETCH));

    assertEquals(result.size(), 2);

    Incident incident = result.get(0).getData();
    assertEquals(incident.getUrn(), TEST_INCIDENT_URN.toString());
    assertEquals(incident.getType(), EntityType.INCIDENT);
    assertEquals(incident.getIncidentType().toString(), TEST_INCIDENT_INFO.getType().toString());
    assertEquals(incident.getTitle(), TEST_INCIDENT_INFO.getTitle());
    assertEquals(incident.getDescription(), TEST_INCIDENT_INFO.getDescription());
    assertEquals(incident.getCustomType(), TEST_INCIDENT_INFO.getCustomType());
    assertEquals(
        incident.getStatus().getState().toString(),
        TEST_INCIDENT_INFO.getStatus().getState().toString());
    assertEquals(incident.getStatus().getMessage(), TEST_INCIDENT_INFO.getStatus().getMessage());
    assertEquals(
        incident.getStatus().getLastUpdated().getTime(),
        TEST_INCIDENT_INFO.getStatus().getLastUpdated().getTime());
    assertEquals(
        incident.getStatus().getLastUpdated().getActor(),
        TEST_INCIDENT_INFO.getStatus().getLastUpdated().getActor().toString());
    assertEquals(
        incident.getSource().getType().toString(),
        TEST_INCIDENT_INFO.getSource().getType().toString());
    assertEquals(
        incident.getSource().getSource().getUrn(),
        TEST_INCIDENT_INFO.getSource().getSourceUrn().toString());
    assertEquals(
        incident.getCreated().getActor(), TEST_INCIDENT_INFO.getCreated().getActor().toString());
    assertEquals(incident.getCreated().getTime(), TEST_INCIDENT_INFO.getCreated().getTime());

    // Assert second element is null.
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet());
    com.linkedin.datahub.graphql.types.incident.IncidentType type =
        new com.linkedin.datahub.graphql.types.incident.IncidentType(mockClient);

    // Execute Batch load
    QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    assertThrows(
        RuntimeException.class,
        () -> type.batchLoad(ImmutableList.of(TEST_INCIDENT_URN, TEST_INCIDENT_URN_2), context));
  }
}

package com.linkedin.datahub.graphql.resolvers.incident;

import static com.linkedin.datahub.graphql.resolvers.incident.EntityIncidentsResolver.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityIncidentsResult;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
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
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.utils.QueryUtils;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class EntityIncidentsResolverTest {
  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Urn assertionUrn = Urn.createFromString("urn:li:assertion:test");
    Urn userUrn = Urn.createFromString("urn:li:corpuser:test");
    Urn datasetUrn = Urn.createFromString("urn:li:dataset:(test,test,test)");
    Urn incidentUrn = Urn.createFromString("urn:li:incident:test-guid");

    Map<String, com.linkedin.entity.EnvelopedAspect> incidentAspects = new HashMap<>();
    incidentAspects.put(
        Constants.INCIDENT_KEY_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(new Aspect(new IncidentKey().setId("test-guid").data())));

    IncidentInfo expectedInfo =
        new IncidentInfo()
            .setType(IncidentType.OPERATIONAL)
            .setCustomType("Custom Type")
            .setDescription("Description")
            .setPriority(5)
            .setTitle("Title")
            .setEntities(new UrnArray(ImmutableList.of(datasetUrn)))
            .setSource(
                new IncidentSource().setType(IncidentSourceType.MANUAL).setSourceUrn(assertionUrn))
            .setStatus(
                new IncidentStatus()
                    .setState(IncidentState.ACTIVE)
                    .setMessage("Message")
                    .setLastUpdated(new AuditStamp().setTime(1L).setActor(userUrn)))
            .setCreated(new AuditStamp().setTime(0L).setActor(userUrn));

    incidentAspects.put(
        Constants.INCIDENT_INFO_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(expectedInfo.data())));

    final Map<String, String> criterionMap = new HashMap<>();
    criterionMap.put(INCIDENT_ENTITIES_SEARCH_INDEX_FIELD_NAME, datasetUrn.toString());
    Filter expectedFilter = QueryUtils.newFilter(criterionMap);

    SortCriterion expectedSort = new SortCriterion();
    expectedSort.setField(CREATED_TIME_SEARCH_INDEX_FIELD_NAME);
    expectedSort.setOrder(SortOrder.DESCENDING);

    Mockito.when(
            mockClient.filter(
                Mockito.any(),
                Mockito.eq(Constants.INCIDENT_ENTITY_NAME),
                Mockito.eq(expectedFilter),
                Mockito.eq(Collections.singletonList(expectedSort)),
                Mockito.eq(0),
                Mockito.eq(10)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(new SearchEntity().setEntity(incidentUrn)))));

    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.INCIDENT_ENTITY_NAME),
                Mockito.eq(ImmutableSet.of(incidentUrn)),
                Mockito.eq(null)))
        .thenReturn(
            ImmutableMap.of(
                incidentUrn,
                new EntityResponse()
                    .setEntityName(Constants.INCIDENT_ENTITY_NAME)
                    .setUrn(incidentUrn)
                    .setAspects(new EnvelopedAspectMap(incidentAspects))));

    EntityIncidentsResolver resolver = new EntityIncidentsResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("start"), Mockito.eq(0))).thenReturn(0);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("count"), Mockito.eq(20))).thenReturn(10);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentEntity = new Dataset();
    parentEntity.setUrn(datasetUrn.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentEntity);

    EntityIncidentsResult result = resolver.get(mockEnv).get();

    // Assert that GraphQL Incident run event matches expectations
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 1);
    assertEquals(result.getTotal(), 1);

    com.linkedin.datahub.graphql.generated.Incident incident =
        resolver.get(mockEnv).get().getIncidents().get(0);
    assertEquals(incident.getUrn(), incidentUrn.toString());
    assertEquals(incident.getType(), EntityType.INCIDENT);
    assertEquals(incident.getIncidentType().toString(), expectedInfo.getType().toString());
    assertEquals(incident.getTitle(), expectedInfo.getTitle());
    assertEquals(incident.getDescription(), expectedInfo.getDescription());
    assertEquals(incident.getCustomType(), expectedInfo.getCustomType());
    assertEquals(
        incident.getStatus().getState().toString(), expectedInfo.getStatus().getState().toString());
    assertEquals(incident.getStatus().getMessage(), expectedInfo.getStatus().getMessage());
    assertEquals(
        incident.getStatus().getLastUpdated().getTime(),
        expectedInfo.getStatus().getLastUpdated().getTime());
    assertEquals(
        incident.getStatus().getLastUpdated().getActor(),
        expectedInfo.getStatus().getLastUpdated().getActor().toString());
    assertEquals(
        incident.getSource().getType().toString(), expectedInfo.getSource().getType().toString());
    assertEquals(
        incident.getSource().getSource().getUrn(),
        expectedInfo.getSource().getSourceUrn().toString());
    assertEquals(incident.getCreated().getActor(), expectedInfo.getCreated().getActor().toString());
    assertEquals(incident.getCreated().getTime(), expectedInfo.getCreated().getTime());
  }
}

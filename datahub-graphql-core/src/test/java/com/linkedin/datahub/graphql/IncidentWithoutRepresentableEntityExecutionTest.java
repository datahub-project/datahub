package com.linkedin.datahub.graphql;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.Constants.INCIDENT_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.DoubleMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.MatchedFieldArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.SearchSuggestionArray;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.StrictHandling;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandlingsForTests;
import graphql.ExecutionResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.Answers;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Runs queries through the real schema and wiring. Incident.entity is non-null, so an incident none
 * of whose entities GraphQL can represent (all of a type only a newer version knows, stripped on
 * read) must be left out of non-null lists: returned with entity unset, it would null the whole
 * list.
 */
public class IncidentWithoutRepresentableEntityExecutionTest {

  private static final String DATASET = "urn:li:dataset:(urn:li:dataPlatform:hive,linked,PROD)";
  private static final String KNOWN_INCIDENT = "urn:li:incident:known";
  private static final String UNREPRESENTABLE_INCIDENT = "urn:li:incident:only-newer-entity";
  // telemetry is in the registry but GraphQL doesn't model it: entity maps to null either way.
  private static final String UNMODELLED_INCIDENT = "urn:li:incident:only-unmodelled-entity";

  private static final String SEARCH =
      "{ searchAcrossEntities(input: {types: [DATASET, INCIDENT], query: \"*\", start: 0,"
          + " count: 10}) { searchResults { entity { urn ... on Incident { entity { urn } } } } } }";
  private static final String ENTITY_INCIDENTS =
      "{ dataset(urn: \""
          + DATASET
          + "\") { incidents(start: 0, count: 10) { incidents { urn entity { urn } } } } }";
  private static final String ENTITY_INCIDENT_URNS =
      "{ dataset(urn: \""
          + DATASET
          + "\") { incidents(start: 0, count: 10) { incidents { urn } } } }";

  // The incidents an entity's incidents filter returns.
  private List<String> filterHits =
      List.of(KNOWN_INCIDENT, UNREPRESENTABLE_INCIDENT, UNMODELLED_INCIDENT);

  @BeforeMethod
  public void tolerateUnknownData() {
    UnknownDataHandlingsForTests.use(TolerantHandling.INSTANCE);
  }

  @AfterMethod
  public void resetHandling() {
    UnknownDataHandlingsForTests.reset();
  }

  @Test
  public void testSearchResultsKeepTheOtherResults() throws Exception {
    // Search doesn't map incident hits to a GraphQL entity, so none reaches searchResults: the
    // page still loads with the other results.
    ExecutionResult result = execute(SEARCH);

    assertTrue(result.getErrors().isEmpty(), result.getErrors().toString());
    List<Map<String, Object>> hits = get(result.getData(), "searchAcrossEntities", "searchResults");
    assertEquals(
        hits.stream().map(hit -> get(hit, "entity", "urn")).collect(Collectors.toList()),
        List.of(DATASET));
  }

  @Test
  public void testEntityIncidentsKeepTheOtherIncidents() throws Exception {
    ExecutionResult result = execute(ENTITY_INCIDENTS);

    assertTrue(result.getErrors().isEmpty(), result.getErrors().toString());
    List<Map<String, Object>> incidents =
        get(result.getData(), "dataset", "incidents", "incidents");
    assertEquals(
        incidents.stream().map(incident -> incident.get("urn")).collect(Collectors.toList()),
        List.of(KNOWN_INCIDENT));
    assertEquals(get(incidents.get(0), "entity", "urn"), DATASET);
  }

  @Test
  public void testStrictKeepsEveryIncidentAsBefore() throws Exception {
    // Without tolerance the list is as it was: an incident whose entity GraphQL can't map is
    // still listed, and only a query selecting its entity fails.
    UnknownDataHandlingsForTests.use(StrictHandling.INSTANCE);
    filterHits = List.of(KNOWN_INCIDENT, UNMODELLED_INCIDENT);

    ExecutionResult result = execute(ENTITY_INCIDENT_URNS);

    assertTrue(result.getErrors().isEmpty(), result.getErrors().toString());
    List<Map<String, Object>> incidents =
        get(result.getData(), "dataset", "incidents", "incidents");
    assertEquals(
        incidents.stream().map(incident -> incident.get("urn")).collect(Collectors.toList()),
        List.of(KNOWN_INCIDENT, UNMODELLED_INCIDENT));
  }

  private ExecutionResult execute(String query) throws Exception {
    QueryContext context = getMockAllowContext();
    EntityRegistry registry = context.getOperationContext().getEntityRegistry();
    // Both incidents as read: the unrepresentable one's reference was stripped, leaving none.
    Map<Urn, IncidentInfo> stored =
        Map.of(
            UrnUtils.getUrn(KNOWN_INCIDENT), incidentInfo(DATASET),
            UrnUtils.getUrn(UNREPRESENTABLE_INCIDENT), incidentInfo(),
            UrnUtils.getUrn(UNMODELLED_INCIDENT), incidentInfo("urn:li:telemetry:client"));
    AspectRetriever retriever = mock(AspectRetriever.class);
    when(retriever.getLatestAspectObjects(any(), anySet(), anySet()))
        .thenAnswer(
            invocation ->
                ((Set<Urn>) invocation.getArgument(1))
                    .stream()
                        .filter(stored::containsKey)
                        .collect(
                            Collectors.toMap(
                                urn -> urn,
                                urn ->
                                    Map.of(
                                        INCIDENT_INFO_ASPECT_NAME,
                                        new Aspect(stored.get(urn).data())))));
    OperationContext opContext = spy(context.getOperationContext());
    when(opContext.getAspectRetriever()).thenReturn(retriever);
    when(context.getOperationContext()).thenReturn(opContext);

    EntityClient entityClient = mock(EntityClient.class, backend(registry, stored));
    return MockedGmsGraphQLEngine.execute(query, context, entityClient);
  }

  /** The hits for any search or filter; entities as stored or by their key. */
  private Answer<Object> backend(EntityRegistry registry, Map<Urn, IncidentInfo> stored) {
    return invocation -> {
      switch (invocation.getMethod().getName()) {
        case "searchAcrossEntities":
          return hits(DATASET, KNOWN_INCIDENT, UNREPRESENTABLE_INCIDENT);
        case "filter":
          return hits(filterHits.toArray(new String[0]));
        case "batchGetV2":
          Map<Urn, EntityResponse> responses = new HashMap<>();
          for (Object requested : (Set<?>) invocation.getArgument(2)) {
            Urn urn = (Urn) requested;
            EntityResponse response = keyOnlyResponse(registry, urn);
            if (stored.containsKey(urn)) {
              response
                  .getAspects()
                  .put(
                      INCIDENT_INFO_ASPECT_NAME,
                      new EnvelopedAspect()
                          .setName(INCIDENT_INFO_ASPECT_NAME)
                          .setValue(new Aspect(stored.get(urn).data())));
            }
            responses.put(urn, response);
          }
          return responses;
        default:
          return Answers.RETURNS_DEFAULTS.answer(invocation);
      }
    };
  }

  /** The entity as only its key aspect, enough for the mappers to resolve it. */
  private static EntityResponse keyOnlyResponse(EntityRegistry registry, Urn urn) {
    AspectSpec keySpec = registry.getEntitySpec(urn.getEntityType()).getKeyAspectSpec();
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        keySpec.getName(),
        new EnvelopedAspect()
            .setName(keySpec.getName())
            .setValue(new Aspect(EntityKeyUtils.convertUrnToEntityKey(urn, keySpec).data())));
    return new EntityResponse().setUrn(urn).setEntityName(urn.getEntityType()).setAspects(aspects);
  }

  private static SearchResult hits(String... urns) {
    SearchEntityArray hits = new SearchEntityArray();
    for (String urn : urns) {
      hits.add(
          new SearchEntity()
              .setEntity(UrnUtils.getUrn(urn))
              .setMatchedFields(new MatchedFieldArray())
              .setFeatures(new DoubleMap()));
    }
    return new SearchResult()
        .setFrom(0)
        .setPageSize(10)
        .setNumEntities(hits.size())
        .setEntities(hits)
        .setMetadata(
            new SearchResultMetadata()
                .setAggregations(new AggregationMetadataArray())
                .setSuggestions(new SearchSuggestionArray()));
  }

  private static IncidentInfo incidentInfo(String... entities) {
    UrnArray urns = new UrnArray();
    for (String entity : entities) {
      urns.add(UrnUtils.getUrn(entity));
    }
    return new IncidentInfo()
        .setType(IncidentType.OPERATIONAL)
        .setTitle("incident")
        .setEntities(urns)
        .setStatus(
            new IncidentStatus()
                .setState(IncidentState.ACTIVE)
                .setLastUpdated(AuditStampUtils.createDefaultAuditStamp()))
        .setCreated(AuditStampUtils.createDefaultAuditStamp());
  }

  @SuppressWarnings("unchecked")
  private static <T> T get(Object node, String... path) {
    Object current = node;
    for (String key : path) {
      current = ((Map<String, Object>) current).get(key);
    }
    return (T) current;
  }
}

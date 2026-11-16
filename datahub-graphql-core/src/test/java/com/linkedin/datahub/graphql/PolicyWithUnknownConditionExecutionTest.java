package com.linkedin.datahub.graphql;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.Constants.DATAHUB_POLICY_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.DoubleMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.MatchedFieldArray;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.SearchSuggestionArray;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandlingsForTests;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.policy.PolicyMatchCondition;
import com.linkedin.policy.PolicyMatchCriterion;
import com.linkedin.policy.PolicyMatchCriterionArray;
import com.linkedin.policy.PolicyMatchFilter;
import graphql.ExecutionResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
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
 * Runs queries through the real schema and wiring. PolicyMatchCriterion.condition is non-null, so a
 * policy with a match condition only a newer version defines is returned as missing on get and left
 * out of search and scroll, as listPolicies leaves it out, instead of failing the query that
 * selects the condition.
 */
public class PolicyWithUnknownConditionExecutionTest {

  private static final String KNOWN = "urn:li:dataHubPolicy:known";
  private static final String UNKNOWN = "urn:li:dataHubPolicy:unknown-condition";
  private static final String MIXED = "urn:li:dataHubPolicy:mixed-conditions";
  private static final String FIELDS =
      "urn ... on DataHubPolicy { resources { filter { criteria { field condition } } } }";
  private static final String SEARCH =
      "{ searchAcrossEntities(input: {types: [DATAHUB_POLICY], query: \"*\", start: 0, count: 10})"
          + " { searchResults { entity { "
          + FIELDS
          + " } } } }";
  private static final String SCROLL =
      "{ scrollAcrossEntities(input: {types: [DATAHUB_POLICY], query: \"*\", count: 10})"
          + " { searchResults { entity { "
          + FIELDS
          + " } } } }";

  private static final Map<Urn, DataHubPolicyInfo> STORED =
      Map.of(
          UrnUtils.getUrn(KNOWN), policy(criterion("TYPE", "EQUALS")),
          UrnUtils.getUrn(UNKNOWN), policy(criterion("TYPE", "CONDITION_FROM_NEWER_BUILD")),
          UrnUtils.getUrn(MIXED),
              policy(criterion("TYPE", "EQUALS"), criterion("URN", "CONDITION_FROM_NEWER_BUILD")));

  // Search's read of the policies' info fails.
  private boolean failPolicyInfoRead;

  @BeforeMethod
  public void tolerateUnknownData() {
    failPolicyInfoRead = false;
    UnknownDataHandlingsForTests.use(TolerantHandling.INSTANCE);
  }

  @AfterMethod
  public void resetHandling() {
    UnknownDataHandlingsForTests.reset();
  }

  @Test
  public void testGetReturnsThePolicyAsMissing() throws Exception {
    ExecutionResult result = execute("{ entity(urn: \"" + UNKNOWN + "\") { " + FIELDS + " } }");

    assertTrue(result.getErrors().isEmpty(), result.getErrors().toString());
    assertNull(((Map<?, ?>) result.getData()).get("entity"));
  }

  @Test
  public void testBatchGetKeepsTheOtherPolicies() throws Exception {
    ExecutionResult result =
        execute(
            "{ entities(urns: [\""
                + KNOWN
                + "\", \""
                + UNKNOWN
                + "\", \""
                + MIXED
                + "\"]) { "
                + FIELDS
                + " } }");

    assertTrue(result.getErrors().isEmpty(), result.getErrors().toString());
    List<Map<String, Object>> entities = get(result.getData(), "entities");
    assertEquals(entities.get(0).get("urn"), KNOWN);
    assertNull(entities.get(1));
    assertNull(entities.get(2));
  }

  @Test
  public void testSearchAndScrollKeepTheOtherPolicies() throws Exception {
    assertOnlyKnownPolicy(execute(SEARCH), "searchAcrossEntities");
    assertOnlyKnownPolicy(execute(SCROLL), "scrollAcrossEntities");
  }

  @Test
  public void testSearchAndScrollWithOnlyUnknownEnumsTolerated() throws Exception {
    // The loader leaves these policies out whenever unknown enum values are tolerated, so search
    // must too, even while unknown entity types aren't.
    UnknownDataHandlingsForTests.use(TolerantHandling.of(false, true, entityType -> true));

    assertOnlyKnownPolicy(execute(SEARCH), "searchAcrossEntities");
    assertOnlyKnownPolicy(execute(SCROLL), "scrollAcrossEntities");
  }

  @Test
  public void testSearchLeavesPoliciesOutWhenTheirInfoCannotBeRead() throws Exception {
    failPolicyInfoRead = true;

    ExecutionResult result = execute(SEARCH);

    assertTrue(result.getErrors().isEmpty(), result.getErrors().toString());
    List<Map<String, Object>> hits = get(result.getData(), "searchAcrossEntities", "searchResults");
    assertTrue(hits.isEmpty(), hits.toString());
  }

  private static void assertOnlyKnownPolicy(ExecutionResult result, String field) {
    assertTrue(result.getErrors().isEmpty(), result.getErrors().toString());
    List<Map<String, Object>> hits = get(result.getData(), field, "searchResults");
    assertEquals(
        hits.stream().map(hit -> get(hit, "entity", "urn")).collect(Collectors.toList()),
        List.of(KNOWN));
  }

  private ExecutionResult execute(String query) throws Exception {
    QueryContext context = getMockAllowContext();
    EntityRegistry registry = context.getOperationContext().getEntityRegistry();
    AspectRetriever retriever = mock(AspectRetriever.class);
    if (failPolicyInfoRead) {
      when(retriever.getLatestAspectObjects(any(), anySet(), anySet()))
          .thenThrow(new RuntimeException("storage unavailable"));
    } else {
      when(retriever.getLatestAspectObjects(any(), anySet(), anySet()))
          .thenAnswer(
              invocation ->
                  ((Set<Urn>) invocation.getArgument(1))
                      .stream()
                          .filter(STORED::containsKey)
                          .collect(
                              Collectors.toMap(
                                  urn -> urn,
                                  urn ->
                                      Map.of(
                                          DATAHUB_POLICY_INFO_ASPECT_NAME,
                                          new Aspect(STORED.get(urn).data())))));
    }
    OperationContext opContext = spy(context.getOperationContext());
    when(opContext.getAspectRetriever()).thenReturn(retriever);
    when(context.getOperationContext()).thenReturn(opContext);
    return MockedGmsGraphQLEngine.execute(
        query, context, mock(EntityClient.class, backend(registry)));
  }

  /** The three policies for any search or scroll; each policy as stored. */
  private static Answer<Object> backend(EntityRegistry registry) {
    return invocation -> {
      switch (invocation.getMethod().getName()) {
        case "searchAcrossEntities":
          return new SearchResult()
              .setFrom(0)
              .setPageSize(10)
              .setNumEntities(3)
              .setEntities(hits())
              .setMetadata(metadata());
        case "scrollAcrossEntities":
          return new ScrollResult()
              .setPageSize(10)
              .setNumEntities(3)
              .setEntities(hits())
              .setMetadata(metadata());
        case "batchGetV2":
          Map<Urn, EntityResponse> responses = new HashMap<>();
          for (Object requested : (Set<?>) invocation.getArgument(2)) {
            Urn urn = (Urn) requested;
            EntityResponse response = keyOnlyResponse(registry, urn);
            response
                .getAspects()
                .put(
                    DATAHUB_POLICY_INFO_ASPECT_NAME,
                    new EnvelopedAspect()
                        .setName(DATAHUB_POLICY_INFO_ASPECT_NAME)
                        .setValue(new Aspect(STORED.get(urn).data())));
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

  private static SearchEntityArray hits() {
    SearchEntityArray hits = new SearchEntityArray();
    for (String urn : List.of(KNOWN, UNKNOWN, MIXED)) {
      hits.add(
          new SearchEntity()
              .setEntity(UrnUtils.getUrn(urn))
              .setMatchedFields(new MatchedFieldArray())
              .setFeatures(new DoubleMap()));
    }
    return hits;
  }

  private static SearchResultMetadata metadata() {
    return new SearchResultMetadata()
        .setAggregations(new AggregationMetadataArray())
        .setSuggestions(new SearchSuggestionArray());
  }

  private static PolicyMatchCriterion criterion(String field, String condition) {
    PolicyMatchCriterion criterion =
        new PolicyMatchCriterion()
            .setField(field)
            .setValues(new StringArray("dataset"))
            .setCondition(PolicyMatchCondition.EQUALS);
    // As a newer version wrote it: the symbol may not be in this version's enum.
    criterion.data().put("condition", condition);
    return criterion;
  }

  private static DataHubPolicyInfo policy(PolicyMatchCriterion... criteria) {
    return new DataHubPolicyInfo()
        .setDisplayName("policy")
        .setDescription("policy")
        .setType("METADATA")
        .setState("ACTIVE")
        .setPrivileges(new StringArray("VIEW_ENTITY_PAGE"))
        .setActors(new DataHubActorFilter())
        .setEditable(true)
        .setResources(
            new DataHubResourceFilter()
                .setFilter(
                    new PolicyMatchFilter()
                        .setCriteria(new PolicyMatchCriterionArray(Arrays.asList(criteria)))));
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

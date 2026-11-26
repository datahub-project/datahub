package com.linkedin.datahub.graphql;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.DoubleMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.MatchedFieldArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.SearchSuggestionArray;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandlingsForTests;
import graphql.ExecutionResult;
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
 * Runs queries through the real schema and wiring. SchemaFieldEntity.parent is non-null, so a
 * schema field whose parent GraphQL can't represent (telemetry is in the registry but not modelled)
 * is returned as missing, and left out of search, instead of failing the query that selects parent.
 */
public class SchemaFieldWithoutRepresentableParentExecutionTest {

  private static final String ON_DATASET =
      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,linked,PROD),col_a)";
  private static final String ON_TELEMETRY = "urn:li:schemaField:(urn:li:telemetry:client,col_b)";
  private static final String FIELDS = "urn ... on SchemaFieldEntity { fieldPath parent { urn } }";

  @BeforeMethod
  public void tolerateUnknownData() {
    UnknownDataHandlingsForTests.use(TolerantHandling.INSTANCE);
  }

  @AfterMethod
  public void resetHandling() {
    UnknownDataHandlingsForTests.reset();
  }

  @Test
  public void testDirectLoadReturnsTheFieldAsMissing() throws Exception {
    ExecutionResult result =
        execute("{ entity(urn: \"" + ON_TELEMETRY + "\") { " + FIELDS + " } }");

    assertTrue(result.getErrors().isEmpty(), result.getErrors().toString());
    assertNull(((Map<?, ?>) result.getData()).get("entity"));
  }

  @Test
  public void testBatchLoadKeepsTheOtherFields() throws Exception {
    ExecutionResult result =
        execute(
            "{ entities(urns: [\""
                + ON_DATASET
                + "\", \""
                + ON_TELEMETRY
                + "\"]) { "
                + FIELDS
                + " } }");

    assertTrue(result.getErrors().isEmpty(), result.getErrors().toString());
    List<Map<String, Object>> entities = get(result.getData(), "entities");
    assertEquals(entities.get(0).get("urn"), ON_DATASET);
    assertNull(entities.get(1));
  }

  @Test
  public void testSearchResultsKeepTheOtherFields() throws Exception {
    ExecutionResult result =
        execute(
            "{ searchAcrossEntities(input: {types: [SCHEMA_FIELD], query: \"*\", start: 0,"
                + " count: 10}) { searchResults { entity { "
                + FIELDS
                + " } } } }");

    assertTrue(result.getErrors().isEmpty(), result.getErrors().toString());
    List<Map<String, Object>> hits = get(result.getData(), "searchAcrossEntities", "searchResults");
    assertEquals(
        hits.stream().map(hit -> get(hit, "entity", "urn")).collect(Collectors.toList()),
        List.of(ON_DATASET));
  }

  private ExecutionResult execute(String query) throws Exception {
    QueryContext context = getMockAllowContext();
    EntityRegistry registry = context.getOperationContext().getEntityRegistry();
    return MockedGmsGraphQLEngine.execute(
        query, context, mock(EntityClient.class, backend(registry)));
  }

  /** Both schema fields for any search; any entity by its key alone. */
  private static Answer<Object> backend(EntityRegistry registry) {
    return invocation -> {
      switch (invocation.getMethod().getName()) {
        case "searchAcrossEntities":
          return hits(ON_DATASET, ON_TELEMETRY);
        case "batchGetV2":
          Map<Urn, EntityResponse> responses = new HashMap<>();
          for (Object requested : (Set<?>) invocation.getArgument(2)) {
            Urn urn = (Urn) requested;
            responses.put(urn, keyOnlyResponse(registry, urn));
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
    Arrays.stream(urns)
        .forEach(
            urn ->
                hits.add(
                    new SearchEntity()
                        .setEntity(UrnUtils.getUrn(urn))
                        .setMatchedFields(new MatchedFieldArray())
                        .setFeatures(new DoubleMap())));
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

  @SuppressWarnings("unchecked")
  private static <T> T get(Object node, String... path) {
    Object current = node;
    for (String key : path) {
      current = ((Map<String, Object>) current).get(key);
    }
    return (T) current;
  }
}

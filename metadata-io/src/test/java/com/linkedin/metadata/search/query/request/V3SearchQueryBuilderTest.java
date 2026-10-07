package com.linkedin.metadata.search.query.request;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder.STRUCTURED_QUERY_PREFIX;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.FieldConfiguration;
import com.linkedin.metadata.config.search.custom.SearchFields;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields;
import com.linkedin.metadata.search.elasticsearch.query.request.V3SearchQueryBuilder;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.testng.annotations.Test;

/** The Stage 1 query over the shared {@code _search} fields of the Search V3 indices. */
public class V3SearchQueryBuilderTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final List<EntitySpec> DATASET =
      List.of(OP_CONTEXT.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME));

  @Test
  public void testFullQueryIsStage1OverSharedFields() {
    QueryBuilder query = builder(null).buildQuery(OP_CONTEXT, DATASET, "orders table", true);

    assertTrue(
        ((FunctionScoreQueryBuilder) query).query() instanceof DisMaxQueryBuilder,
        query.toString());
    // The fuzzy simple queries, one per analyzer: the shared text and stemmed subfields
    Map<String, Set<String>> fuzzy =
        find(query, SimpleQueryStringBuilder.class).stream()
            .filter(sqs -> sqs.value().contains("~"))
            .collect(
                Collectors.toMap(
                    SimpleQueryStringBuilder::analyzer,
                    sqs -> sqs.fields().keySet(),
                    (a, b) -> {
                      Set<String> merged = new HashSet<>(a);
                      merged.addAll(b);
                      return merged;
                    }));
    Set<String> text = fuzzy.get(V3SearchFields.TEXT_SEARCH_ANALYZER);
    assertNotNull(text, fuzzy.toString());
    assertTrue(
        text.containsAll(
            Set.of(
                "_search.entityName.text",
                "_search.description.text",
                "_search.columns.text",
                "_search.structuredProperties.text",
                "_search.other.text")),
        text.toString());
    assertTrue(text.stream().allMatch(field -> field.endsWith(".text")), text.toString());
    Set<String> stemmed = fuzzy.get(V3SearchFields.STEMMED_SEARCH_ANALYZER);
    assertNotNull(stemmed, fuzzy.toString());
    assertTrue(stemmed.contains("_search.entityName.stemmed"), stemmed.toString());
    assertTrue(stemmed.stream().allMatch(field -> field.endsWith(".stemmed")), stemmed.toString());
  }

  /** V3 indices have neither the per-field analyzed subfields nor their analyzers. */
  @Test
  public void testNoPerFieldSubfieldsOrAnalyzers() {
    V3SearchQueryBuilder builder = builder(null);
    for (String input :
        List.of(
            "orders",
            "orders table",
            "sales.orders",
            "customer master data for orders",
            "load_job-0001",
            "orders2017",
            "\"orders table\"",
            STRUCTURED_QUERY_PREFIX + "name:orders")) {
      for (boolean light : List.of(false, true)) {
        QueryBuilder query = builder.buildQuery(OP_CONTEXT, DATASET, input, true, light);
        if (query == null) {
          continue;
        }
        String json = query.toString();
        for (String v2Only :
            List.of(".delimited", "wordGrams", "word_gram", ".ngram", "query_word_delimited")) {
          assertFalse(json.contains(v2Only), input + " (light " + light + "): " + json);
        }
      }
    }
  }

  /**
   * Deliberate V3 change: shared-field weights replace @Searchable boost scores in the analyzed
   * clauses, so a name weighs more than a description, and that more than the catch-all.
   */
  @Test
  public void testSharedFieldWeights() {
    QueryBuilder query = builder(null).buildQuery(OP_CONTEXT, DATASET, "orders", true);

    Map<String, Float> weights =
        find(query, SimpleQueryStringBuilder.class).stream()
            .filter(
                sqs ->
                    sqs.value().contains("~")
                        && V3SearchFields.TEXT_SEARCH_ANALYZER.equals(sqs.analyzer()))
            .findFirst()
            .orElseThrow()
            .fields();
    assertTrue(
        weights.get("_search.entityName.text") > weights.get("_search.description.text"),
        weights.toString());
    assertTrue(
        weights.get("_search.description.text") > weights.get("_search.other.text"),
        weights.toString());
    // Structured property values weigh what DataHub Cloud's query gives them, between the two
    assertTrue(
        weights.get("_search.description.text") > weights.get("_search.structuredProperties.text"),
        weights.toString());
    assertTrue(
        weights.get("_search.structuredProperties.text") > weights.get("_search.other.text"),
        weights.toString());
  }

  /**
   * Exact matches read the root keywords and the urn, with the constant score for an exact name;
   * phrase prefixes and the wildcard read the shared fields that name the entity.
   */
  @Test
  public void testExactPrefixAndWildcardClauses() {
    QueryBuilder query = builder(null).buildQuery(OP_CONTEXT, DATASET, "orders", true);

    assertTrue(
        find(query, ConstantScoreQueryBuilder.class).stream()
            .map(ConstantScoreQueryBuilder::innerQuery)
            .anyMatch(
                inner ->
                    inner instanceof TermQueryBuilder term
                        && term.fieldName().equals("name.keyword")),
        query.toString());
    Set<String> exact =
        find(query, TermQueryBuilder.class).stream()
            .map(TermQueryBuilder::fieldName)
            .collect(Collectors.toSet());
    assertTrue(exact.containsAll(Set.of("name.keyword", "urn")), exact.toString());
    assertTrue(
        exact.stream().allMatch(field -> field.equals("urn") || field.endsWith(".keyword")),
        exact.toString());

    Set<String> prefixes =
        find(query, MatchPhrasePrefixQueryBuilder.class).stream()
            .map(MatchPhrasePrefixQueryBuilder::fieldName)
            .collect(Collectors.toSet());
    assertTrue(prefixes.contains("_search.entityName.text"), prefixes.toString());
    // A prefix of the catch-all would match any urn component or custom property
    assertTrue(
        prefixes.stream()
            .allMatch(
                field ->
                    field.startsWith("_search.entityName.")
                        || field.startsWith("_search.qualifiedName.")),
        prefixes.toString());
    assertTrue(
        find(query, WildcardQueryBuilder.class).stream()
            .allMatch(wildcard -> wildcard.fieldName().startsWith("_search.entityName.")),
        query.toString());
    assertFalse(find(query, WildcardQueryBuilder.class).isEmpty(), query.toString());
  }

  @Test
  public void testFqnAndDescriptionPhraseReadSharedFields() {
    V3SearchQueryBuilder builder = builder(null);

    Set<String> fqn =
        find(builder.buildQuery(OP_CONTEXT, DATASET, "sales.orders", true), MatchQueryBuilder.class)
            .stream()
            .filter(match -> match.operator() == Operator.AND)
            .map(MatchQueryBuilder::fieldName)
            .collect(Collectors.toSet());
    assertEquals(fqn, Set.of("_search.qualifiedName.text", "_search.other.text"));

    assertEquals(
        find(
                builder.buildQuery(OP_CONTEXT, DATASET, "customer master data for orders", true),
                MatchQueryBuilder.class)
            .stream()
            .map(MatchQueryBuilder::fieldName)
            .collect(Collectors.toSet()),
        Set.of("_search.description.text"));
  }

  @Test
  public void testLightQueryReadsSharedFields() {
    V3SearchQueryBuilder builder = builder(null);

    // A single name searches the shared fields that name the entity and, as DataHub Cloud's light
    // query does, the structured property values
    Set<String> name =
        only(find(
                builder.buildQuery(OP_CONTEXT, DATASET, "orders", true, true),
                MultiMatchQueryBuilder.class))
            .fields()
            .keySet();
    assertEquals(
        name,
        Set.of(
            "_search.entityName.text",
            "_search.entityName.stemmed",
            "_search.qualifiedName.text",
            "_search.qualifiedName.stemmed",
            "_search.structuredProperties.text",
            "_search.structuredProperties.stemmed"));

    // Several words search every shared field
    Set<String> words =
        only(find(
                builder.buildQuery(OP_CONTEXT, DATASET, "orders table", true, true),
                MultiMatchQueryBuilder.class))
            .fields()
            .keySet();
    assertTrue(
        words.containsAll(Set.of("_search.description.text", "_search.columns.stemmed")),
        words.toString());
    assertTrue(words.stream().allMatch(field -> field.startsWith("_search.")), words.toString());

    // An identifier the escaping breaks up is matched whole on the names
    List<MultiMatchQueryBuilder> identity =
        find(
                builder.buildQuery(OP_CONTEXT, DATASET, "load_job-0001", true, true),
                MultiMatchQueryBuilder.class)
            .stream()
            .filter(match -> match.value().equals("load_job-0001"))
            .collect(Collectors.toList());
    assertEquals(
        only(identity).fields().keySet(),
        Set.of("_search.entityName.text", "_search.qualifiedName.text"));
  }

  @Test
  public void testStructuredQueryDefaultFields() {
    QueryBuilder query =
        builder(null)
            .buildQuery(OP_CONTEXT, DATASET, STRUCTURED_QUERY_PREFIX + "name:orders", true);

    QueryStringQueryBuilder structured = only(find(query, QueryStringQueryBuilder.class));
    assertEquals(structured.queryString(), "name:orders");
    assertTrue(
        structured.fields().keySet().containsAll(Set.of("_search.entityName.text", "name")),
        structured.fields().toString());
    assertTrue(find(query, SimpleQueryStringBuilder.class).isEmpty());
  }

  /**
   * Deliberate V3 change: a field configuration names shared fields, directly or by a field that
   * feeds one, so removing fieldPaths removes the whole columns field.
   */
  @Test
  public void testFieldConfigurationsSelectSharedFields() {
    V3SearchQueryBuilder configured =
        builder(
            CustomSearchConfiguration.builder()
                .fieldConfigurations(
                    Map.of(
                        "names",
                        searchFields(SearchFields.builder().replace(List.of("name")).build()),
                        "noColumns",
                        searchFields(SearchFields.builder().remove(List.of("fieldPaths")).build()),
                        "invalid",
                        searchFields(
                            SearchFields.builder()
                                .replace(List.of("name"))
                                .add(List.of("description"))
                                .build())))
                .build());

    assertEquals(
        configured.searchedFields(withFieldConfiguration("names"), DATASET).keySet(),
        Set.of(V3SearchFields.ENTITY_NAME));
    // Only the entity name, and the root fields that feed it
    List<String> nameRoots =
        configured.searchedFields(OP_CONTEXT, DATASET).get(V3SearchFields.ENTITY_NAME);
    for (boolean light : List.of(false, true)) {
      QueryBuilder names =
          configured.buildQuery(withFieldConfiguration("names"), DATASET, "orders", true, light);
      Set<String> fields = new HashSet<>();
      find(names, SimpleQueryStringBuilder.class).forEach(q -> fields.addAll(q.fields().keySet()));
      find(names, MultiMatchQueryBuilder.class).forEach(q -> fields.addAll(q.fields().keySet()));
      assertTrue(fields.contains("_search.entityName.text"), fields.toString());
      assertTrue(
          fields.stream()
              .allMatch(
                  field -> field.startsWith("_search.entityName.") || nameRoots.contains(field)),
          fields.toString());
    }

    Set<String> noColumns =
        configured.searchedFields(withFieldConfiguration("noColumns"), DATASET).keySet();
    assertFalse(noColumns.contains(V3SearchFields.COLUMNS), noColumns.toString());
    assertTrue(noColumns.contains(V3SearchFields.DESCRIPTION), noColumns.toString());
    for (boolean light : List.of(false, true)) {
      String query =
          configured
              .buildQuery(withFieldConfiguration("noColumns"), DATASET, "orders table", true, light)
              .toString();
      assertTrue(query.contains("_search.description.text"), query);
      assertFalse(query.contains("_search.columns"), query);
      assertFalse(query.contains("\"fieldPaths"), query);
    }
    // Replace cannot be combined with add: the configuration is ignored
    assertEquals(
        configured.searchedFields(withFieldConfiguration("invalid"), DATASET),
        configured.searchedFields(OP_CONTEXT, DATASET));
  }

  /** A long-tail field does not stand for the whole catch-all other field. */
  @Test
  public void testFieldConfigurationNamingALongTailFieldKeepsOther() {
    V3SearchQueryBuilder configured =
        builder(
            CustomSearchConfiguration.builder()
                .fieldConfigurations(
                    Map.of(
                        "noTags",
                        searchFields(SearchFields.builder().remove(List.of("tags")).build())))
                .build());

    assertTrue(
        configured
            .searchedFields(withFieldConfiguration("noTags"), DATASET)
            .containsKey(V3SearchFields.OTHER));
  }

  /** A field configuration that removes every shared field searches them all, as V2 does. */
  @Test
  public void testFieldConfigurationRemovingEveryFieldKeepsTheBaseFields() {
    V3SearchQueryBuilder configured =
        builder(
            CustomSearchConfiguration.builder()
                .fieldConfigurations(
                    Map.of(
                        "nothing",
                        searchFields(
                            SearchFields.builder()
                                .remove(
                                    List.of(
                                        V3SearchFields.ENTITY_NAME,
                                        V3SearchFields.QUALIFIED_NAME,
                                        V3SearchFields.DESCRIPTION,
                                        V3SearchFields.COLUMNS,
                                        V3SearchFields.STRUCTURED_PROPERTIES,
                                        V3SearchFields.OTHER))
                                .build())))
                .build());

    assertEquals(
        configured.searchedFields(withFieldConfiguration("nothing"), DATASET),
        configured.searchedFields(OP_CONTEXT, DATASET));
    assertEquals(
        configured.buildQuery(withFieldConfiguration("nothing"), DATASET, "orders", true),
        configured.buildQuery(OP_CONTEXT, DATASET, "orders", true));
  }

  /**
   * As on V2, the production query configuration skips the simple queries, one per analyzer, for a
   * quoted query; the all-terms match of the names still applies.
   */
  @Test
  public void testProductionConfigurationSkipsSimpleQueryForQuotedQuery() throws Exception {
    CustomConfiguration customConfiguration = new CustomConfiguration();
    customConfiguration.setEnabled(true);
    customConfiguration.setFile("search_config.yaml");
    V3SearchQueryBuilder production = builder(customConfiguration.resolve(new YAMLMapper()));

    QueryBuilder quoted = production.buildQuery(OP_CONTEXT, DATASET, "\"orders table\"", true);
    assertTrue(
        find(quoted, SimpleQueryStringBuilder.class).stream()
            .allMatch(simple -> simple.analyzer() == null),
        quoted.toString());
    assertFalse(find(quoted, MatchPhrasePrefixQueryBuilder.class).isEmpty(), quoted.toString());

    QueryBuilder plain = production.buildQuery(OP_CONTEXT, DATASET, "orders table", true);
    assertTrue(
        find(plain, SimpleQueryStringBuilder.class).stream()
            .anyMatch(simple -> V3SearchFields.TEXT_SEARCH_ANALYZER.equals(simple.analyzer())),
        plain.toString());
  }

  private static V3SearchQueryBuilder builder(CustomSearchConfiguration customConfiguration) {
    return new V3SearchQueryBuilder(SearchQueryBuilderTest.testQueryConfig, customConfiguration);
  }

  private static FieldConfiguration searchFields(SearchFields searchFields) {
    return FieldConfiguration.builder().searchFields(searchFields).build();
  }

  private static OperationContext withFieldConfiguration(String label) {
    return OP_CONTEXT.withSearchFlags(flags -> flags.setFieldConfiguration(label));
  }

  private static <T extends QueryBuilder> List<T> find(QueryBuilder query, Class<T> type) {
    List<T> found = new ArrayList<>();
    if (type.isInstance(query)) {
      found.add(type.cast(query));
    } else if (query instanceof FunctionScoreQueryBuilder functionScore) {
      found.addAll(find(functionScore.query(), type));
    } else if (query instanceof DisMaxQueryBuilder disMax) {
      disMax.innerQueries().forEach(clause -> found.addAll(find(clause, type)));
    } else if (query instanceof ConstantScoreQueryBuilder constantScore) {
      found.addAll(find(constantScore.innerQuery(), type));
    } else if (query instanceof BoolQueryBuilder bool) {
      for (List<QueryBuilder> clauses : List.of(bool.must(), bool.should(), bool.filter())) {
        clauses.forEach(clause -> found.addAll(find(clause, type)));
      }
    }
    return found;
  }

  private static <T> T only(List<T> found) {
    assertEquals(found.size(), 1, found.toString());
    return found.get(0);
  }
}

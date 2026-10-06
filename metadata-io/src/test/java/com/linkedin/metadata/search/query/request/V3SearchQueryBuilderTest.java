package com.linkedin.metadata.search.query.request;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder.STRUCTURED_QUERY_PREFIX;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.FieldConfiguration;
import com.linkedin.metadata.config.search.custom.SearchFields;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields;
import com.linkedin.metadata.search.elasticsearch.query.request.V3SearchQueryBuilder;
import com.linkedin.metadata.search.utils.ESUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.testng.annotations.Test;

public class V3SearchQueryBuilderTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final List<EntitySpec> DATASET =
      List.of(OP_CONTEXT.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME));

  @Test
  public void testQueryReadsOnlySharedFieldsAndTheUrn() {
    QueryBuilder query = builder(null).buildQuery(OP_CONTEXT, DATASET, "orders table", true);

    Set<String> fields = queriedFields(query);
    assertTrue(
        fields.containsAll(Set.of("_search.entityName.text", "_search.description.stemmed", "urn")),
        fields.toString());
    assertTrue(
        fields.stream().allMatch(field -> field.equals("urn") || field.startsWith("_search.")),
        fields.toString());
    assertTrue(
        fields.stream()
            .noneMatch(
                field ->
                    field.contains(".delimited")
                        || field.contains(".ngram")
                        || field.contains("wordGrams")),
        fields.toString());
  }

  /**
   * Deliberate V3 change: shared-field weights replace @Searchable boost scores, and exact match
   * reads only the keywords of the fields that name the entity, and the urn.
   */
  @Test
  public void testNamesWeighMoreAndOnlyNamesAndUrnMatchExactly() {
    QueryBuilder query = builder(null).buildQuery(OP_CONTEXT, DATASET, "orders", true);

    Map<String, Float> weights = only(find(query, SimpleQueryStringBuilder.class)).fields();
    assertTrue(
        weights.get("_search.entityName.text") > weights.get("_search.description.text"),
        weights.toString());
    assertTrue(
        weights.get("_search.description.text") > weights.get("_search.other.text"),
        weights.toString());
    assertEquals(
        find(query, TermQueryBuilder.class).stream()
            .map(TermQueryBuilder::fieldName)
            .collect(Collectors.toSet()),
        Set.of(
            "_search.entityName",
            "_search.entityName.keyword",
            "_search.qualifiedName",
            "_search.qualifiedName.keyword",
            "urn"));
    // The urn has a term per casing as well
    assertEquals(
        find(query, TermQueryBuilder.class).stream()
            .filter(term -> term.fieldName().equals("urn"))
            .map(TermQueryBuilder::caseInsensitive)
            .collect(Collectors.toSet()),
        Set.of(false, true));
    List<MatchPhrasePrefixQueryBuilder> prefixes = find(query, MatchPhrasePrefixQueryBuilder.class);
    assertFalse(prefixes.isEmpty());
    assertTrue(
        prefixes.stream()
            .allMatch(prefix -> prefix.fieldName().endsWith("." + V3SearchFields.TEXT)));
  }

  /** As on V2, an exact match in the stored casing counts more than one that ignores case. */
  @Test
  public void testExactMatchInStoredCasingCountsMore() {
    QueryBuilder query = builder(null).buildQuery(OP_CONTEXT, DATASET, "Orders", true);

    Map<String, Float> exact =
        find(query, TermQueryBuilder.class).stream()
            .collect(
                // The urn has a term per casing too
                Collectors.toMap(TermQueryBuilder::fieldName, TermQueryBuilder::boost, Float::sum));
    assertTrue(
        exact.get("_search.entityName.keyword") > exact.get("_search.entityName"),
        exact.toString());

    ExactMatchConfiguration caseInsensitive = new ExactMatchConfiguration();
    caseInsensitive.setExactFactor(
        SearchQueryBuilderTest.testQueryConfig.getExactMatch().getExactFactor());
    caseInsensitive.setCaseSensitivityFactor(0.0f);
    SearchConfiguration config = new SearchConfiguration();
    config.setExactMatch(caseInsensitive);
    config.setPartial(SearchQueryBuilderTest.testQueryConfig.getPartial());
    config.setWordGram(SearchQueryBuilderTest.testQueryConfig.getWordGram());
    QueryBuilder insensitive =
        new V3SearchQueryBuilder(config, null).buildQuery(OP_CONTEXT, DATASET, "Orders", true);
    assertTrue(
        find(insensitive, TermQueryBuilder.class).stream()
            .noneMatch(term -> term.fieldName().endsWith("." + ESUtils.KEYWORD)));
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
                                        V3SearchFields.OTHER))
                                .build())))
                .build());

    assertEquals(
        configured.searchedFields(withFieldConfiguration("nothing"), DATASET),
        configured.searchedFields(OP_CONTEXT, DATASET));
  }

  /** As on V2, the production query configuration skips the simple query for a quoted query. */
  @Test
  public void testProductionConfigurationSkipsSimpleQueryForQuotedQuery() throws Exception {
    CustomConfiguration customConfiguration = new CustomConfiguration();
    customConfiguration.setEnabled(true);
    customConfiguration.setFile("search_config.yaml");
    V3SearchQueryBuilder production = builder(customConfiguration.resolve(new YAMLMapper()));

    QueryBuilder quoted = production.buildQuery(OP_CONTEXT, DATASET, "\"orders table\"", true);
    assertTrue(find(quoted, SimpleQueryStringBuilder.class).isEmpty());
    assertFalse(find(quoted, MatchPhrasePrefixQueryBuilder.class).isEmpty());

    QueryBuilder plain = production.buildQuery(OP_CONTEXT, DATASET, "orders table", true);
    assertEquals(find(plain, SimpleQueryStringBuilder.class).size(), 1);
  }

  @Test
  public void testStructuredQueryReadsSharedFields() {
    QueryBuilder query =
        builder(null)
            .buildQuery(OP_CONTEXT, DATASET, STRUCTURED_QUERY_PREFIX + "name:orders", true);

    QueryStringQueryBuilder structured = only(find(query, QueryStringQueryBuilder.class));
    assertEquals(structured.queryString(), "name:orders");
    assertTrue(structured.fields().containsKey("_search.entityName.text"));
    assertTrue(
        structured.fields().keySet().stream().allMatch(field -> field.startsWith("_search.")),
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
    assertEquals(
        only(find(
                configured.buildQuery(withFieldConfiguration("names"), DATASET, "orders", true),
                SimpleQueryStringBuilder.class))
            .fields()
            .keySet(),
        Set.of("_search.entityName.text", "_search.entityName.stemmed"));
    Set<String> noColumns =
        configured.searchedFields(withFieldConfiguration("noColumns"), DATASET).keySet();
    assertFalse(noColumns.contains(V3SearchFields.COLUMNS), noColumns.toString());
    assertTrue(noColumns.contains(V3SearchFields.DESCRIPTION), noColumns.toString());
    // Replace cannot be combined with add: the configuration is ignored
    assertEquals(
        configured.searchedFields(withFieldConfiguration("invalid"), DATASET),
        configured.searchedFields(OP_CONTEXT, DATASET));
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

  /** The fields the full-text clauses read, leaving out the score functions. */
  private static Set<String> queriedFields(QueryBuilder query) {
    Set<String> fields = new HashSet<>();
    find(query, SimpleQueryStringBuilder.class).forEach(q -> fields.addAll(q.fields().keySet()));
    find(query, QueryStringQueryBuilder.class).forEach(q -> fields.addAll(q.fields().keySet()));
    find(query, MatchPhrasePrefixQueryBuilder.class).forEach(q -> fields.add(q.fieldName()));
    find(query, TermQueryBuilder.class).forEach(q -> fields.add(q.fieldName()));
    return fields;
  }

  private static <T extends QueryBuilder> List<T> find(QueryBuilder query, Class<T> type) {
    List<T> found = new ArrayList<>();
    if (type.isInstance(query)) {
      found.add(type.cast(query));
    } else if (query instanceof FunctionScoreQueryBuilder functionScore) {
      found.addAll(find(functionScore.query(), type));
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

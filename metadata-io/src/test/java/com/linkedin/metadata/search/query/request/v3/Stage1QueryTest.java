package com.linkedin.metadata.search.query.request.v3;

import static com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder.STRUCTURED_QUERY_PREFIX;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.TestEntitySpecBuilder;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.V3SearchQueryBuilder;
import com.linkedin.metadata.search.query.request.SearchQueryBuilderTest;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/** Stage 1 query behavior, run against the Search V3 field rewrite. */
@Import(SearchCommonTestConfiguration.class)
public class Stage1QueryTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Qualifier("queryOperationContext")
  private OperationContext operationContext;

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private final V3SearchQueryBuilder TEST_V3_BUILDER =
      new V3SearchQueryBuilder(SearchQueryBuilderTest.testQueryConfig, null);

  @Test
  public void testV3QueryUsesStage1Shape() {
    FunctionScoreQueryBuilder result =
        (FunctionScoreQueryBuilder)
            TEST_V3_BUILDER.buildQuery(
                opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), "testQuery", true);
    assertTrue(result.query() instanceof DisMaxQueryBuilder, result.query().toString());
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(result.query(), clauses);

    // OR simple queries, fuzzy except on the word gram fields
    List<SimpleQueryStringBuilder> simpleQueries =
        clauses.stream()
            .filter(SimpleQueryStringBuilder.class::isInstance)
            .map(SimpleQueryStringBuilder.class::cast)
            .collect(Collectors.toList());
    assertTrue(simpleQueries.stream().allMatch(sqs -> sqs.defaultOperator() == Operator.OR));
    assertTrue(
        simpleQueries.stream().anyMatch(sqs -> "testQuery~2".equals(sqs.value())),
        simpleQueries.toString());

    // Exact urn matches: boost 10 x exact factor 10 x 6, and x 0.7 when the case differs
    List<Float> urnTermBoosts =
        clauses.stream()
            .filter(TermQueryBuilder.class::isInstance)
            .map(TermQueryBuilder.class::cast)
            .filter(term -> term.fieldName().equals("urn"))
            .map(TermQueryBuilder::boost)
            .collect(Collectors.toList());
    assertTrue(urnTermBoosts.contains(600.0f), urnTermBoosts.toString());
    assertTrue(urnTermBoosts.stream().anyMatch(b -> Math.abs(b - 420.0f) < 0.01f));

    // The contains-wildcard searches names and titles, not the URN
    assertTrue(
        clauses.stream()
            .filter(WildcardQueryBuilder.class::isInstance)
            .map(WildcardQueryBuilder.class::cast)
            .noneMatch(w -> w.fieldName().equals("urn.delimited")));
  }

  @Test
  public void testV3WildcardSearchesNamesAndTitlesOnly() {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            operationContext,
            List.of(
                operationContext.getEntityRegistry().getEntitySpec("dataset"),
                operationContext.getEntityRegistry().getEntitySpec("dashboard")),
            "Revenue",
            true),
        clauses);
    List<WildcardQueryBuilder> wildcards =
        clauses.stream()
            .filter(WildcardQueryBuilder.class::isInstance)
            .map(WildcardQueryBuilder.class::cast)
            .collect(Collectors.toList());
    assertFalse(wildcards.isEmpty(), wildcards.toString());
    assertTrue(
        wildcards.stream()
            .allMatch(w -> w.fieldName().startsWith("name.") || w.fieldName().startsWith("title.")),
        wildcards.toString());
    // A lower-cased pattern: case-insensitive wildcards fail shards on OpenSearch 3.x
    assertTrue(
        wildcards.stream().allMatch(w -> "*revenue*".equals(w.value()) && !w.caseInsensitive()),
        wildcards.toString());
  }

  @Test
  public void testIsSameName() {
    for (String[] same :
        new String[][] {
          {"stg", "staging"},
          {"prod", "production"},
          {"dev", "development"},
          {"s3", "s_3"},
          {"data platform", "dataplatform"}
        }) {
      assertTrue(V3SearchQueryBuilder.isSameName(same[0], same[1]), String.join("/", same));
    }
    // Related names, and abbreviations too short to tell
    for (String[] related :
        new String[][] {{"glue", "athena"}, {"pg", "processing"}, {"ab", "airbyte"}}) {
      assertFalse(
          V3SearchQueryBuilder.isSameName(related[0], related[1]), String.join("/", related));
    }
  }

  @Test
  public void testV3ExactNameScoreSkipsRelatedNames() {
    // "athena" is a synonym of "glue" in the default file, but a different name
    List<QueryBuilder> glue = v3DatasetClauses("glue");
    assertEquals(exactNameConstants(glue), Set.of("glue"));
    assertTrue(
        glue.stream()
            .filter(MatchPhrasePrefixQueryBuilder.class::isInstance)
            .map(MatchPhrasePrefixQueryBuilder.class::cast)
            .noneMatch(prefix -> "athena".equals(prefix.value())));
    // A quoted query names a value, so it does not expand to its synonyms
    assertEquals(exactNameConstants(v3DatasetClauses("\"staging\"")), Set.of("staging"));
  }

  private List<QueryBuilder> v3DatasetClauses(String query) {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            operationContext,
            List.of(operationContext.getEntityRegistry().getEntitySpec("dataset")),
            query,
            true),
        clauses);
    return clauses;
  }

  private static Set<Object> exactNameConstants(List<QueryBuilder> clauses) {
    return clauses.stream()
        .filter(ConstantScoreQueryBuilder.class::isInstance)
        .map(cs -> (TermQueryBuilder) ((ConstantScoreQueryBuilder) cs).innerQuery())
        .filter(term -> term.fieldName().equals("name.keyword"))
        .map(TermQueryBuilder::value)
        .collect(Collectors.toSet());
  }

  @Test
  public void testV3ExactNameScoresAboveAnyPartialMatch() {
    EntitySpec datasetSpec = operationContext.getEntityRegistry().getEntitySpec("dataset");
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(operationContext, List.of(datasetSpec), "staging", true),
        clauses);
    List<ConstantScoreQueryBuilder> exactNames =
        clauses.stream()
            .filter(ConstantScoreQueryBuilder.class::isInstance)
            .map(ConstantScoreQueryBuilder.class::cast)
            .collect(Collectors.toList());
    assertTrue(exactNames.stream().allMatch(cs -> cs.boost() == 1000.0f));
    // The query and its synonym from the default synonym file, on the name keyword
    Set<Object> exactNameValues =
        exactNames.stream()
            .map(cs -> (TermQueryBuilder) cs.innerQuery())
            .filter(term -> term.fieldName().equals("name.keyword"))
            .map(TermQueryBuilder::value)
            .collect(Collectors.toSet());
    assertEquals(exactNameValues, Set.of("staging", "stg"));
  }

  @Test
  public void testV3MultiWordAndDottedQueriesAddBonusClauses() {
    EntitySpec datasetSpec = operationContext.getEntityRegistry().getEntitySpec("dataset");

    List<QueryBuilder> dotted = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            operationContext, List.of(datasetSpec), "my_db.sales.orders", true),
        dotted);
    assertTrue(
        dotted.stream()
            .filter(MatchQueryBuilder.class::isInstance)
            .map(MatchQueryBuilder.class::cast)
            .anyMatch(
                match ->
                    match.fieldName().equals("qualifiedName.text")
                        && match.operator() == Operator.AND
                        && match.boost() == 50.0f),
        dotted.toString());

    List<QueryBuilder> sentence = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            operationContext, List.of(datasetSpec), "orders placed by each customer", true),
        sentence);
    // Every word in the name, and every word in the description for four or more words
    assertTrue(
        sentence.stream()
            .filter(SimpleQueryStringBuilder.class::isInstance)
            .map(SimpleQueryStringBuilder.class::cast)
            .anyMatch(
                sqs ->
                    sqs.defaultOperator() == Operator.AND && sqs.fields().containsKey("name.text")),
        sentence.toString());
    assertTrue(
        sentence.stream()
            .filter(MatchQueryBuilder.class::isInstance)
            .map(MatchQueryBuilder.class::cast)
            .anyMatch(
                match ->
                    match.fieldName().equals("description.text")
                        && match.operator() == Operator.AND),
        sentence.toString());
    // Multi-word queries skip the contains wildcard
    assertTrue(sentence.stream().noneMatch(WildcardQueryBuilder.class::isInstance));
  }

  @Test
  public void testV3UrnQueryTargetsIdentityFields() {
    String urn = "urn:li:dataset:(urn:li:dataPlatform:hive,my_db.orders,PROD)";
    BoolQueryBuilder query =
        (BoolQueryBuilder)
            ((FunctionScoreQueryBuilder)
                    TEST_V3_BUILDER.buildQuery(
                        opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), urn, true))
                .query();
    assertEquals(query.minimumShouldMatch(), "1");
    // The identity query: an exact urn term among its clauses
    BoolQueryBuilder identity = (BoolQueryBuilder) query.should().get(0);
    assertTrue(
        identity.should().stream()
            .anyMatch(
                clause ->
                    clause instanceof TermQueryBuilder
                        && ((TermQueryBuilder) clause).fieldName().equals("urn")
                        && urn.equals(((TermQueryBuilder) clause).value())),
        identity.toString());
    // V2's all-terms simple query, for URNs that differ in case or are cut short, and entities that
    // reference the URN
    BoolQueryBuilder allTerms = (BoolQueryBuilder) query.should().get(1);
    assertTrue(
        allTerms.should().stream()
            .map(SimpleQueryStringBuilder.class::cast)
            .allMatch(sqs -> sqs.defaultOperator() == Operator.AND),
        allTerms.toString());
  }

  @Test
  public void testV3CustomBoolQueryWrapsStage1AndIdentityQueries() throws IOException {
    CustomSearchConfiguration config =
        new YAMLMapper()
            .readValue(
                """
                queryConfigurations:
                  - queryRegex: .*
                    simpleQuery: true
                    prefixMatchQuery: true
                    exactMatchQuery: true
                    boolQuery:
                      must_not:
                        - term:
                            deprecated: true
                """,
                CustomSearchConfiguration.class);
    V3SearchQueryBuilder builder =
        new V3SearchQueryBuilder(SearchQueryBuilderTest.testQueryConfig, config);
    for (String query :
        List.of("orders", "urn:li:dataset:(urn:li:dataPlatform:hive,my_db.orders,PROD)")) {
      BoolQueryBuilder root =
          (BoolQueryBuilder)
              ((FunctionScoreQueryBuilder)
                      builder.buildQuery(
                          opContext,
                          ImmutableList.of(TestEntitySpecBuilder.getSpec()),
                          query,
                          true))
                  .query();
      assertEquals(root.mustNot().size(), 1, query);
      assertEquals(root.must().size(), 1, query);
    }
    // The light query keeps the wrapper too
    BoolQueryBuilder lightRoot =
        (BoolQueryBuilder)
            ((FunctionScoreQueryBuilder)
                    builder.buildQuery(
                        opContext,
                        ImmutableList.of(TestEntitySpecBuilder.getSpec()),
                        "orders",
                        true,
                        true))
                .query();
    assertEquals(lightRoot.mustNot().size(), 1);
    assertEquals(lightRoot.must().size(), 1);
  }

  @Test
  public void testV3CustomConfigWithoutTextMatchesAddsNoBonusClauses() throws IOException {
    CustomSearchConfiguration config =
        new YAMLMapper()
            .readValue(
                """
                queryConfigurations:
                  - queryRegex: .*
                    simpleQuery: false
                    prefixMatchQuery: false
                    exactMatchQuery: false
                """,
                CustomSearchConfiguration.class);
    V3SearchQueryBuilder builder =
        new V3SearchQueryBuilder(SearchQueryBuilderTest.testQueryConfig, config);
    EntitySpec datasetSpec = operationContext.getEntityRegistry().getEntitySpec("dataset");
    for (String query :
        List.of("orders", "orders2017", "my_db.sales.orders", "orders placed by each customer")) {
      List<QueryBuilder> clauses = new ArrayList<>();
      collectClauses(
          builder.buildQuery(operationContext, List.of(datasetSpec), query, true), clauses);
      assertTrue(
          clauses.stream()
              .noneMatch(
                  clause ->
                      clause instanceof WildcardQueryBuilder
                          || clause instanceof MatchQueryBuilder
                          || clause instanceof MultiMatchQueryBuilder
                          || clause instanceof SimpleQueryStringBuilder
                          || clause instanceof TermQueryBuilder),
          query + ": " + clauses);
      // No light query either, so the full query serves the search
      assertNull(builder.buildQuery(operationContext, List.of(datasetSpec), query, true, true));
    }
  }

  @Test
  public void testV3UnsplitQueryOnlyForLetterDigitRuns() {
    // "orders2017" also matches as the one token the analyzers index; an escaped operator does
    // not change any token, so it adds no copy
    assertTrue(v3SimpleQueryCount("orders2017") > v3SimpleQueryCount("orders 2017"));
    assertEquals(v3SimpleQueryCount("revenue -archive"), v3SimpleQueryCount("revenue archive"));
  }

  @Test
  public void testV3LongQueriesStayUnderTheClauseLimit() {
    EntitySpec datasetSpec = operationContext.getEntityRegistry().getEntitySpec("dataset");
    // One word gets every expansion; more words share a budget of expanded terms
    assertTrue(
        v3SimpleQueries(datasetSpec, "revenue").stream()
            .anyMatch(sqs -> sqs.value().contains("~") && sqs.fuzzyMaxExpansions() == 10));
    // Past the budget the words are matched without fuzziness. Shared fields are fewer than the
    // old per-field subfields, so the budget is reached with a longer query.
    StringBuilder longQuery = new StringBuilder();
    for (int i = 0; i < 55; i++) {
      if (i > 0) {
        longQuery.append(' ');
      }
      longQuery.append("w").append(i);
    }
    List<SimpleQueryStringBuilder> many = v3SimpleQueries(datasetSpec, longQuery.toString());
    assertTrue(many.stream().anyMatch(sqs -> sqs.value().contains("w0")));
    assertTrue(many.stream().noneMatch(sqs -> sqs.value().contains("w54")));
    assertTrue(many.stream().noneMatch(sqs -> sqs.value().contains("~")));
  }

  @Test
  public void testV3PhrasePrefixesExpandLessForSeveralTerms() {
    EntitySpec datasetSpec = operationContext.getEntityRegistry().getEntitySpec("dataset");
    // A single term keeps the default: for a word too short for fuzziness and the wildcard, the
    // prefix is its only partial match. "stg" also prefix-matches its synonym "staging"
    assertEquals(v3PhrasePrefixExpansions(datasetSpec, "stg"), Set.of(50));
    // Several terms take most of the clause budget
    assertEquals(v3PhrasePrefixExpansions(datasetSpec, "stg orders"), Set.of(10));
  }

  private Set<Integer> v3PhrasePrefixExpansions(EntitySpec spec, String query) {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(operationContext, List.of(spec), query, true), clauses);
    return clauses.stream()
        .filter(MatchPhrasePrefixQueryBuilder.class::isInstance)
        .map(prefix -> ((MatchPhrasePrefixQueryBuilder) prefix).maxExpansions())
        .collect(Collectors.toSet());
  }

  private List<SimpleQueryStringBuilder> v3SimpleQueries(EntitySpec spec, String query) {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(operationContext, List.of(spec), query, true), clauses);
    return clauses.stream()
        .filter(SimpleQueryStringBuilder.class::isInstance)
        .map(SimpleQueryStringBuilder.class::cast)
        .collect(Collectors.toList());
  }

  private long v3SimpleQueryCount(String query) {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), query, true),
        clauses);
    return clauses.stream().filter(SimpleQueryStringBuilder.class::isInstance).count();
  }

  @Test(expectedExceptions = ValidationException.class)
  public void testV3ValidatesUrnQueries() {
    // Urn queries skip the general query, so validation must run before the dispatch
    TEST_V3_BUILDER.buildQuery(
        opContext,
        ImmutableList.of(TestEntitySpecBuilder.getSpec()),
        "urn:li:java.lang.Runtime",
        true);
  }

  @Test
  public void testV3StructuredQuery() {
    QueryBuilder query =
        ((FunctionScoreQueryBuilder)
                TEST_V3_BUILDER.buildQuery(
                    opContext,
                    ImmutableList.of(TestEntitySpecBuilder.getSpec()),
                    STRUCTURED_QUERY_PREFIX + "keyPart1:value",
                    true))
            .query();
    DisMaxQueryBuilder disMax = (DisMaxQueryBuilder) query;
    QueryStringQueryBuilder structured = (QueryStringQueryBuilder) disMax.innerQueries().get(0);
    assertEquals(structured.queryString(), "keyPart1:value");
    assertEquals(structured.fields().get("keyPart1").floatValue(), 10.0f);
  }

  @Test
  public void testV3QuotedQueryRunsNoFuzzyMatch() {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), "\"test query\"", true),
        clauses);
    assertTrue(
        clauses.stream()
            .filter(SimpleQueryStringBuilder.class::isInstance)
            .map(SimpleQueryStringBuilder.class::cast)
            .noneMatch(sqs -> sqs.value().contains("~")),
        clauses.toString());
    // Nor a substring match of a quoted word
    List<QueryBuilder> quotedWord = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), "\"testQuery\"", true),
        quotedWord);
    assertTrue(quotedWord.stream().noneMatch(WildcardQueryBuilder.class::isInstance));
  }

  @Test
  public void testSplitAlphanumericTokens() {
    assertEquals(V3SearchQueryBuilder.splitAlphanumericTokens("hello"), "hello");
    assertEquals(V3SearchQueryBuilder.splitAlphanumericTokens("2017"), "2017");
    assertEquals(V3SearchQueryBuilder.splitAlphanumericTokens("orders2017"), "orders 2017");
    assertEquals(V3SearchQueryBuilder.splitAlphanumericTokens("2023table"), "2023 table");
    assertEquals(V3SearchQueryBuilder.splitAlphanumericTokens("abc123def"), "abc 123 def");
    assertEquals(
        V3SearchQueryBuilder.splitAlphanumericTokens("hello table2023 world"),
        "hello table 2023 world");
    assertEquals(V3SearchQueryBuilder.splitAlphanumericTokens(""), "");
  }

  @Test
  public void testEscapeSimpleQueryStringOperators() {
    // Hyphen replaced with space (prevents NOT operator)
    assertEquals(
        V3SearchQueryBuilder.escapeSimpleQueryStringOperators("user-interaction"),
        "user interaction");
    assertEquals(V3SearchQueryBuilder.escapeSimpleQueryStringOperators("user~5"), "user 5");
    assertEquals(V3SearchQueryBuilder.escapeSimpleQueryStringOperators("(user)"), " user ");
    assertEquals(V3SearchQueryBuilder.escapeSimpleQueryStringOperators("pre*"), "pre ");
    // Bare "*" preserved for browse-all, double quotes for phrase matching
    assertEquals(V3SearchQueryBuilder.escapeSimpleQueryStringOperators(" * "), " * ");
    assertEquals(
        V3SearchQueryBuilder.escapeSimpleQueryStringOperators("\"exact phrase\""),
        "\"exact phrase\"");
  }

  @Test
  public void testMakeFuzzyQuery() {
    // Up to 4 characters: no fuzzy, short acronyms are too easily corrupted
    assertEquals(V3SearchQueryBuilder.makeFuzzyQuery("etl"), "etl");
    assertEquals(V3SearchQueryBuilder.makeFuzzyQuery("gdpr"), "gdpr");
    // 5-6 characters: one edit; 7+: two
    assertEquals(V3SearchQueryBuilder.makeFuzzyQuery("alert"), "alert~1");
    assertEquals(V3SearchQueryBuilder.makeFuzzyQuery("revenue"), "revenue~2");
    // Each underscore or hyphen separated term gets its own distance
    assertEquals(V3SearchQueryBuilder.makeFuzzyQuery("user_facts"), "user facts~1");
    assertEquals(V3SearchQueryBuilder.makeFuzzyQuery("active-users"), "active~1 users~1");
  }

  @Test
  public void testV3LightQuerySkipsExpensiveClauses() {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), "testQuery", true, true),
        clauses);
    assertTrue(clauses.stream().noneMatch(WildcardQueryBuilder.class::isInstance));
    assertTrue(clauses.stream().noneMatch(SimpleQueryStringBuilder.class::isInstance));
    // A single word searches the name fields and structured property values
    MultiMatchQueryBuilder multiMatch =
        clauses.stream()
            .filter(MultiMatchQueryBuilder.class::isInstance)
            .map(MultiMatchQueryBuilder.class::cast)
            .findFirst()
            .orElseThrow();
    assertEquals(multiMatch.type(), MultiMatchQueryBuilder.Type.BEST_FIELDS);
    assertEquals(multiMatch.fields().keySet(), Set.of("name.text", "name.stemmed"));
    // Exact names still score a constant above every partial match
    assertTrue(
        clauses.stream()
            .filter(TermQueryBuilder.class::isInstance)
            .map(TermQueryBuilder.class::cast)
            .anyMatch(term -> term.fieldName().equals("name.keyword") && term.boost() == 1000.0f));
  }

  @Test
  public void testV3LightQueryForKeywordsSearchesEveryField() {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            opContext,
            ImmutableList.of(TestEntitySpecBuilder.getSpec()),
            "test query words",
            true,
            true),
        clauses);
    Set<String> fields =
        clauses.stream()
            .filter(MultiMatchQueryBuilder.class::isInstance)
            .map(MultiMatchQueryBuilder.class::cast)
            .flatMap(multiMatch -> multiMatch.fields().keySet().stream())
            .collect(Collectors.toSet());
    assertEquals(fields, Set.of("name.text", "name.stemmed", "other.text", "other.stemmed"));
  }

  @Test
  public void testV3LightQueryForDeepFqnRequiresEveryToken() {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            opContext,
            ImmutableList.of(TestEntitySpecBuilder.getSpec()),
            "my_db.sales.orders",
            true,
            true),
        clauses);
    assertTrue(
        clauses.stream()
            .filter(MultiMatchQueryBuilder.class::isInstance)
            .map(MultiMatchQueryBuilder.class::cast)
            .anyMatch(multiMatch -> multiMatch.operator() == Operator.AND),
        clauses.toString());
  }

  @Test
  public void testV3LightQueryExpandsSynonymsOfExactNames() {
    EntitySpec datasetSpec = operationContext.getEntityRegistry().getEntitySpec("dataset");
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(operationContext, List.of(datasetSpec), "staging", true, true),
        clauses);
    assertTrue(
        clauses.stream()
            .filter(ConstantScoreQueryBuilder.class::isInstance)
            .map(cs -> (TermQueryBuilder) ((ConstantScoreQueryBuilder) cs).innerQuery())
            .anyMatch(
                term -> term.fieldName().equals("name.keyword") && "stg".equals(term.value())),
        clauses.toString());
  }

  @Test
  public void testV3LightQueryScoresOnlySameNameSynonymsAsExact() {
    // "athena" is a synonym of "glue" but a different name: no exact-name score, neither the
    // constant nor the synonym multi_match's exact-name terms
    List<QueryBuilder> glue = v3LightDatasetClauses("glue");
    assertEquals(exactNameConstants(glue), Set.of("glue"));
    assertTrue(
        glue.stream()
            .filter(TermQueryBuilder.class::isInstance)
            .map(TermQueryBuilder.class::cast)
            .noneMatch(term -> "athena".equals(term.value())),
        glue.toString());
    // A quoted query names a value, so it does not expand to its synonyms
    assertEquals(exactNameConstants(v3LightDatasetClauses("\"staging\"")), Set.of("staging"));
  }

  private List<QueryBuilder> v3LightDatasetClauses(String query) {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            operationContext,
            List.of(operationContext.getEntityRegistry().getEntitySpec("dataset")),
            query,
            true,
            true),
        clauses);
    return clauses;
  }

  @Test
  public void testV3LightQueryKeepsSplitWordsWhole() {
    // "cargo2017" splits into "cargo 2017": no multi_match takes a name holding only one part,
    // and the whole run is re-queried on the identity fields
    List<MultiMatchQueryBuilder> split = v3LightMultiMatches("cargo2017");
    assertEquals(split.size(), 1, split.toString());
    assertEquals(split.get(0).value(), "cargo2017");
    assertEquals(split.get(0).operator(), Operator.AND);
    // Escaping alone keeps matching any part, and re-queries the hyphenated identifier whole
    List<MultiMatchQueryBuilder> escaped = v3LightMultiMatches("load-job");
    assertTrue(
        escaped.stream().anyMatch(m -> "load job".equals(m.value()) && m.operator() == Operator.OR),
        escaped.toString());
    assertTrue(
        escaped.stream()
            .anyMatch(m -> "load-job".equals(m.value()) && m.operator() == Operator.AND),
        escaped.toString());
    // An unchanged word adds no re-query
    assertEquals(v3LightMultiMatches("cargo").size(), 1);
  }

  @Test
  public void testV3LightQueryRequeriesEveryNameField() {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            operationContext,
            List.of(
                operationContext.getEntityRegistry().getEntitySpec("dataset"),
                operationContext.getEntityRegistry().getEntitySpec("corpuser")),
            "cargo2017",
            true,
            true),
        clauses);
    Set<String> fields =
        clauses.stream()
            .filter(MultiMatchQueryBuilder.class::isInstance)
            .map(MultiMatchQueryBuilder.class::cast)
            .filter(multiMatch -> multiMatch.operator() == Operator.AND)
            .flatMap(multiMatch -> multiMatch.fields().keySet().stream())
            .collect(Collectors.toSet());
    assertTrue(fields.containsAll(Set.of("name.text", "qualifiedName.text")), fields.toString());
  }

  @Test
  public void testV3LightQueryForLongExactNameRequiresEveryToken() {
    // Five parts make a long name, every part required; a leading delimiter adds no part
    assertTrue(
        v3LightMultiMatches("aa_bb_cc_dd_ee").stream()
            .anyMatch(multiMatch -> multiMatch.operator() == Operator.AND));
    assertTrue(
        v3LightMultiMatches("_aa_bb_cc_dd").stream()
            .noneMatch(multiMatch -> multiMatch.operator() == Operator.AND));
  }

  @Test
  public void testV3LightExactNameIsTheQueryAsTyped() {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), "load-job", true, true),
        clauses);
    // Not "load job", which would make a name "Load Job" an exact match
    assertEquals(
        clauses.stream()
            .filter(TermQueryBuilder.class::isInstance)
            .map(TermQueryBuilder.class::cast)
            .filter(term -> term.fieldName().equals("name.keyword"))
            .map(TermQueryBuilder::value)
            .collect(Collectors.toSet()),
        Set.of("load-job"));
  }

  @Test
  public void testV3LightQueryCutsLongQueriesAtAWordBoundary() {
    // The light query matches the first 80 characters of a longer query, cut between words
    String query =
        "quarterly revenue forecast by region and product line for the north american sales team";
    List<MultiMatchQueryBuilder> multiMatches = v3LightMultiMatches(query);
    assertFalse(multiMatches.isEmpty());
    for (MultiMatchQueryBuilder multiMatch : multiMatches) {
      String value = String.valueOf(multiMatch.value());
      assertTrue(value.length() <= 80 && query.startsWith(value + " "), value);
    }
    // A word longer than that is cut at 80 characters
    assertTrue(
        v3LightMultiMatches("x".repeat(90)).stream()
            .allMatch(multiMatch -> String.valueOf(multiMatch.value()).equals("x".repeat(80))));
  }

  private List<MultiMatchQueryBuilder> v3LightMultiMatches(String query) {
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            opContext, ImmutableList.of(TestEntitySpecBuilder.getSpec()), query, true, true),
        clauses);
    return clauses.stream()
        .filter(MultiMatchQueryBuilder.class::isInstance)
        .map(MultiMatchQueryBuilder.class::cast)
        .collect(Collectors.toList());
  }

  @Test
  public void testV3LightQuerySearchesDocumentBodies() {
    EntitySpec documentSpec = operationContext.getEntityRegistry().getEntitySpec("document");
    List<QueryBuilder> clauses = new ArrayList<>();
    collectClauses(
        TEST_V3_BUILDER.buildQuery(
            operationContext, List.of(documentSpec), "quarterly revenue", true, true),
        clauses);
    assertTrue(
        clauses.stream()
            .filter(MultiMatchQueryBuilder.class::isInstance)
            .map(MultiMatchQueryBuilder.class::cast)
            .anyMatch(
                multiMatch ->
                    multiMatch.fields().keySet().stream()
                        .anyMatch(field -> "name.text".equals(field))),
        clauses.toString());
  }

  /** Collects every clause of a built query tree, descending into compound queries. */
  private static void collectClauses(QueryBuilder query, List<QueryBuilder> out) {
    out.add(query);
    List<QueryBuilder> children = new ArrayList<>();
    if (query instanceof FunctionScoreQueryBuilder) {
      children.add(((FunctionScoreQueryBuilder) query).query());
    } else if (query instanceof BoolQueryBuilder) {
      BoolQueryBuilder bool = (BoolQueryBuilder) query;
      children.addAll(bool.must());
      children.addAll(bool.should());
      children.addAll(bool.filter());
    } else if (query instanceof DisMaxQueryBuilder) {
      children.addAll(((DisMaxQueryBuilder) query).innerQueries());
    }
    children.forEach(child -> collectClauses(child, out));
  }
}

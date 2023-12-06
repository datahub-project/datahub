package com.linkedin.metadata.search.fixtures;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_JOB_ENTITY_NAME;
import static com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder.STRUCTURED_QUERY_PREFIX;
import static com.linkedin.metadata.utils.SearchUtil.AGGREGATION_SEPARATOR_CHAR;
import static io.datahubproject.test.search.SearchTestUtils.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.types.chart.ChartType;
import com.linkedin.datahub.graphql.types.container.ContainerType;
import com.linkedin.datahub.graphql.types.corpgroup.CorpGroupType;
import com.linkedin.datahub.graphql.types.corpuser.CorpUserType;
import com.linkedin.datahub.graphql.types.dataset.DatasetType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.r2.RemoteInvocationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.junit.Assert;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.AnalyzeRequest;
import org.opensearch.client.indices.AnalyzeResponse;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public abstract class SampleDataFixtureTestBase extends AbstractTestNGSpringContextTests {
  protected static final Authentication AUTHENTICATION =
      new Authentication(new Actor(ActorType.USER, "test"), "");

  @Nonnull
  protected abstract EntityRegistry getEntityRegistry();

  @Nonnull
  protected abstract SearchService getSearchService();

  @Nonnull
  protected abstract EntityClient getEntityClient();

  @Nonnull
  protected abstract RestHighLevelClient getSearchClient();

  @Test
  public void testSearchFieldConfig() throws IOException {
    /*
     For every field in every entity fixture, ensure proper detection of field types and analyzers
    */
    Map<EntitySpec, String> fixtureEntities = new HashMap<>();
    fixtureEntities.put(getEntityRegistry().getEntitySpec("dataset"), "smpldat_datasetindex_v2");
    fixtureEntities.put(getEntityRegistry().getEntitySpec("chart"), "smpldat_chartindex_v2");
    fixtureEntities.put(
        getEntityRegistry().getEntitySpec("container"), "smpldat_containerindex_v2");
    fixtureEntities.put(
        getEntityRegistry().getEntitySpec("corpgroup"), "smpldat_corpgroupindex_v2");
    fixtureEntities.put(getEntityRegistry().getEntitySpec("corpuser"), "smpldat_corpuserindex_v2");
    fixtureEntities.put(
        getEntityRegistry().getEntitySpec("dashboard"), "smpldat_dashboardindex_v2");
    fixtureEntities.put(getEntityRegistry().getEntitySpec("dataflow"), "smpldat_dataflowindex_v2");
    fixtureEntities.put(getEntityRegistry().getEntitySpec("datajob"), "smpldat_datajobindex_v2");
    fixtureEntities.put(getEntityRegistry().getEntitySpec("domain"), "smpldat_domainindex_v2");
    fixtureEntities.put(
        getEntityRegistry().getEntitySpec("glossarynode"), "smpldat_glossarynodeindex_v2");
    fixtureEntities.put(
        getEntityRegistry().getEntitySpec("glossaryterm"), "smpldat_glossarytermindex_v2");
    fixtureEntities.put(
        getEntityRegistry().getEntitySpec("mlfeature"), "smpldat_mlfeatureindex_v2");
    fixtureEntities.put(
        getEntityRegistry().getEntitySpec("mlfeaturetable"), "smpldat_mlfeaturetableindex_v2");
    fixtureEntities.put(
        getEntityRegistry().getEntitySpec("mlmodelgroup"), "smpldat_mlmodelgroupindex_v2");
    fixtureEntities.put(getEntityRegistry().getEntitySpec("mlmodel"), "smpldat_mlmodelindex_v2");
    fixtureEntities.put(
        getEntityRegistry().getEntitySpec("mlprimarykey"), "smpldat_mlprimarykeyindex_v2");
    fixtureEntities.put(getEntityRegistry().getEntitySpec("tag"), "smpldat_tagindex_v2");

    for (Map.Entry<EntitySpec, String> entry : fixtureEntities.entrySet()) {
      EntitySpec entitySpec = entry.getKey();
      GetMappingsRequest req = new GetMappingsRequest().indices(entry.getValue());

      GetMappingsResponse resp =
          getSearchClient().indices().getMapping(req, RequestOptions.DEFAULT);
      Map<String, Map<String, Object>> mappings =
          (Map<String, Map<String, Object>>)
              resp.mappings().get(entry.getValue()).sourceAsMap().get("properties");

      // For every fieldSpec determine whether the SearchFieldConfig is accurate
      for (SearchableFieldSpec fieldSpec : entitySpec.getSearchableFieldSpecs()) {
        SearchFieldConfig test = SearchFieldConfig.detectSubFieldType(fieldSpec);

        if (!test.fieldName().contains(".")) {
          Map<String, Object> actual = mappings.get(test.fieldName());

          final String expectedAnalyzer;
          if (actual.get("search_analyzer") != null) {
            expectedAnalyzer = (String) actual.get("search_analyzer");
          } else if (actual.get("analyzer") != null) {
            expectedAnalyzer = (String) actual.get("analyzer");
          } else {
            expectedAnalyzer = "keyword";
          }

          assertEquals(
              test.analyzer(),
              expectedAnalyzer,
              String.format(
                  "Expected search analyzer to match for entity: `%s`field: `%s`",
                  entitySpec.getName(), test.fieldName()));

          if (test.hasDelimitedSubfield()) {
            assertTrue(
                ((Map<String, Map<String, String>>) actual.get("fields")).containsKey("delimited"),
                String.format(
                    "Expected entity: `%s` field to have .delimited subfield: `%s`",
                    entitySpec.getName(), test.fieldName()));
          } else {
            boolean nosubfield =
                !actual.containsKey("fields")
                    || !((Map<String, Map<String, String>>) actual.get("fields"))
                        .containsKey("delimited");
            assertTrue(
                nosubfield,
                String.format(
                    "Expected entity: `%s` field to NOT have .delimited subfield: `%s`",
                    entitySpec.getName(), test.fieldName()));
          }
          if (test.hasKeywordSubfield()) {
            assertTrue(
                ((Map<String, Map<String, String>>) actual.get("fields")).containsKey("keyword"),
                String.format(
                    "Expected entity: `%s` field to have .keyword subfield: `%s`",
                    entitySpec.getName(), test.fieldName()));
          } else {
            boolean nosubfield =
                !actual.containsKey("fields")
                    || !((Map<String, Map<String, String>>) actual.get("fields"))
                        .containsKey("keyword");
            assertTrue(
                nosubfield,
                String.format(
                    "Expected entity: `%s` field to NOT have .keyword subfield: `%s`",
                    entitySpec.getName(), test.fieldName()));
          }
        } else {
          // this is a subfield therefore cannot have a subfield
          assertFalse(test.hasKeywordSubfield());
          assertFalse(test.hasDelimitedSubfield());
          assertFalse(test.hasWordGramSubfields());

          String[] fieldAndSubfield = test.fieldName().split("[.]", 2);

          Map<String, Object> actualParent = mappings.get(fieldAndSubfield[0]);
          Map<String, Object> actualSubfield =
              ((Map<String, Map<String, Object>>) actualParent.get("fields"))
                  .get(fieldAndSubfield[0]);

          String expectedAnalyzer =
              actualSubfield.get("search_analyzer") != null
                  ? (String) actualSubfield.get("search_analyzer")
                  : "keyword";

          assertEquals(
              test.analyzer(),
              expectedAnalyzer,
              String.format("Expected search analyzer to match for field `%s`", test.fieldName()));
        }
      }
    }
  }

  @Test
  public void testGetSortOrder() {
    String dateFieldName = "lastOperationTime";
    List<String> entityNamesToTestSearch = List.of("dataset", "chart", "corpgroup");
    List<EntitySpec> entitySpecs =
        entityNamesToTestSearch.stream()
            .map(name -> getEntityRegistry().getEntitySpec(name))
            .collect(Collectors.toList());
    SearchSourceBuilder builder = new SearchSourceBuilder();
    SortCriterion sortCriterion =
        new SortCriterion().setOrder(SortOrder.DESCENDING).setField(dateFieldName);
    ESUtils.buildSortOrder(builder, sortCriterion, entitySpecs);
    List<SortBuilder<?>> sorts = builder.sorts();
    assertEquals(sorts.size(), 2); // sort by last modified and then by urn
    for (SortBuilder sort : sorts) {
      assertTrue(sort instanceof FieldSortBuilder);
      FieldSortBuilder fieldSortBuilder = (FieldSortBuilder) sort;
      if (fieldSortBuilder.getFieldName().equals(dateFieldName)) {
        assertEquals(fieldSortBuilder.order(), org.opensearch.search.sort.SortOrder.DESC);
        assertEquals(fieldSortBuilder.unmappedType(), "date");
      } else {
        assertEquals(fieldSortBuilder.getFieldName(), "urn");
      }
    }

    // Test alias field
    String entityNameField = "_entityName";
    SearchSourceBuilder nameBuilder = new SearchSourceBuilder();
    SortCriterion nameCriterion =
        new SortCriterion().setOrder(SortOrder.ASCENDING).setField(entityNameField);
    ESUtils.buildSortOrder(nameBuilder, nameCriterion, entitySpecs);
    sorts = nameBuilder.sorts();
    assertEquals(sorts.size(), 2);
    for (SortBuilder sort : sorts) {
      assertTrue(sort instanceof FieldSortBuilder);
      FieldSortBuilder fieldSortBuilder = (FieldSortBuilder) sort;
      if (fieldSortBuilder.getFieldName().equals(entityNameField)) {
        assertEquals(fieldSortBuilder.order(), org.opensearch.search.sort.SortOrder.ASC);
        assertEquals(fieldSortBuilder.unmappedType(), "keyword");
      } else {
        assertEquals(fieldSortBuilder.getFieldName(), "urn");
      }
    }
  }

  @Test
  public void testDatasetHasTags() throws IOException {
    GetMappingsRequest req = new GetMappingsRequest().indices("smpldat_datasetindex_v2");
    GetMappingsResponse resp = getSearchClient().indices().getMapping(req, RequestOptions.DEFAULT);
    Map<String, Map<String, String>> mappings =
        (Map<String, Map<String, String>>)
            resp.mappings().get("smpldat_datasetindex_v2").sourceAsMap().get("properties");
    assertTrue(mappings.containsKey("hasTags"));
    assertEquals(mappings.get("hasTags"), Map.of("type", "boolean"));
  }

  @Test
  public void testFixtureInitialization() {
    assertNotNull(getSearchService());
    SearchResult noResult = searchAcrossEntities(getSearchService(), "no results");
    assertEquals(0, noResult.getEntities().size());

    final SearchResult result = searchAcrossEntities(getSearchService(), "test");

    Map<String, Integer> expectedTypes =
        Map.of(
            "dataset", 13,
            "chart", 0,
            "container", 1,
            "dashboard", 0,
            "tag", 0,
            "mlmodel", 0);

    Map<String, List<Urn>> actualTypes = new HashMap<>();
    for (String key : expectedTypes.keySet()) {
      actualTypes.put(
          key,
          result.getEntities().stream()
              .map(SearchEntity::getEntity)
              .filter(entity -> key.equals(entity.getEntityType()))
              .collect(Collectors.toList()));
    }

    expectedTypes.forEach(
        (key, value) ->
            assertEquals(
                actualTypes.get(key).size(),
                value.intValue(),
                String.format(
                    "Expected entity `%s` matches for %s. Found %s",
                    value,
                    key,
                    result.getEntities().stream()
                        .filter(e -> e.getEntity().getEntityType().equals(key))
                        .map(e -> e.getEntity().getEntityKey())
                        .collect(Collectors.toList()))));
  }

  @Test
  public void testDataPlatform() {
    Map<String, Integer> expected =
        ImmutableMap.<String, Integer>builder()
            .put("urn:li:dataPlatform:BigQuery", 8)
            .put("urn:li:dataPlatform:hive", 3)
            .put("urn:li:dataPlatform:mysql", 5)
            .put("urn:li:dataPlatform:s3", 1)
            .put("urn:li:dataPlatform:hdfs", 1)
            .put("urn:li:dataPlatform:graph", 1)
            .put("urn:li:dataPlatform:dbt", 9)
            .put("urn:li:dataplatform:BigQuery", 8)
            .put("urn:li:dataplatform:hive", 3)
            .put("urn:li:dataplatform:mysql", 5)
            .put("urn:li:dataplatform:s3", 1)
            .put("urn:li:dataplatform:hdfs", 1)
            .put("urn:li:dataplatform:graph", 1)
            .put("urn:li:dataplatform:dbt", 9)
            .build();

    expected.forEach(
        (key, value) -> {
          SearchResult result = searchAcrossEntities(getSearchService(), key);
          assertEquals(
              result.getEntities().size(),
              value.intValue(),
              String.format(
                  "Unexpected data platform `%s` hits.", key)); // max is 100 without pagination
        });
  }

  @Test
  public void testUrn() {
    List.of(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,harshal-playground-306419.test_schema.austin311_derived,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:graph,graph-test,PROD)",
            "urn:li:chart:(looker,baz1)",
            "urn:li:dashboard:(looker,baz)",
            "urn:li:mlFeature:(test_feature_table_all_feature_dtypes,test_BOOL_LIST_feature)",
            "urn:li:mlModel:(urn:li:dataPlatform:science,scienceModel,PROD)")
        .forEach(
            query ->
                assertTrue(
                    searchAcrossEntities(getSearchService(), query).getEntities().size() >= 1,
                    String.format("Unexpected >1 urn result for `%s`", query)));
  }

  @Test
  public void testExactTable() {
    SearchResult results = searchAcrossEntities(getSearchService(), "stg_customers");
    assertEquals(
        results.getEntities().size(), 1, "Unexpected single urn result for `stg_customers`");
    assertEquals(
        results.getEntities().get(0).getEntity().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.stg_customers,PROD)");
  }

  @Test
  public void testStemming() {
    List<Set<String>> testSets =
        List.of(
            Set.of("log", "logs", "logging"),
            Set.of("border", "borders", "bordered", "bordering"),
            Set.of("indicates", "indicate", "indicated"));

    testSets.forEach(
        testSet -> {
          Integer expectedResults = null;
          for (String testQuery : testSet) {
            SearchResult results = searchAcrossEntities(getSearchService(), testQuery);

            assertTrue(
                results.hasEntities() && !results.getEntities().isEmpty(),
                String.format("Expected search results for `%s`", testQuery));
            if (expectedResults == null) {
              expectedResults = results.getNumEntities();
            }
            assertEquals(
                expectedResults,
                results.getNumEntities(),
                String.format("Expected all result counts to match after stemming. %s", testSet));
          }
        });
  }

  @Test
  public void testStemmingOverride() throws IOException {
    Set<String> testSet = Set.of("customer", "customers");

    Set<SearchResult> results =
        testSet.stream()
            .map(test -> searchAcrossEntities(getSearchService(), test))
            .collect(Collectors.toSet());

    results.forEach(
        r -> assertTrue(r.hasEntities() && !r.getEntities().isEmpty(), "Expected search results"));
    assertEquals(
        results.stream().map(r -> r.getEntities().size()).distinct().count(),
        1,
        String.format("Expected all result counts to match after stemming. %s", testSet));

    // Additional inspect token
    AnalyzeRequest request =
        AnalyzeRequest.withIndexAnalyzer("smpldat_datasetindex_v2", "word_delimited", "customers");

    List<String> tokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(tokens, List.of("customer"), "Expected `customer` and not `custom`");
  }

  @Test
  public void testDelimitedSynonym() throws IOException {
    List<String> expectedTokens = List.of("cac");
    List<String> analyzers =
        List.of("urn_component", "word_delimited", "query_urn_component", "query_word_delimited");
    List<String> testTexts =
        List.of(
            "customer acquisition cost",
            "cac",
            "urn:li:dataset:(urn:li:dataPlatform:testsynonym,cac_table,TEST)");

    for (String analyzer : analyzers) {
      for (String text : testTexts) {
        AnalyzeRequest request =
            AnalyzeRequest.withIndexAnalyzer("smpldat_datasetindex_v2", analyzer, text);
        List<String> tokens =
            getTokens(request)
                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                .collect(Collectors.toList());
        expectedTokens.forEach(
            expected ->
                assertTrue(
                    tokens.contains(expected),
                    String.format(
                        "Analyzer: `%s` Text: `%s` - Expected token `%s` in tokens: %s",
                        analyzer, text, expected, tokens)));
      }
    }

    // {"urn":"urn:li:dataset:(urn:li:dataPlatform:testsynonym,cac_table,TEST)","id":"cac_table",...
    List<String> testSet = List.of("cac", "customer acquisition cost");
    List<Integer> resultCounts =
        testSet.stream()
            .map(
                q -> {
                  SearchResult result = searchAcrossEntities(getSearchService(), q);
                  assertTrue(
                      result.hasEntities() && !result.getEntities().isEmpty(),
                      "Expected search results for: " + q);
                  return result.getEntities().size();
                })
            .collect(Collectors.toList());
  }

  @Test
  public void testNegateAnalysis() throws IOException {
    String queryWithMinus = "logging_events -bckp";
    AnalyzeRequest request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2", "query_word_delimited", queryWithMinus);
    assertEquals(
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList()),
        List.of("logging_events -bckp", "logging_ev", "-bckp", "log", "event", "bckp"));

    request =
        AnalyzeRequest.withIndexAnalyzer("smpldat_datasetindex_v2", "word_gram_3", queryWithMinus);
    assertEquals(
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList()),
        List.of("logging events -bckp"));

    request =
        AnalyzeRequest.withIndexAnalyzer("smpldat_datasetindex_v2", "word_gram_4", queryWithMinus);
    assertEquals(
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList()),
        List.of());
  }

  @Test
  public void testWordGram() throws IOException {
    String text = "hello.cat_cool_customer";
    AnalyzeRequest request =
        AnalyzeRequest.withIndexAnalyzer("smpldat_datasetindex_v2", "word_gram_2", text);
    assertEquals(
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList()),
        List.of("hello cat", "cat cool", "cool customer"));
    request = AnalyzeRequest.withIndexAnalyzer("smpldat_datasetindex_v2", "word_gram_3", text);
    assertEquals(
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList()),
        List.of("hello cat cool", "cat cool customer"));
    request = AnalyzeRequest.withIndexAnalyzer("smpldat_datasetindex_v2", "word_gram_4", text);
    assertEquals(
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList()),
        List.of("hello cat cool customer"));

    String testMoreSeparators = "quick.brown:fox jumped-LAZY_Dog";
    request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2", "word_gram_2", testMoreSeparators);
    assertEquals(
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList()),
        List.of("quick brown", "brown fox", "fox jumped", "jumped lazy", "lazy dog"));
    request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2", "word_gram_3", testMoreSeparators);
    assertEquals(
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList()),
        List.of("quick brown fox", "brown fox jumped", "fox jumped lazy", "jumped lazy dog"));
    request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2", "word_gram_4", testMoreSeparators);
    assertEquals(
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList()),
        List.of("quick brown fox jumped", "brown fox jumped lazy", "fox jumped lazy dog"));

    String textWithQuotesAndDuplicateWord = "\"my_db.my_exact_table\"";
    request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2", "word_gram_2", textWithQuotesAndDuplicateWord);
    assertEquals(
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList()),
        List.of("my db", "db my", "my exact", "exact table"));
    request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2", "word_gram_3", textWithQuotesAndDuplicateWord);
    assertEquals(
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList()),
        List.of("my db my", "db my exact", "my exact table"));
    request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2", "word_gram_4", textWithQuotesAndDuplicateWord);
    assertEquals(
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList()),
        List.of("my db my exact", "db my exact table"));

    String textWithParens = "(hi) there";
    request =
        AnalyzeRequest.withIndexAnalyzer("smpldat_datasetindex_v2", "word_gram_2", textWithParens);
    assertEquals(
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList()),
        List.of("hi there"));

    String oneWordText = "hello";
    for (String analyzer : List.of("word_gram_2", "word_gram_3", "word_gram_4")) {
      request = AnalyzeRequest.withIndexAnalyzer("smpldat_datasetindex_v2", analyzer, oneWordText);
      assertEquals(
          getTokens(request)
              .map(AnalyzeResponse.AnalyzeToken::getTerm)
              .collect(Collectors.toList()),
          List.of());
    }
  }

  @Test
  public void testUrnSynonym() throws IOException {
    List<String> expectedTokens = List.of("bigquery");

    AnalyzeRequest request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2",
            "urn_component",
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,harshal-playground-306419.bq_audit.cloudaudit_googleapis_com_activity,PROD)");
    List<String> indexTokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    expectedTokens.forEach(
        expected ->
            assertTrue(
                indexTokens.contains(expected),
                String.format("Expected token `%s` in %s", expected, indexTokens)));

    request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2", "query_urn_component", "big query");
    List<String> queryTokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(queryTokens, List.of("big query", "big", "query", "bigquery"));

    List<String> testSet = List.of("bigquery", "big query");
    List<SearchResult> results =
        testSet.stream()
            .map(
                query -> {
                  SearchResult result = searchAcrossEntities(getSearchService(), query);
                  assertTrue(
                      result.hasEntities() && !result.getEntities().isEmpty(),
                      "Expected search results for: " + query);
                  return result;
                })
            .collect(Collectors.toList());

    assertEquals(
        results.stream().map(r -> r.getEntities().size()).distinct().count(),
        1,
        String.format(
            "Expected all result counts (%s) to match after synonyms. %s", results, testSet));
    Assert.assertArrayEquals(
        results.get(0).getEntities().stream()
            .map(e -> e.getEntity().toString())
            .sorted()
            .toArray(String[]::new),
        results.get(1).getEntities().stream()
            .map(e -> e.getEntity().toString())
            .sorted()
            .toArray(String[]::new));
  }

  @Test
  public void testTokenization() throws IOException {
    AnalyzeRequest request =
        AnalyzeRequest.withIndexAnalyzer("smpldat_datasetindex_v2", "word_delimited", "my_table");
    List<String> tokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(
        tokens, List.of("my_tabl", "tabl"), String.format("Unexpected tokens. Found %s", tokens));

    request =
        AnalyzeRequest.withIndexAnalyzer("smpldat_datasetindex_v2", "urn_component", "my_table");
    tokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(
        tokens, List.of("my_tabl", "tabl"), String.format("Unexpected tokens. Found %s", tokens));
  }

  @Test
  public void testTokenizationWithNumber() throws IOException {
    AnalyzeRequest request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2",
            "word_delimited",
            "harshal-playground-306419.test_schema.austin311_derived");
    List<String> tokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(
        tokens,
        List.of(
            "harshal-playground-306419",
            "harshal",
            "playground",
            "306419",
            "test_schema",
            "test",
            "schema",
            "austin311_deriv",
            "austin311",
            "deriv"),
        String.format("Unexpected tokens. Found %s", tokens));

    request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2",
            "urn_component",
            "harshal-playground-306419.test_schema.austin311_derived");
    tokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(
        tokens,
        List.of(
            "harshal-playground-306419",
            "harshal",
            "playground",
            "306419",
            "test_schema",
            "test",
            "schema",
            "austin311_deriv",
            "austin311",
            "deriv"),
        String.format("Unexpected tokens. Found %s", tokens));
  }

  @Test
  public void testTokenizationQuote() throws IOException {
    String testQuery = "\"test2\"";

    AnalyzeRequest request =
        AnalyzeRequest.withIndexAnalyzer("smpldat_datasetindex_v2", "urn_component", testQuery);
    List<String> tokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(tokens, List.of("test2"), String.format("Unexpected tokens. Found %s", tokens));

    request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2", "query_urn_component", testQuery);
    tokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(tokens, List.of("test2"), String.format("Unexpected tokens. Found %s", tokens));
  }

  @Test
  public void testTokenizationQuoteUnderscore() throws IOException {
    String testQuery = "\"raw_orders\"";

    AnalyzeRequest request =
        AnalyzeRequest.withIndexAnalyzer("smpldat_datasetindex_v2", "word_delimited", testQuery);
    List<String> tokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(
        tokens,
        List.of("raw_orders", "raw_ord", "raw", "order"),
        String.format("Unexpected tokens. Found %s", tokens));

    request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2", "query_word_delimited", testQuery);
    tokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(
        tokens,
        List.of("raw_orders", "raw_ord", "raw", "order"),
        String.format("Unexpected tokens. Found %s", tokens));

    request =
        AnalyzeRequest.withIndexAnalyzer("smpldat_datasetindex_v2", "quote_analyzer", testQuery);
    tokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(
        tokens, List.of("raw_orders"), String.format("Unexpected tokens. Found %s", tokens));
  }

  @Test
  public void testTokenizationDataPlatform() throws IOException {
    AnalyzeRequest request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2",
            "urn_component",
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,harshal-playground-306419.test_schema.excess_deaths_derived,PROD)");
    List<String> tokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(
        tokens,
        List.of(
            "dataset",
            "dataplatform",
            "data platform",
            "bigquery",
            "big",
            "query",
            "harshal-playground-306419",
            "harshal",
            "playground",
            "306419",
            "test_schema",
            "test",
            "schema",
            "excess_deaths_deriv",
            "excess",
            "death",
            "deriv",
            "prod",
            "production"),
        String.format("Unexpected tokens. Found %s", tokens));

    request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2",
            "urn_component",
            "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset-ac611929-c3ac-4b92-aafb-f4603ddb408a,PROD)");
    tokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(
        tokens,
        List.of(
            "dataset",
            "dataplatform",
            "data platform",
            "hive",
            "samplehivedataset-ac611929-c3ac-4b92-aafb-f4603ddb408a",
            "samplehivedataset",
            "ac611929",
            "c3ac",
            "4b92",
            "aafb",
            "f4603ddb408a",
            "prod",
            "production"),
        String.format("Unexpected tokens. Found %s", tokens));

    request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2",
            "urn_component",
            "urn:li:dataset:(urn:li:dataPlatform:test_rollback,rollback_test_dataset,TEST)");
    tokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(
        tokens,
        List.of(
            "dataset",
            "dataplatform",
            "data platform",
            "test_rollback",
            "test",
            "rollback",
            "rollback_test_dataset"),
        String.format("Unexpected tokens. Found %s", tokens));
  }

  @Test
  public void testChartAutoComplete() throws InterruptedException, IOException {
    // Two charts exist Baz Chart 1 & Baz Chart 2
    List.of(
            "B",
            "Ba",
            "Baz",
            "Baz ",
            "Baz C",
            "Baz Ch",
            "Baz Cha",
            "Baz Char",
            "Baz Chart",
            "Baz Chart ")
        .forEach(
            query -> {
              try {
                AutoCompleteResults result = autocomplete(new ChartType(getEntityClient()), query);
                assertTrue(
                    result.getEntities().size() == 2,
                    String.format(
                        "Expected 2 results for `%s` found %s",
                        query, result.getEntities().size()));
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
  }

  @Test
  public void testDatasetAutoComplete() {
    List.of(
            "excess",
            "excess_",
            "excess_d",
            "excess_de",
            "excess_death",
            "excess_deaths",
            "excess_deaths_d",
            "excess_deaths_de",
            "excess_deaths_der",
            "excess_deaths_derived")
        .forEach(
            query -> {
              try {
                AutoCompleteResults result =
                    autocomplete(new DatasetType(getEntityClient()), query);
                assertTrue(
                    result.getEntities().size() >= 1,
                    String.format(
                        "Expected >= 1 results for `%s` found %s",
                        query, result.getEntities().size()));
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
  }

  @Test
  public void testContainerAutoComplete() {
    List.of(
            "cont",
            "container",
            "container-a",
            "container-auto",
            "container-autocomp",
            "container-autocomp-te",
            "container-autocomp-test")
        .forEach(
            query -> {
              try {
                AutoCompleteResults result =
                    autocomplete(new ContainerType(getEntityClient()), query);
                assertTrue(
                    result.getEntities().size() >= 1,
                    String.format(
                        "Expected >= 1 results for `%s` found %s",
                        query, result.getEntities().size()));
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
  }

  @Test
  public void testGroupAutoComplete() {
    List.of("T", "Te", "Tes", "Test ", "Test G", "Test Gro", "Test Group ")
        .forEach(
            query -> {
              try {
                AutoCompleteResults result =
                    autocomplete(new CorpGroupType(getEntityClient()), query);
                assertTrue(
                    result.getEntities().size() == 1,
                    String.format(
                        "Expected 1 results for `%s` found %s",
                        query, result.getEntities().size()));
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
  }

  @Test
  public void testUserAutoComplete() {
    List.of("D", "Da", "Dat", "Data ", "Data H", "Data Hu", "Data Hub", "Data Hub ")
        .forEach(
            query -> {
              try {
                AutoCompleteResults result =
                    autocomplete(new CorpUserType(getEntityClient(), null), query);
                assertTrue(
                    result.getEntities().size() >= 1,
                    String.format(
                        "Expected at least 1 results for `%s` found %s",
                        query, result.getEntities().size()));
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
  }

  @Test
  public void testSmokeTestQueries() {
    Map<String, Integer> expectedFulltextMinimums =
        Map.of(
            "sample",
            3,
            "covid",
            2,
            "\"raw_orders\"",
            6,
            STRUCTURED_QUERY_PREFIX + "sample",
            3,
            STRUCTURED_QUERY_PREFIX + "\"sample\"",
            2,
            STRUCTURED_QUERY_PREFIX + "covid",
            2,
            STRUCTURED_QUERY_PREFIX + "\"raw_orders\"",
            1);

    Map<String, SearchResult> results =
        expectedFulltextMinimums.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> searchAcrossEntities(getSearchService(), entry.getKey())));

    results.forEach(
        (key, value) -> {
          Integer actualCount = value.getEntities().size();
          Integer expectedCount = expectedFulltextMinimums.get(key);
          assertSame(
              actualCount,
              expectedCount,
              String.format(
                  "Search term `%s` has %s fulltext results, expected %s results.",
                  key, actualCount, expectedCount));
        });

    Map<String, Integer> expectedStructuredMinimums =
        Map.of(
            "sample", 3,
            "covid", 2,
            "\"raw_orders\"", 1);

    results =
        expectedStructuredMinimums.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> searchStructured(getSearchService(), entry.getKey())));

    results.forEach(
        (key, value) -> {
          Integer actualCount = value.getEntities().size();
          Integer expectedCount = expectedStructuredMinimums.get(key);
          assertSame(
              actualCount,
              expectedCount,
              String.format(
                  "Search term `%s` has %s structured results, expected %s results.",
                  key, actualCount, expectedCount));
        });
  }

  @Test
  public void testMinNumberLengthLimit() throws IOException {
    AnalyzeRequest request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2", "word_delimited", "data2022.data22");
    List<String> expected = List.of("data2022", "data22");
    List<String> actual =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(actual, expected, String.format("Expected: %s Actual: %s", expected, actual));
  }

  @Test
  public void testUnderscore() throws IOException {
    String testQuery = "bad_fraud_id";
    List<String> expected = List.of("bad_fraud_id", "bad", "fraud");

    AnalyzeRequest request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2", "query_word_delimited", testQuery);

    List<String> actual =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(
        actual,
        expected,
        String.format("Analayzer: query_word_delimited Expected: %s Actual: %s", expected, actual));

    request =
        AnalyzeRequest.withIndexAnalyzer("smpldat_datasetindex_v2", "word_delimited", testQuery);
    actual =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(
        actual,
        expected,
        String.format("Analyzer: word_delimited Expected: %s Actual: %s", expected, actual));
  }

  @Test
  public void testFacets() {
    Set<String> expectedFacets = Set.of("entity", "typeNames", "platform", "origin", "tags");
    SearchResult testResult = searchAcrossEntities(getSearchService(), "cypress");
    expectedFacets.forEach(
        facet -> {
          assertTrue(
              testResult.getMetadata().getAggregations().stream()
                  .anyMatch(agg -> agg.getName().equals(facet)),
              String.format(
                  "Failed to find facet `%s` in %s",
                  facet,
                  testResult.getMetadata().getAggregations().stream()
                      .map(AggregationMetadata::getName)
                      .collect(Collectors.toList())));
        });
    AggregationMetadata entityAggMeta =
        testResult.getMetadata().getAggregations().stream()
            .filter(aggMeta -> aggMeta.getName().equals("entity"))
            .findFirst()
            .get();
    Map<String, Long> expectedEntityTypeCounts = new HashMap<>();
    expectedEntityTypeCounts.put("container", 0L);
    expectedEntityTypeCounts.put("corpuser", 0L);
    expectedEntityTypeCounts.put("corpgroup", 0L);
    expectedEntityTypeCounts.put("mlmodel", 0L);
    expectedEntityTypeCounts.put("mlfeaturetable", 1L);
    expectedEntityTypeCounts.put("mlmodelgroup", 1L);
    expectedEntityTypeCounts.put("dataflow", 1L);
    expectedEntityTypeCounts.put("glossarynode", 1L);
    expectedEntityTypeCounts.put("mlfeature", 0L);
    expectedEntityTypeCounts.put("datajob", 2L);
    expectedEntityTypeCounts.put("domain", 0L);
    expectedEntityTypeCounts.put("tag", 0L);
    expectedEntityTypeCounts.put("glossaryterm", 2L);
    expectedEntityTypeCounts.put("mlprimarykey", 1L);
    expectedEntityTypeCounts.put("dataset", 9L);
    expectedEntityTypeCounts.put("chart", 0L);
    expectedEntityTypeCounts.put("dashboard", 0L);
    assertEquals(entityAggMeta.getAggregations(), expectedEntityTypeCounts);
  }

  @Test
  public void testNestedAggregation() {
    Set<String> expectedFacets = Set.of("platform");
    SearchResult testResult =
        searchAcrossEntities(getSearchService(), "cypress", List.copyOf(expectedFacets));
    assertEquals(testResult.getMetadata().getAggregations().size(), 1);
    expectedFacets.forEach(
        facet -> {
          assertTrue(
              testResult.getMetadata().getAggregations().stream()
                  .anyMatch(agg -> agg.getName().equals(facet)),
              String.format(
                  "Failed to find facet `%s` in %s",
                  facet,
                  testResult.getMetadata().getAggregations().stream()
                      .map(AggregationMetadata::getName)
                      .collect(Collectors.toList())));
        });

    expectedFacets = Set.of("platform", "typeNames", "_entityType", "entity");
    SearchResult testResult2 =
        searchAcrossEntities(getSearchService(), "cypress", List.copyOf(expectedFacets));
    assertEquals(testResult2.getMetadata().getAggregations().size(), 4);
    expectedFacets.forEach(
        facet -> {
          assertTrue(
              testResult2.getMetadata().getAggregations().stream()
                  .anyMatch(agg -> agg.getName().equals(facet)),
              String.format(
                  "Failed to find facet `%s` in %s",
                  facet,
                  testResult2.getMetadata().getAggregations().stream()
                      .map(AggregationMetadata::getName)
                      .collect(Collectors.toList())));
        });
    AggregationMetadata entityTypeAggMeta =
        testResult2.getMetadata().getAggregations().stream()
            .filter(aggMeta -> aggMeta.getName().equals("_entityType"))
            .findFirst()
            .get();
    AggregationMetadata entityAggMeta =
        testResult2.getMetadata().getAggregations().stream()
            .filter(aggMeta -> aggMeta.getName().equals("entity"))
            .findFirst()
            .get();
    assertEquals(entityTypeAggMeta.getAggregations(), entityAggMeta.getAggregations());
    Map<String, Long> expectedEntityTypeCounts = new HashMap<>();
    expectedEntityTypeCounts.put("container", 0L);
    expectedEntityTypeCounts.put("corpuser", 0L);
    expectedEntityTypeCounts.put("corpgroup", 0L);
    expectedEntityTypeCounts.put("mlmodel", 0L);
    expectedEntityTypeCounts.put("mlfeaturetable", 1L);
    expectedEntityTypeCounts.put("mlmodelgroup", 1L);
    expectedEntityTypeCounts.put("dataflow", 1L);
    expectedEntityTypeCounts.put("glossarynode", 1L);
    expectedEntityTypeCounts.put("mlfeature", 0L);
    expectedEntityTypeCounts.put("datajob", 2L);
    expectedEntityTypeCounts.put("domain", 0L);
    expectedEntityTypeCounts.put("tag", 0L);
    expectedEntityTypeCounts.put("glossaryterm", 2L);
    expectedEntityTypeCounts.put("mlprimarykey", 1L);
    expectedEntityTypeCounts.put("dataset", 9L);
    expectedEntityTypeCounts.put("chart", 0L);
    expectedEntityTypeCounts.put("dashboard", 0L);
    assertEquals(entityTypeAggMeta.getAggregations(), expectedEntityTypeCounts);

    expectedFacets = Set.of("platform", "typeNames", "entity");
    SearchResult testResult3 =
        searchAcrossEntities(getSearchService(), "cypress", List.copyOf(expectedFacets));
    assertEquals(testResult3.getMetadata().getAggregations().size(), 4);
    expectedFacets.forEach(
        facet -> {
          assertTrue(
              testResult3.getMetadata().getAggregations().stream()
                  .anyMatch(agg -> agg.getName().equals(facet)),
              String.format(
                  "Failed to find facet `%s` in %s",
                  facet,
                  testResult3.getMetadata().getAggregations().stream()
                      .map(AggregationMetadata::getName)
                      .collect(Collectors.toList())));
        });
    AggregationMetadata entityTypeAggMeta3 =
        testResult3.getMetadata().getAggregations().stream()
            .filter(aggMeta -> aggMeta.getName().equals("_entityType"))
            .findFirst()
            .get();
    AggregationMetadata entityAggMeta3 =
        testResult3.getMetadata().getAggregations().stream()
            .filter(aggMeta -> aggMeta.getName().equals("entity"))
            .findFirst()
            .get();
    assertEquals(entityTypeAggMeta3.getAggregations(), entityAggMeta3.getAggregations());
    assertEquals(entityTypeAggMeta3.getAggregations(), expectedEntityTypeCounts);

    String singleNestedFacet = String.format("_entityType%sowners", AGGREGATION_SEPARATOR_CHAR);
    expectedFacets = Set.of(singleNestedFacet);
    SearchResult testResultSingleNested =
        searchAcrossEntities(getSearchService(), "cypress", List.copyOf(expectedFacets));
    assertEquals(testResultSingleNested.getMetadata().getAggregations().size(), 1);
    Map<String, Long> expectedNestedFacetCounts = new HashMap<>();
    expectedNestedFacetCounts.put("datajob␞urn:li:corpuser:datahub", 2L);
    expectedNestedFacetCounts.put("glossarynode␞urn:li:corpuser:jdoe", 1L);
    expectedNestedFacetCounts.put("dataflow␞urn:li:corpuser:datahub", 1L);
    expectedNestedFacetCounts.put("mlfeaturetable", 1L);
    expectedNestedFacetCounts.put("mlmodelgroup", 1L);
    expectedNestedFacetCounts.put("glossarynode", 1L);
    expectedNestedFacetCounts.put("dataflow", 1L);
    expectedNestedFacetCounts.put("mlmodelgroup␞urn:li:corpuser:some-user", 1L);
    expectedNestedFacetCounts.put("datajob", 2L);
    expectedNestedFacetCounts.put("glossaryterm␞urn:li:corpuser:jdoe", 2L);
    expectedNestedFacetCounts.put("glossaryterm", 2L);
    expectedNestedFacetCounts.put("dataset", 9L);
    expectedNestedFacetCounts.put("mlprimarykey", 1L);
    assertEquals(
        testResultSingleNested.getMetadata().getAggregations().get(0).getAggregations(),
        expectedNestedFacetCounts);

    expectedFacets = Set.of("platform", singleNestedFacet, "typeNames", "origin");
    SearchResult testResultNested =
        searchAcrossEntities(getSearchService(), "cypress", List.copyOf(expectedFacets));
    assertEquals(testResultNested.getMetadata().getAggregations().size(), 4);
    expectedFacets.forEach(
        facet -> {
          assertTrue(
              testResultNested.getMetadata().getAggregations().stream()
                  .anyMatch(agg -> agg.getName().equals(facet)),
              String.format(
                  "Failed to find facet `%s` in %s",
                  facet,
                  testResultNested.getMetadata().getAggregations().stream()
                      .map(AggregationMetadata::getName)
                      .collect(Collectors.toList())));
        });

    List<AggregationMetadata> expectedNestedAgg =
        testResultNested.getMetadata().getAggregations().stream()
            .filter(agg -> agg.getName().equals(singleNestedFacet))
            .collect(Collectors.toList());
    assertEquals(expectedNestedAgg.size(), 1);
    AggregationMetadata nestedAgg = expectedNestedAgg.get(0);
    assertEquals(
        nestedAgg.getDisplayName(), String.format("Type%sOwned By", AGGREGATION_SEPARATOR_CHAR));
  }

  @Test
  public void testPartialUrns() throws IOException {
    Set<String> expectedQueryTokens =
        Set.of("dataplatform", "data platform", "samplehdfsdataset", "prod", "production");
    Set<String> expectedIndexTokens =
        Set.of("dataplatform", "data platform", "hdfs", "samplehdfsdataset", "prod", "production");

    AnalyzeRequest request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2",
            "query_urn_component",
            ":(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    List<String> searchQueryTokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    expectedQueryTokens.forEach(
        expected ->
            assertTrue(
                searchQueryTokens.contains(expected),
                String.format("Expected token `%s` in %s", expected, searchQueryTokens)));

    request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2",
            "urn_component",
            ":(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    List<String> searchIndexTokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    expectedIndexTokens.forEach(
        expected ->
            assertTrue(
                searchIndexTokens.contains(expected),
                String.format("Expected token `%s` in %s", expected, searchIndexTokens)));
  }

  @Test
  public void testPartialUnderscoreUrns() throws IOException {
    String testQuery = ":(urn:li:dataPlatform:hdfs,party_email,PROD)";
    Set<String> expectedQueryTokens =
        Set.of(
            "dataplatform",
            "data platform",
            "hdfs",
            "party_email",
            "parti",
            "email",
            "prod",
            "production");
    Set<String> expectedIndexTokens =
        Set.of(
            "dataplatform",
            "data platform",
            "hdfs",
            "party_email",
            "parti",
            "email",
            "prod",
            "production");

    AnalyzeRequest request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2", "query_urn_component", testQuery);
    List<String> searchQueryTokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    expectedQueryTokens.forEach(
        expected ->
            assertTrue(
                searchQueryTokens.contains(expected),
                String.format("Expected token `%s` in %s", expected, searchQueryTokens)));

    request =
        AnalyzeRequest.withIndexAnalyzer("smpldat_datasetindex_v2", "urn_component", testQuery);
    List<String> searchIndexTokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    expectedIndexTokens.forEach(
        expected ->
            assertTrue(
                searchIndexTokens.contains(expected),
                String.format("Expected token `%s` in %s", expected, searchIndexTokens)));
  }

  @Test
  public void testScrollAcrossEntities() throws IOException {
    String query = "logging_events";
    final int batchSize = 1;
    int totalResults = 0;
    String scrollId = null;
    do {
      ScrollResult result = scroll(getSearchService(), query, batchSize, scrollId);
      int numResults = result.hasEntities() ? result.getEntities().size() : 0;
      assertTrue(numResults <= batchSize);
      totalResults += numResults;
      scrollId = result.getScrollId();
    } while (scrollId != null);
    // expect 8 total matching results
    assertEquals(totalResults, 8);
  }

  @Test
  public void testSearchAcrossMultipleEntities() {
    String query = "logging_events";
    SearchResult result = search(getSearchService(), query);
    assertEquals((int) result.getNumEntities(), 8);
    result = search(getSearchService(), List.of(DATASET_ENTITY_NAME, DATA_JOB_ENTITY_NAME), query);
    assertEquals((int) result.getNumEntities(), 8);
    result = search(getSearchService(), List.of(DATASET_ENTITY_NAME), query);
    assertEquals((int) result.getNumEntities(), 4);
    result = search(getSearchService(), List.of(DATA_JOB_ENTITY_NAME), query);
    assertEquals((int) result.getNumEntities(), 4);
  }

  @Test
  public void testQuotedAnalyzer() throws IOException {
    AnalyzeRequest request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2", "quote_analyzer", "\"party_email\"");
    List<String> searchQuotedQueryTokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(
        List.of("party_email"),
        searchQuotedQueryTokens,
        String.format("Actual %s", searchQuotedQueryTokens));

    request =
        AnalyzeRequest.withIndexAnalyzer("smpldat_datasetindex_v2", "quote_analyzer", "\"test2\"");
    searchQuotedQueryTokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(List.of("test2"), searchQuotedQueryTokens);

    request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2", "quote_analyzer", "\"party_email\"");
    searchQuotedQueryTokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(List.of("party_email"), searchQuotedQueryTokens);

    request =
        AnalyzeRequest.withIndexAnalyzer("smpldat_datasetindex_v2", "quote_analyzer", "\"test2\"");
    searchQuotedQueryTokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(List.of("test2"), searchQuotedQueryTokens);

    request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2", "quote_analyzer", "\"test_BYTES_LIST_feature\"");
    searchQuotedQueryTokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertEquals(List.of("test_bytes_list_feature"), searchQuotedQueryTokens);

    request =
        AnalyzeRequest.withIndexAnalyzer(
            "smpldat_datasetindex_v2", "query_word_delimited", "test_BYTES_LIST_feature");
    searchQuotedQueryTokens =
        getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
    assertTrue(searchQuotedQueryTokens.contains("test_bytes_list_featur"));
  }

  @Test
  public void testFragmentUrns() {
    List<String> testSet =
        List.of(
            "hdfs,SampleHdfsDataset,PROD",
            "hdfs,SampleHdfsDataset",
            "SampleHdfsDataset",
            "(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
            "urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD",
            "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
            ":(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");

    testSet.forEach(
        query -> {
          SearchResult result = searchAcrossEntities(getSearchService(), query);

          assertTrue(
              result.hasEntities() && !result.getEntities().isEmpty(),
              String.format("%s - Expected partial urn search results", query));
          assertTrue(
              result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
              String.format("%s - Expected search results to include matched fields", query));
        });
  }

  @Test
  public void testPlatformTest() {
    List<String> testFields = List.of("platform.keyword", "platform");
    final String testPlatform = "urn:li:dataPlatform:dbt";

    // Ensure backend code path works as expected
    List<SearchResult> results =
        testFields.stream()
            .map(
                fieldName -> {
                  final String query =
                      String.format("%s:%s", fieldName, testPlatform.replaceAll(":", "\\\\:"));
                  SearchResult result = searchStructured(getSearchService(), query);
                  assertTrue(
                      result.hasEntities() && !result.getEntities().isEmpty(),
                      String.format("%s - Expected search results", query));
                  assertTrue(
                      result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                      String.format(
                          "%s - Expected search results to include matched fields", query));
                  return result;
                })
            .collect(Collectors.toList());

    IntStream.range(0, testFields.size())
        .forEach(
            idx -> {
              assertEquals(
                  results.get(idx).getEntities().size(),
                  9,
                  String.format("Search results for fields `%s` != 9", testFields.get(idx)));
            });

    // Construct problematic search entity query
    List<Filter> testFilters =
        testFields.stream()
            .map(
                fieldName -> {
                  Filter filter = new Filter();
                  ArrayList<Criterion> criteria = new ArrayList<>();
                  Criterion hasPlatformCriterion =
                      new Criterion()
                          .setField(fieldName)
                          .setCondition(Condition.EQUAL)
                          .setValue(testPlatform);
                  criteria.add(hasPlatformCriterion);
                  filter.setOr(
                      new ConjunctiveCriterionArray(
                          new ConjunctiveCriterion().setAnd(new CriterionArray(criteria))));
                  return filter;
                })
            .collect(Collectors.toList());

    // Test variations of fulltext flags
    for (Boolean fulltextFlag : List.of(true, false)) {

      // Test field variations with/without .keyword
      List<SearchResult> entityClientResults =
          testFilters.stream()
              .map(
                  filter -> {
                    try {
                      return getEntityClient()
                          .search(
                              "dataset",
                              "*",
                              filter,
                              null,
                              0,
                              100,
                              AUTHENTICATION,
                              new SearchFlags().setFulltext(fulltextFlag));
                    } catch (RemoteInvocationException e) {
                      throw new RuntimeException(e);
                    }
                  })
              .collect(Collectors.toList());

      IntStream.range(0, testFields.size())
          .forEach(
              idx -> {
                assertEquals(
                    entityClientResults.get(idx).getEntities().size(),
                    9,
                    String.format(
                        "Search results for entityClient fields (fulltextFlag: %s): `%s` != 9",
                        fulltextFlag, testFields.get(idx)));
              });
    }
  }

  @Test
  public void testStructQueryFieldMatch() {
    String query = STRUCTURED_QUERY_PREFIX + "name: customers";
    SearchResult result = searchAcrossEntities(getSearchService(), query);

    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));

    assertEquals(result.getEntities().size(), 1);
  }

  @Test
  public void testStructQueryFieldPrefixMatch() {
    String query = STRUCTURED_QUERY_PREFIX + "name: customers*";
    SearchResult result = searchAcrossEntities(getSearchService(), query);

    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));

    assertEquals(result.getEntities().size(), 2);
  }

  @Test
  public void testStructQueryCustomPropertiesKeyPrefix() {
    String query = STRUCTURED_QUERY_PREFIX + "customProperties: node_type=*";
    SearchResult result = searchAcrossEntities(getSearchService(), query);

    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));

    assertEquals(result.getEntities().size(), 9);
  }

  @Test
  public void testStructQueryCustomPropertiesMatch() {
    String query = STRUCTURED_QUERY_PREFIX + "customProperties: node_type=model";
    SearchResult result = searchAcrossEntities(getSearchService(), query);

    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));

    assertEquals(result.getEntities().size(), 5);
  }

  @Test
  public void testCustomPropertiesQuoted() {
    Map<String, Integer> expectedResults =
        Map.of(
            "\"materialization=view\"",
            3,
            STRUCTURED_QUERY_PREFIX + "customProperties:\"materialization=view\"",
            3);

    Map<String, SearchResult> results =
        expectedResults.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> searchAcrossEntities(getSearchService(), entry.getKey())));

    results.forEach(
        (key, value) -> {
          Integer actualCount = value.getEntities().size();
          Integer expectedCount = expectedResults.get(key);
          assertSame(
              actualCount,
              expectedCount,
              String.format(
                  "Search term `%s` has %s fulltext results, expected %s results.",
                  key, actualCount, expectedCount));
        });
  }

  @Test
  public void testStructQueryFieldPaths() {
    String query = STRUCTURED_QUERY_PREFIX + "fieldPaths: customer_id";
    SearchResult result = searchAcrossEntities(getSearchService(), query);

    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));

    assertEquals(result.getEntities().size(), 3);
  }

  @Test
  public void testStructQueryBoolean() {
    String query =
        STRUCTURED_QUERY_PREFIX
            + "editedFieldTags:urn\\:li\\:tag\\:Legacy OR tags:urn\\:li\\:tag\\:testTag";
    SearchResult result = searchAcrossEntities(getSearchService(), query);

    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));

    assertEquals(result.getEntities().size(), 2);

    query = STRUCTURED_QUERY_PREFIX + "editedFieldTags:urn\\:li\\:tag\\:Legacy";
    result = searchAcrossEntities(getSearchService(), query);

    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));

    assertEquals(result.getEntities().size(), 1);

    query = STRUCTURED_QUERY_PREFIX + "tags:urn\\:li\\:tag\\:testTag";
    result = searchAcrossEntities(getSearchService(), query);

    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));

    assertEquals(result.getEntities().size(), 1);
  }

  @Test
  public void testStructQueryBrowsePaths() {
    String query = STRUCTURED_QUERY_PREFIX + "browsePaths:*/dbt/*";
    SearchResult result = searchAcrossEntities(getSearchService(), query);

    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));

    assertEquals(result.getEntities().size(), 9);
  }

  @Test
  public void testOr() {
    String query = "stg_customers | logging_events";
    SearchResult result = searchAcrossEntities(getSearchService(), query);
    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));
    assertEquals(result.getEntities().size(), 9);

    query = "stg_customers";
    result = searchAcrossEntities(getSearchService(), query);
    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));
    assertEquals(result.getEntities().size(), 1);

    query = "logging_events";
    result = searchAcrossEntities(getSearchService(), query);
    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));
    assertEquals(result.getEntities().size(), 8);
  }

  @Test
  public void testNegate() {
    String query = "logging_events -bckp";
    SearchResult result = searchAcrossEntities(getSearchService(), query);
    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));
    assertEquals(result.getEntities().size(), 7);

    query = "logging_events";
    result = searchAcrossEntities(getSearchService(), query);
    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));
    assertEquals(result.getEntities().size(), 8);
  }

  @Test
  public void testPrefix() {
    String query = "bigquery";
    SearchResult result = searchAcrossEntities(getSearchService(), query);
    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));
    assertEquals(result.getEntities().size(), 8);

    query = "big*";
    result = searchAcrossEntities(getSearchService(), query);
    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));
    assertEquals(result.getEntities().size(), 8);
  }

  @Test
  public void testParens() {
    String query = "dbt | (bigquery + covid19)";
    SearchResult result = searchAcrossEntities(getSearchService(), query);
    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));
    assertEquals(result.getEntities().size(), 11);

    query = "dbt";
    result = searchAcrossEntities(getSearchService(), query);
    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));
    assertEquals(result.getEntities().size(), 9);

    query = "bigquery + covid19";
    result = searchAcrossEntities(getSearchService(), query);
    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));
    assertEquals(result.getEntities().size(), 2);

    query = "bigquery";
    result = searchAcrossEntities(getSearchService(), query);
    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));
    assertEquals(result.getEntities().size(), 8);

    query = "covid19";
    result = searchAcrossEntities(getSearchService(), query);
    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));
    assertEquals(result.getEntities().size(), 2);
  }

  @Test
  public void testGram() {
    String query = "jaffle shop customers";
    SearchResult result = searchAcrossEntities(getSearchService(), query);
    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));

    assertEquals(
        result.getEntities().get(0).getEntity().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.customers,PROD)",
        "Expected exact match in 1st position");

    query = "shop customers source";
    result = searchAcrossEntities(getSearchService(), query);
    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));

    assertEquals(
        result.getEntities().get(0).getEntity().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.customers_source,PROD)",
        "Expected ngram match in 1st position");

    query = "jaffle shop stg customers";
    result = searchAcrossEntities(getSearchService(), query);
    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));

    assertEquals(
        result.getEntities().get(0).getEntity().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.stg_customers,PROD)",
        "Expected ngram match in 1st position");

    query = "jaffle shop transformers customers";
    result = searchAcrossEntities(getSearchService(), query);
    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));

    assertEquals(
        result.getEntities().get(0).getEntity().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.transformers_customers,PROD)",
        "Expected ngram match in 1st position");

    query = "shop raw customers";
    result = searchAcrossEntities(getSearchService(), query);
    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));

    assertEquals(
        result.getEntities().get(0).getEntity().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.raw_customers,PROD)",
        "Expected ngram match in 1st position");
  }

  @Test
  public void testPrefixVsExact() {
    String query = "\"customers\"";
    SearchResult result = searchAcrossEntities(getSearchService(), query);

    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));

    assertEquals(result.getEntities().size(), 10);
    assertEquals(
        result.getEntities().get(0).getEntity().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.customers,PROD)",
        "Expected exact match and 1st position");
  }

  // Note: This test can fail if not using .keyword subfields (check for possible query builder
  // regression)
  @Test
  public void testPrefixVsExactCaseSensitivity() {
    List<String> insensitiveExactMatches =
        List.of("testExactMatchCase", "testexactmatchcase", "TESTEXACTMATCHCASE");
    for (String query : insensitiveExactMatches) {
      SearchResult result = searchAcrossEntities(getSearchService(), query);

      assertTrue(
          result.hasEntities() && !result.getEntities().isEmpty(),
          String.format("%s - Expected search results", query));
      assertTrue(
          result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
          String.format("%s - Expected search results to include matched fields", query));

      assertEquals(result.getEntities().size(), insensitiveExactMatches.size());
      assertEquals(
          result.getEntities().get(0).getEntity().toString(),
          "urn:li:dataset:(urn:li:dataPlatform:testOnly," + query + ",PROD)",
          "Expected exact match as first match with matching case");
    }
  }

  @Test
  public void testColumnExactMatch() {
    String query = "unit_data";
    SearchResult result = searchAcrossEntities(getSearchService(), query);
    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));

    assertTrue(
        result.getEntities().size() > 2,
        String.format("%s - Expected search results to have at least two results", query));
    assertEquals(
        result.getEntities().get(0).getEntity().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:testOnly," + query + ",PROD)",
        "Expected table name exact match first");

    query = "special_column_only_present_here_info";
    result = searchAcrossEntities(getSearchService(), query);
    assertTrue(
        result.hasEntities() && !result.getEntities().isEmpty(),
        String.format("%s - Expected search results", query));
    assertTrue(
        result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
        String.format("%s - Expected search results to include matched fields", query));

    assertTrue(
        result.getEntities().size() > 2,
        String.format("%s - Expected search results to have at least two results", query));
    assertEquals(
        result.getEntities().get(0).getEntity().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:testOnly," + "important_units" + ",PROD)",
        "Expected table with column name exact match first");
  }

  @Test
  public void testSortOrdering() {
    String query = "unit_data";
    SortCriterion criterion =
        new SortCriterion().setOrder(SortOrder.ASCENDING).setField("lastOperationTime");
    SearchResult result =
        getSearchService()
            .searchAcrossEntities(
                SEARCHABLE_ENTITIES,
                query,
                null,
                criterion,
                0,
                100,
                new SearchFlags().setFulltext(true).setSkipCache(true),
                null);
    assertTrue(
        result.getEntities().size() > 2,
        String.format("%s - Expected search results to have at least two results", query));
  }

  private Stream<AnalyzeResponse.AnalyzeToken> getTokens(AnalyzeRequest request)
      throws IOException {
    return getSearchClient()
        .indices()
        .analyze(request, RequestOptions.DEFAULT)
        .getTokens()
        .stream();
  }
}

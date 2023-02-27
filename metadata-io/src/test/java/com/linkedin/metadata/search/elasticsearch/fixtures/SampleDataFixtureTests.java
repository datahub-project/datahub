package com.linkedin.metadata.search.elasticsearch.fixtures;

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
import com.linkedin.metadata.ESSampleDataFixture;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;

import com.linkedin.r2.RemoteInvocationException;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.linkedin.metadata.ESTestUtils.autocomplete;
import static com.linkedin.metadata.ESTestUtils.search;
import static com.linkedin.metadata.ESTestUtils.searchStructured;
import static com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder.STRUCTURED_QUERY_PREFIX;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


@Import(ESSampleDataFixture.class)
public class SampleDataFixtureTests extends AbstractTestNGSpringContextTests {
    private static final Authentication AUTHENTICATION =
            new Authentication(new Actor(ActorType.USER, "test"), "");

    @Autowired
    private RestHighLevelClient _searchClient;

    @Autowired
    @Qualifier("sampleDataSearchService")
    protected SearchService searchService;

    @Autowired
    @Qualifier("sampleDataEntityClient")
    protected EntityClient entityClient;

    @Test
    public void testFixtureInitialization() {
        assertNotNull(searchService);
        SearchResult noResult = search(searchService, "no results");
        assertEquals(0, noResult.getEntities().size());

        final SearchResult result = search(searchService, "test");

        Map<String, Integer> expectedTypes = Map.of(
                "dataset", 7,
                "chart", 0,
                "container", 1,
                "dashboard", 0,
                "tag", 0,
                "mlmodel", 0
        );

        Map<String, List<Urn>> actualTypes = new HashMap<>();
        for (String key : expectedTypes.keySet()) {
            actualTypes.put(key, result.getEntities().stream()
                .map(SearchEntity::getEntity).filter(entity -> key.equals(entity.getEntityType())).collect(Collectors.toList()));
        }

        expectedTypes.forEach((key, value) ->
                assertEquals(actualTypes.get(key).size(), value.intValue(),
                        String.format("Expected entity `%s` matches for %s. Found %s", value, key,
                                result.getEntities().stream()
                                        .filter(e -> e.getEntity().getEntityType().equals(key))
                                        .map(e -> e.getEntity().getEntityKey())
                                        .collect(Collectors.toList()))));
    }

    @Test
    public void testDataPlatform() {
        Map<String, Integer> expected = ImmutableMap.<String, Integer>builder()
                .put("urn:li:dataPlatform:BigQuery", 8)
                .put("urn:li:dataPlatform:hive", 6)
                .put("urn:li:dataPlatform:mysql", 5)
                .put("urn:li:dataPlatform:s3", 1)
                .put("urn:li:dataPlatform:hdfs", 2)
                .put("urn:li:dataPlatform:graph", 1)
                .put("urn:li:dataPlatform:dbt", 9)
                .put("urn:li:dataplatform:BigQuery", 8)
                .put("urn:li:dataplatform:hive", 6)
                .put("urn:li:dataplatform:mysql", 5)
                .put("urn:li:dataplatform:s3", 1)
                .put("urn:li:dataplatform:hdfs", 2)
                .put("urn:li:dataplatform:graph", 1)
                .put("urn:li:dataplatform:dbt", 9)
                .build();

        expected.forEach((key, value) -> {
            SearchResult result = search(searchService, key);
            assertEquals(result.getEntities().size(), value.intValue(),
                    String.format("Unexpected data platform `%s` hits.", key)); // max is 100 without pagination
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
                "urn:li:mlModel:(urn:li:dataPlatform:science,scienceModel,PROD)"
        ).forEach(query ->
            assertTrue(search(searchService, query).getEntities().size() >= 1,
                    String.format("Unexpected >1 urn result for `%s`", query))
        );
    }

    @Test
    public void testExactTable() {
        SearchResult results = search(searchService, "stg_customers");
        assertEquals(results.getEntities().size(), 1, "Unexpected single urn result for `stg_customers`");
        assertEquals(results.getEntities().get(0).getEntity().toString(),
                "urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.stg_customers,PROD)");
    }

    @Test
    public void testStemming() {
        List<Set<String>> testSets = List.of(
                Set.of("log", "logs", "logging"),
                Set.of("border", "borders", "bordered", "bordering"),
                Set.of("indicates", "indicate", "indicated")
        );

        testSets.forEach(testSet -> {
            Set<SearchResult> results = testSet.stream()
                    .map(test -> search(searchService, test))
                    .collect(Collectors.toSet());

            results.forEach(r -> assertTrue(r.hasEntities() && !r.getEntities().isEmpty(), "Expected search results"));
            assertEquals(results.stream().map(r -> r.getEntities().size()).distinct().count(), 1,
                    String.format("Expected all result counts to match after stemming. %s", testSet));
        });
    }

    @Test
    public void testStemmingOverride() throws IOException {
        Set<String> testSet = Set.of("customer", "customers");

        Set<SearchResult> results = testSet.stream()
                .map(test -> search(searchService, test))
                .collect(Collectors.toSet());

        results.forEach(r -> assertTrue(r.hasEntities() && !r.getEntities().isEmpty(), "Expected search results"));
        assertEquals(results.stream().map(r -> r.getEntities().size()).distinct().count(), 1,
                String.format("Expected all result counts to match after stemming. %s", testSet));

        // Additional inspect token
        AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "word_delimited",
                "customers"
        );

        List<String> tokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(tokens, List.of("customer"), "Expected `customer` and not `custom`");
    }

    @Test
    public void testDelimitedSynonym() throws IOException {
        List<String> expectedTokens = List.of("cac");
        List<String> analyzers = List.of(
                "urn_component",
                "word_delimited",
                "query_urn_component",
                "query_word_delimited"
        );
        List<String> testTexts = List.of(
                "customer acquisition cost",
                "cac",
                "urn:li:dataset:(urn:li:dataPlatform:testsynonym,cac_table,TEST)"
        );

        for (String analyzer : analyzers) {
            for (String text : testTexts) {
                AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
                        "smpldat_datasetindex_v2",
                        analyzer, text
                );
                List<String> tokens = getTokens(request)
                        .map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
                expectedTokens.forEach(expected -> assertTrue(tokens.contains(expected),
                        String.format("Analyzer: `%s` Text: `%s` - Expected token `%s` in tokens: %s",
                                analyzer, text, expected, tokens)));
            }
        }

        // {"urn":"urn:li:dataset:(urn:li:dataPlatform:testsynonym,cac_table,TEST)","id":"cac_table",...
        List<String> testSet = List.of(
                "cac",
                "customer acquisition cost"
        );
        List<Integer> resultCounts = testSet.stream().map(q -> {
            SearchResult result = search(searchService, q);
            assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                    "Expected search results for: " + q);
            return result.getEntities().size();
        }).collect(Collectors.toList());
    }

    @Test
    public void testUrnSynonym() throws IOException {
        List<String> expectedTokens = List.of("bigquery");

        AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "urn_component",
                "urn:li:dataset:(urn:li:dataPlatform:bigquery,harshal-playground-306419.bq_audit.cloudaudit_googleapis_com_activity,PROD)"
        );
        List<String> indexTokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        expectedTokens.forEach(expected -> assertTrue(indexTokens.contains(expected),
                String.format("Expected token `%s` in %s", expected, indexTokens)));

        request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "query_urn_component",
                "big query"
        );
        List<String> queryTokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(queryTokens, List.of("big query", "big", "query", "bigquery"));

        List<String> testSet = List.of(
                "bigquery",
                "big query"
        );
        List<SearchResult> results = testSet.stream().map(query -> {
            SearchResult result = search(searchService, query);
            assertTrue(result.hasEntities() && !result.getEntities().isEmpty(), "Expected search results for: " + query);
            return result;
        }).collect(Collectors.toList());

        assertEquals(results.stream().map(r -> r.getEntities().size()).distinct().count(), 1,
                String.format("Expected all result counts (%s) to match after synonyms. %s", results, testSet));
        Assert.assertArrayEquals(results.get(0).getEntities().stream().map(e -> e.getEntity().toString()).sorted().toArray(String[]::new),
                results.get(1).getEntities().stream().map(e -> e.getEntity().toString()).sorted().toArray(String[]::new));
    }

    @Test
    public void testTokenization() throws IOException {
        AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "word_delimited",
                "my_table"
        );
        List<String> tokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(tokens, List.of("my_tabl"),
                String.format("Unexpected tokens. Found %s", tokens));

        request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "urn_component",
                "my_table"
        );
        tokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(tokens, List.of("my_tabl"),
                String.format("Unexpected tokens. Found %s", tokens));
    }

    @Test
    public void testTokenizationWithNumber() throws IOException {
        AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "word_delimited",
                "harshal-playground-306419.test_schema.austin311_derived"
        );
        List<String> tokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(tokens, List.of(
                "harshal-playground-306419", "harshal", "playground", "306419",
                 "test_schema", "test", "schema",
                 "austin311_deriv", "austin311", "deriv"),
                String.format("Unexpected tokens. Found %s", tokens));

        request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "urn_component",
                "harshal-playground-306419.test_schema.austin311_derived"
        );
        tokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(tokens, List.of(
                        "harshal-playground-306419", "harshal", "playground", "306419",
                        "test_schema", "test", "schema",
                        "austin311_deriv", "austin311", "deriv"),
                String.format("Unexpected tokens. Found %s", tokens));
    }

    @Test
    public void testTokenizationQuote() throws IOException {
        String testQuery = "\"test2\"";

        AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "urn_component",
                testQuery
        );
        List<String> tokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(tokens, List.of("test2"), String.format("Unexpected tokens. Found %s", tokens));

        request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "query_urn_component",
                testQuery
        );
        tokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(tokens, List.of("test2"), String.format("Unexpected tokens. Found %s", tokens));
    }

    @Test
    public void testTokenizationQuoteUnderscore() throws IOException {
        String testQuery = "\"raw_orders\"";

        AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "word_delimited",
                testQuery
        );
        List<String> tokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(tokens, List.of("raw_orders", "raw_ord", "raw", "order"), String.format("Unexpected tokens. Found %s", tokens));

        request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "query_word_delimited",
                testQuery
        );
        tokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(tokens, List.of("raw_orders", "raw_ord", "raw", "order"), String.format("Unexpected tokens. Found %s", tokens));

        request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "quote_analyzer",
                testQuery
        );
        tokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(tokens, List.of("raw_orders"), String.format("Unexpected tokens. Found %s", tokens));
    }

    @Test
    public void testTokenizationDataPlatform() throws IOException {
        AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "urn_component",
                "urn:li:dataset:(urn:li:dataPlatform:bigquery,harshal-playground-306419.test_schema.excess_deaths_derived,PROD)"
        );
        List<String> tokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(tokens, List.of(
                        "dataset",
                        "dataplatform", "data platform", "bigquery", "big", "query",
                        "harshal-playground-306419", "harshal", "playground", "306419",
                        "test_schema", "test", "schema",
                        "excess_deaths_deriv", "excess", "death", "deriv",
                        "prod", "production"),
                String.format("Unexpected tokens. Found %s", tokens));

        request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "urn_component",
                "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset-ac611929-c3ac-4b92-aafb-f4603ddb408a,PROD)"
        );
        tokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(tokens, List.of(
                        "dataset",
                        "dataplatform", "data platform",  "hive",
                        "samplehivedataset-ac611929-c3ac-4b92-aafb-f4603ddb408a",
                        "samplehivedataset", "ac611929", "c3ac", "4b92", "aafb", "f4603ddb408a",
                        "prod", "production"),
                String.format("Unexpected tokens. Found %s", tokens));

        request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "urn_component",
                "urn:li:dataset:(urn:li:dataPlatform:test_rollback,rollback_test_dataset,TEST)"
        );
        tokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(tokens, List.of(
                        "dataset",
                        "dataplatform", "data platform",
                        "test_rollback", "test", "rollback", "rollback_test_dataset"),
                String.format("Unexpected tokens. Found %s", tokens));
    }

    @Test
    public void testChartAutoComplete() throws InterruptedException, IOException {
        // Two charts exist Baz Chart 1 & Baz Chart 2
        List.of("B", "Ba", "Baz", "Baz ", "Baz C", "Baz Ch", "Baz Cha", "Baz Char", "Baz Chart", "Baz Chart ")
                .forEach(query -> {
                    try {
                        AutoCompleteResults result = autocomplete(new ChartType(entityClient), query);
                        assertTrue(result.getEntities().size() == 2,
                                String.format("Expected 2 results for `%s` found %s", query, result.getEntities().size()));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    public void testDatasetAutoComplete() {
        List.of("excess", "excess_", "excess_d", "excess_de", "excess_death", "excess_deaths", "excess_deaths_d",
                        "excess_deaths_de", "excess_deaths_der", "excess_deaths_derived")
                .forEach(query -> {
                    try {
                        AutoCompleteResults result = autocomplete(new DatasetType(entityClient), query);
                        assertTrue(result.getEntities().size() >= 1,
                                String.format("Expected >= 1 results for `%s` found %s", query, result.getEntities().size()));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    public void testContainerAutoComplete() {
        List.of("cont", "container", "container-a", "container-auto", "container-autocomp", "container-autocomp-te",
                        "container-autocomp-test")
                .forEach(query -> {
                    try {
                        AutoCompleteResults result = autocomplete(new ContainerType(entityClient), query);
                        assertTrue(result.getEntities().size() >= 1,
                                String.format("Expected >= 1 results for `%s` found %s", query, result.getEntities().size()));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    public void testGroupAutoComplete() {
        List.of("T", "Te", "Tes", "Test ", "Test G", "Test Gro", "Test Group ")
                .forEach(query -> {
                    try {
                        AutoCompleteResults result = autocomplete(new CorpGroupType(entityClient), query);
                        assertTrue(result.getEntities().size() == 1,
                                String.format("Expected 1 results for `%s` found %s", query, result.getEntities().size()));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    public void testUserAutoComplete() {
        List.of("D", "Da", "Dat", "Data ", "Data H", "Data Hu", "Data Hub", "Data Hub ")
                .forEach(query -> {
                    try {
                        AutoCompleteResults result = autocomplete(new CorpUserType(entityClient, null), query);
                        assertTrue(result.getEntities().size() >= 1,
                                String.format("Expected at least 1 results for `%s` found %s", query, result.getEntities().size()));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    public void testSmokeTestQueries() {
        Map<String, Integer> expectedFulltextMinimums = Map.of(
                "sample", 3,
                "covid", 2,
                "\"raw_orders\"", 1,
                STRUCTURED_QUERY_PREFIX + "sample", 1,
                STRUCTURED_QUERY_PREFIX + "covid", 0,
                STRUCTURED_QUERY_PREFIX + "\"raw_orders\"", 1
        );

        Map<String, SearchResult> results = expectedFulltextMinimums.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> search(searchService, entry.getKey())));

        results.forEach((key, value) -> {
            Integer actualCount = value.getEntities().size();
            Integer expectedCount = expectedFulltextMinimums.get(key);
            assertSame(actualCount, expectedCount,
                    String.format("Search term `%s` has %s fulltext results, expected %s results.", key, actualCount,
                            expectedCount));
        });

        Map<String, Integer> expectedStructuredMinimums = Map.of(
                "sample", 3,
                "covid", 2,
                "\"raw_orders\"", 1
        );

        results = expectedStructuredMinimums.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> searchStructured(searchService, entry.getKey())));

        results.forEach((key, value) -> {
            Integer actualCount = value.getEntities().size();
            Integer expectedCount = expectedStructuredMinimums.get(key);
            assertSame(actualCount, expectedCount,
                    String.format("Search term `%s` has %s structured results, expected %s results.", key, actualCount,
                            expectedCount));
        });
    }

    @Test
    public void testMinNumberLengthLimit() throws IOException {
        AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "word_delimited",
                "data2022.data22"
        );
        List<String> expected = List.of("data2022", "data22");
        List<String> actual = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(actual, expected,
                String.format("Expected: %s Actual: %s", expected, actual));
    }

    @Test
    public void testFacets() {
        Set<String> expectedFacets = Set.of("entity", "typeNames", "platform", "origin", "tags");
        SearchResult testResult = search(searchService, "cypress");
        expectedFacets.forEach(facet -> {
            assertTrue(testResult.getMetadata().getAggregations().stream().anyMatch(agg -> agg.getName().equals(facet)),
                    String.format("Failed to find facet `%s` in %s", facet,
                            testResult.getMetadata().getAggregations().stream()
                                    .map(AggregationMetadata::getName).collect(Collectors.toList())));
        });
    }

    @Test
    public void testPartialUrns() throws IOException {
        Set<String> expectedQueryTokens = Set.of("dataplatform", "data platform", "samplehdfsdataset", "prod", "production");
        Set<String> expectedIndexTokens = Set.of("dataplatform", "data platform", "hdfs", "samplehdfsdataset", "prod", "production");

        AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "query_urn_component",
                ":(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"
        );
        List<String> searchQueryTokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        expectedQueryTokens.forEach(expected -> assertTrue(searchQueryTokens.contains(expected),
                String.format("Expected token `%s` in %s", expected, searchQueryTokens)));

        request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "urn_component",
                ":(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"
        );
        List<String> searchIndexTokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        expectedIndexTokens.forEach(expected -> assertTrue(searchIndexTokens.contains(expected),
                String.format("Expected token `%s` in %s", expected, searchIndexTokens)));
    }

    @Test
    public void testPartialUnderscoreUrns() throws IOException {
        String testQuery = ":(urn:li:dataPlatform:hdfs,party_email,PROD)";
        Set<String> expectedQueryTokens = Set.of("dataplatform", "data platform", "hdfs", "party_email", "parti",
                "email", "prod", "production");
        Set<String> expectedIndexTokens = Set.of("dataplatform", "data platform", "hdfs", "party_email", "parti",
                "email", "prod", "production");

        AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "query_urn_component",
                testQuery
        );
        List<String> searchQueryTokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        expectedQueryTokens.forEach(expected -> assertTrue(searchQueryTokens.contains(expected),
                String.format("Expected token `%s` in %s", expected, searchQueryTokens)));

        request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "urn_component",
                testQuery
        );
        List<String> searchIndexTokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        expectedIndexTokens.forEach(expected -> assertTrue(searchIndexTokens.contains(expected),
                String.format("Expected token `%s` in %s", expected, searchIndexTokens)));
    }

    @Test
    public void testQuotedAnalyzer() throws IOException {
        AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "quote_analyzer",
                "\"party_email\""
        );
        List<String> searchQuotedQueryTokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(List.of("party_email"), searchQuotedQueryTokens, String.format("Actual %s", searchQuotedQueryTokens));

        request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "quote_analyzer",
                "\"test2\""
        );
        searchQuotedQueryTokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(List.of("test2"), searchQuotedQueryTokens);

        request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "quote_analyzer",
                "\"party_email\""
        );
        searchQuotedQueryTokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(List.of("party_email"), searchQuotedQueryTokens);

        request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "quote_analyzer",
                "\"test2\""
        );
        searchQuotedQueryTokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(List.of("test2"), searchQuotedQueryTokens);

        request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "quote_analyzer",
                "\"test_BYTES_LIST_feature\""
        );
        searchQuotedQueryTokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(List.of("test_bytes_list_feature"), searchQuotedQueryTokens);

        request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "query_word_delimited",
                "test_BYTES_LIST_feature"
        );
        searchQuotedQueryTokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertTrue(searchQuotedQueryTokens.contains("test_bytes_list_featur"));
    }

    @Test
    public void testFragmentUrns() {
        List<String> testSet = List.of(
                "hdfs,SampleHdfsDataset,PROD",
                "hdfs,SampleHdfsDataset",
                "SampleHdfsDataset",
                "(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
                "urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD",
                "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
                ":(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"
        );

        testSet.forEach(query -> {
            SearchResult result = search(searchService, query);

            assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                    String.format("%s - Expected partial urn search results", query));
            assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                    String.format("%s - Expected search results to include matched fields", query));
        });
    }

    @Test
    public void testPlatformTest() {
        List<String> testFields = List.of("platform.keyword", "platform");
        final String testPlatform = "urn:li:dataPlatform:dbt";

        // Ensure backend code path works as expected
        List<SearchResult> results = testFields.stream()
                .map(fieldName -> {
                    final String query = String.format("%s:%s", fieldName, testPlatform.replaceAll(":", "\\\\:"));
                    SearchResult result = searchStructured(searchService, query);
                    assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                            String.format("%s - Expected search results", query));
                    assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                            String.format("%s - Expected search results to include matched fields", query));
                    return result;
                })
                .collect(Collectors.toList());

        IntStream.range(0, testFields.size()).forEach(idx -> {
            assertEquals(results.get(idx).getEntities().size(), 9,
                    String.format("Search results for fields `%s` != 9", testFields.get(idx)));
        });

        // Construct problematic search entity query
        List<Filter> testFilters = testFields.stream()
                .map(fieldName -> {
                    Filter filter = new Filter();
                    ArrayList<Criterion> criteria = new ArrayList<>();
                    Criterion hasPlatformCriterion = new Criterion().setField(fieldName).setCondition(Condition.EQUAL).setValue(testPlatform);
                    criteria.add(hasPlatformCriterion);
                    filter.setOr(new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(new CriterionArray(criteria))));
                    return filter;
                }).collect(Collectors.toList());

        // Test variations of fulltext flags
        for (Boolean fulltextFlag : List.of(true, false)) {

            // Test field variations with/without .keyword
            List<SearchResult> entityClientResults = testFilters.stream().map(filter -> {
                try {
                    return entityClient.search("dataset", "*", filter, null, 0, 100,
                            AUTHENTICATION, fulltextFlag, null);
                } catch (RemoteInvocationException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList());

            IntStream.range(0, testFields.size()).forEach(idx -> {
                assertEquals(entityClientResults.get(idx).getEntities().size(), 9,
                        String.format("Search results for entityClient fields (fulltextFlag: %s): `%s` != 9", fulltextFlag, testFields.get(idx)));
            });
        }
    }

    public void testStructQueryFieldMatch() {
        String query = STRUCTURED_QUERY_PREFIX + "name: customers";
        SearchResult result = search(searchService, query);

        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));

        assertEquals(result.getEntities().size(), 1);
    }

    @Test
    public void testStructQueryFieldPrefixMatch() {
        String query = STRUCTURED_QUERY_PREFIX + "name: customers*";
        SearchResult result = search(searchService, query);

        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));

        assertEquals(result.getEntities().size(), 2);
    }

    @Test
    public void testStructQueryCustomPropertiesKeyPrefix() {
        String query = STRUCTURED_QUERY_PREFIX + "customProperties: node_type=*";
        SearchResult result = search(searchService, query);

        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));

        assertEquals(result.getEntities().size(), 9);
    }

    @Test
    public void testStructQueryCustomPropertiesMatch() {
        String query = STRUCTURED_QUERY_PREFIX + "customProperties: node_type=model";
        SearchResult result = search(searchService, query);

        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));

        assertEquals(result.getEntities().size(), 5);
    }

    @Test
    public void testStructQueryFieldPaths() {
        String query = STRUCTURED_QUERY_PREFIX + "fieldPaths: customer_id";
        SearchResult result = search(searchService, query);

        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));

        assertEquals(result.getEntities().size(), 3);
    }

    @Test
    public void testStructQueryBoolean() {
        String query = STRUCTURED_QUERY_PREFIX + "editedFieldTags:urn\\:li\\:tag\\:Legacy OR tags:urn\\:li\\:tag\\:testTag";
        SearchResult result = search(searchService, query);

        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));

        assertEquals(result.getEntities().size(), 2);

        query = STRUCTURED_QUERY_PREFIX + "editedFieldTags:urn\\:li\\:tag\\:Legacy";
        result = search(searchService, query);

        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));

        assertEquals(result.getEntities().size(), 1);

        query = STRUCTURED_QUERY_PREFIX + "tags:urn\\:li\\:tag\\:testTag";
        result = search(searchService, query);

        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));

        assertEquals(result.getEntities().size(), 1);
    }

    @Test
    public void testStructQueryBrowsePaths() {
        String query = STRUCTURED_QUERY_PREFIX + "browsePaths:*/dbt/*";
        SearchResult result = search(searchService, query);

        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));

        assertEquals(result.getEntities().size(), 9);
    }

    @Test
    public void testOr() {
        String query = "stg_customers | logging_events";
        SearchResult result = search(searchService, query);
        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));
        assertEquals(result.getEntities().size(), 9);

        query = "stg_customers";
        result = search(searchService, query);
        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));
        assertEquals(result.getEntities().size(), 1);

        query = "logging_events";
        result = search(searchService, query);
        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));
        assertEquals(result.getEntities().size(), 8);
    }

    @Test
    public void testNegate() {
        String query = "logging_events -bckp";
        SearchResult result = search(searchService, query);
        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));
        assertEquals(result.getEntities().size(), 7);

        query = "logging_events";
        result = search(searchService, query);
        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));
        assertEquals(result.getEntities().size(), 8);
    }

    @Test
    public void testPrefix() {
        String query = "bigquery";
        SearchResult result = search(searchService, query);
        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));
        assertEquals(result.getEntities().size(), 8);

        query = "big*";
        result = search(searchService, query);
        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));
        assertEquals(result.getEntities().size(), 8);
    }

    @Test
    public void testParens() {
        String query = "dbt | (bigquery + covid19)";
        SearchResult result = search(searchService, query);
        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));
        assertEquals(result.getEntities().size(), 11);

        query = "dbt";
        result = search(searchService, query);
        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));
        assertEquals(result.getEntities().size(), 9);

        query = "bigquery + covid19";
        result = search(searchService, query);
        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));
        assertEquals(result.getEntities().size(), 2);

        query = "bigquery";
        result = search(searchService, query);
        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));
        assertEquals(result.getEntities().size(), 8);

        query = "covid19";
        result = search(searchService, query);
        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));
        assertEquals(result.getEntities().size(), 2);
    }

    @Test
    public void testPrefixVsExact() {
        String query = "\"customers\"";
        SearchResult result = search(searchService, query);

        assertTrue(result.hasEntities() && !result.getEntities().isEmpty(),
                String.format("%s - Expected search results", query));
        assertTrue(result.getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", query));

        assertEquals(result.getEntities().size(), 2);
        assertEquals(result.getEntities().get(0).getEntity().toString(),
                "urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.customers,PROD)",
                "Expected exact match");
    }

    private Stream<AnalyzeResponse.AnalyzeToken> getTokens(AnalyzeRequest request) throws IOException {
        return _searchClient.indices().analyze(request, RequestOptions.DEFAULT).getTokens().stream();
    }
}

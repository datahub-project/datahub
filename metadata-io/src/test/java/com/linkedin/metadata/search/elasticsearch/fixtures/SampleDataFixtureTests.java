package com.linkedin.metadata.search.elasticsearch.fixtures;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.types.chart.ChartType;
import com.linkedin.datahub.graphql.types.dataset.DatasetType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.ESSampleDataFixture;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;
import java.util.HashMap;

import com.linkedin.util.Pair;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.linkedin.metadata.ESTestUtils.autocomplete;
import static com.linkedin.metadata.ESTestUtils.search;
import static com.linkedin.metadata.ESTestUtils.searchStructured;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNotNull;


@Import(ESSampleDataFixture.class)
public class SampleDataFixtureTests extends AbstractTestNGSpringContextTests {

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
                "dataset", 8,
                "chart", 0,
                "container", 0,
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
        List<String> expectedTokens = List.of("cac", "customer", "acquisit", "cost");

        AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "word_delimited",
                "mydatabase.myschema.cac_table"
        );
        List<String> indexTokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        // synonyms expected at query time
        expectedTokens.forEach(expected -> assertTrue(indexTokens.contains(expected),
                        String.format("Expected token `%s` in %s", expected, indexTokens)));

        request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "query_word_delimited",
                "customer acquisition cost"
        );
        List<String> searchTokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        expectedTokens.forEach(expected -> assertTrue(searchTokens.contains(expected),
                String.format("Expected token `%s` in %s", expected, searchTokens)));

        // {"urn":"urn:li:dataset:(urn:li:dataPlatform:test_synonym,cac_table,TEST)","id":"cac_table",...
        List<String> testSet = List.of(
                "cac",
                "customer acquisition cost"
        );
        List<SearchResult> results = testSet.stream()
                .map(test -> search(searchService, test))
                .collect(Collectors.toList());

        results.forEach(r -> assertTrue(r.hasEntities() && !r.getEntities().isEmpty(), "Expected search results"));

        List<Integer> resultCounts = results.stream().map(r -> r.getEntities().size()).collect(Collectors.toList());
        assertEquals(resultCounts.stream().distinct().count(), 1,
                String.format("Expected all result counts (%s) to match after synonyms. %s", resultCounts, testSet));
    }

    @Test
    public void testUrnSynonym() throws IOException {
        List<String> expectedTokens = List.of("bigqueri", "big", "queri");

        AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "urn_component",
                "urn:li:dataset:(urn:li:dataPlatform:bigquery,harshal-playground-306419.bq_audit.cloudaudit_googleapis_com_activity,PROD)"
        );
        List<String> indexTokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        expectedTokens.forEach(expected -> assertTrue(indexTokens.contains(expected),
                String.format("Expected token `%s` in %s", expected, indexTokens)));

        List<String> testSet = List.of(
                "big query",
                "bigquery"
        );
        List<SearchResult> results = testSet.stream()
                .map(test -> search(searchService, test))
                .collect(Collectors.toList());

        results.forEach(r -> assertTrue(r.hasEntities() && !r.getEntities().isEmpty(), "Expected search results"));

        Assert.assertArrayEquals(results.get(0).getEntities().stream().map(e -> e.getEntity().toString()).sorted().toArray(String[]::new),
                results.get(1).getEntities().stream().map(e -> e.getEntity().toString()).sorted().toArray(String[]::new));

        List<Integer> resultCounts = results.stream().map(r -> r.getEntities().size()).collect(Collectors.toList());
        assertEquals(resultCounts.stream().distinct().count(), 1,
                String.format("Expected all result counts (%s) to match after synonyms. %s", resultCounts, testSet));
    }

    @Test
    public void testTokenization() throws IOException {
        AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "word_delimited",
                "my_table"
        );
        List<String> tokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(tokens, List.of("my_tabl", "my", "tabl"),
                String.format("Unexpected tokens. Found %s", tokens));

        request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "urn_component",
                "my_table"
        );
        tokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        assertEquals(tokens, List.of("my_tabl", "my", "tabl"),
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
                 "austin311_deriv", "austin311", "deriv", "austin", "311"),
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
                        "austin311_deriv", "austin311", "deriv", "austin", "311"),
                String.format("Unexpected tokens. Found %s", tokens));
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
                        "dataplatform", "data", "platform", "bigqueri", "big", "queri",
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
                        "dataplatform", "data", "platform", "hive",
                        "samplehivedataset-ac611929-c3ac-4b92-aafb-f4603ddb408a",
                        "samplehivedataset", "ac611929", "c3ac", "4b92", "aafb", "f4603ddb408a", "sampl",
                        "ac", "611929", "92", "4603", "ddb", "408",
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
                        "dataplatform", "data", "platform",
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
    public void testSmokeTestQueries() {
        Map<String, Integer> expectedMinimums = Map.of(
                "sample", 3,
                "covid", 1
        );

        Map<String, SearchResult> results = expectedMinimums.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> search(searchService, entry.getKey())));

        results.forEach((key, value) -> {
            Integer actualCount = value.getEntities().size();
            Integer expectedCount = expectedMinimums.get(key);
            assertTrue(actualCount >= expectedCount,
                    String.format("Search term `%s` has %s fulltext results, expected %s results.", key,
                            actualCount, expectedCount));
        });

        results = expectedMinimums.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> searchStructured(searchService, entry.getKey())));

        results.forEach((key, value) -> {
            Integer actualCount = value.getEntities().size();
            Integer expectedCount = expectedMinimums.get(key);
            assertTrue(actualCount >= expectedCount,
                    String.format("Search term `%s` has %s structured results, expected %s results.", key,
                            actualCount, expectedCount));
        });
    }

    @Test
    public void testMinNumberLengthLimit() throws IOException {
        AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "word_delimited",
                "data2022.data22"
        );
        List<String> expected = List.of("data2022", "data", "2022", "data22", "22");
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
        Set<String> expectedTokens = Set.of("dataplatform", "data", "platform", "hdfs", "samplehdfsdataset", "sampl",
                "dataset", "prod", "production");
        AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
                "smpldat_datasetindex_v2",
                "query_urn_component",
                ":(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"
        );
        List<String> searchTokens = getTokens(request).map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
        expectedTokens.forEach(expected -> assertTrue(searchTokens.contains(expected),
                String.format("Expected token `%s` in %s", expected, searchTokens)));

        List<String> testSet = List.of(
                ":(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
                "(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
                "urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD",
                "hdfs,SampleHdfsDataset,PROD",
                "hdfs,SampleHdfsDataset",
                "SampleHdfsDataset",
                "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"
        );
        List<Pair<String, SearchResult>> results = testSet.stream()
                .map(test -> Pair.of(test, search(searchService, test)))
                .collect(Collectors.toList());

        results.forEach(r ->
                assertTrue(r.getValue().hasEntities() && !r.getValue().getEntities().isEmpty(),
                String.format("%s - Expected partial urn search results", r.getKey())));
        results.forEach(r -> assertTrue(r.getValue().getEntities().stream().noneMatch(e -> e.getMatchedFields().isEmpty()),
                String.format("%s - Expected search results to include matched fields", r.getKey())));
    }

    private Stream<AnalyzeResponse.AnalyzeToken> getTokens(AnalyzeRequest request) throws IOException {
        return _searchClient.indices().analyze(request, RequestOptions.DEFAULT).getTokens().stream();
    }
}

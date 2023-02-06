package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.ESTestConfiguration;
import com.linkedin.metadata.config.ElasticSearchConfiguration;
import com.linkedin.metadata.systemmetadata.SystemMetadataMappingsBuilder;
import com.linkedin.metadata.version.GitVersion;
import java.util.Optional;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

@Import(ESTestConfiguration.class)
public class ESIndexBuilderTest extends AbstractTestNGSpringContextTests {

    @Autowired
    private RestHighLevelClient _searchClient;
    private static IndicesClient _indexClient;
    private static final String TEST_INDEX_NAME = "esindex_builder_test";
    private static ESIndexBuilder testDefaultBuilder;


    @BeforeClass
    public void setup() {
        _indexClient = _searchClient.indices();
        GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
        testDefaultBuilder = new ESIndexBuilder(_searchClient, 1, 0, 0,
                0, Map.of(), false, false,
                new ElasticSearchConfiguration(), gitVersion);
    }

    @BeforeMethod
    public static void wipe() throws Exception {
        try {
            _indexClient.getAlias(new GetAliasesRequest(TEST_INDEX_NAME), RequestOptions.DEFAULT)
                    .getAliases().keySet().forEach(index -> {
                        try {
                            _indexClient.delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });

            _indexClient.delete(new DeleteIndexRequest(TEST_INDEX_NAME), RequestOptions.DEFAULT);
        } catch (ElasticsearchException exception) {
            if (exception.status() != RestStatus.NOT_FOUND) {
                throw exception;
            }
        }
    }

    public static GetIndexResponse getTestIndex() throws IOException {
        return _indexClient.get(new GetIndexRequest(TEST_INDEX_NAME).includeDefaults(true), RequestOptions.DEFAULT);
    }

    @Test
    public void testESIndexBuilderCreation() throws Exception {
        GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
        ESIndexBuilder customIndexBuilder = new ESIndexBuilder(_searchClient, 2, 0, 1,
                0, Map.of(), false, false,
                new ElasticSearchConfiguration(), gitVersion);
        customIndexBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
        GetIndexResponse resp = getTestIndex();

        assertEquals("2", resp.getSetting(TEST_INDEX_NAME, "index.number_of_shards"));
        assertEquals("0", resp.getSetting(TEST_INDEX_NAME, "index.number_of_replicas"));
        assertEquals("0s", resp.getSetting(TEST_INDEX_NAME, "index.refresh_interval"));
    }

    @Test
    public void testMappingReindex() throws Exception {
        GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
        ESIndexBuilder enabledMappingReindex = new ESIndexBuilder(_searchClient, 1, 0, 0,
                0, Map.of(), false, true,
                new ElasticSearchConfiguration(), gitVersion);

        // No mappings
        enabledMappingReindex.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
        String beforeCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");

        // add new mappings
        enabledMappingReindex.buildIndex(TEST_INDEX_NAME, SystemMetadataMappingsBuilder.getMappings(), Map.of());

        String afterAddedMappingCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");
        assertEquals(beforeCreationDate, afterAddedMappingCreationDate, "Expected no reindex on *adding* mappings");

        // change mappings
        Map<String, Object> newProps = ((Map<String, Object>) SystemMetadataMappingsBuilder.getMappings().get("properties"))
                .entrySet().stream()
                .map(m -> !m.getKey().equals("urn") ? m
                        : Map.entry("urn", ImmutableMap.<String, Object>builder().put("type", "wildcard").build()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        enabledMappingReindex.buildIndex(TEST_INDEX_NAME, Map.of("properties", newProps), Map.of());

        assertTrue(Arrays.stream(getTestIndex().getIndices()).noneMatch(name -> name.equals(TEST_INDEX_NAME)),
                "Expected original index to be replaced with alias");

        Map.Entry<String, List<AliasMetadata>> newIndex = getTestIndex().getAliases().entrySet().stream()
                .filter(e -> e.getValue().stream().anyMatch(aliasMeta -> aliasMeta.alias().equals(TEST_INDEX_NAME)))
                .findFirst().get();
        String afterChangedMappingCreationDate = getTestIndex().getSetting(newIndex.getKey(), "index.creation_date");
        assertNotEquals(beforeCreationDate, afterChangedMappingCreationDate, "Expected reindex on *changing* mappings");
    }

    @Test
    public void testSettingsNumberOfShardsReindex() throws Exception {
        // Set test defaults
        testDefaultBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
        assertEquals("1", getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_shards"));
        String beforeCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");

        String expectedShards = "5";
        GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
        ESIndexBuilder changedShardBuilder = new ESIndexBuilder(_searchClient,
                Integer.parseInt(expectedShards),
                testDefaultBuilder.getNumReplicas(),
                testDefaultBuilder.getNumRetries(),
                testDefaultBuilder.getRefreshIntervalSeconds(),
                Map.of(),
                true, false,
                new ElasticSearchConfiguration(), gitVersion);

        // add new shard setting
        changedShardBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
        assertTrue(Arrays.stream(getTestIndex().getIndices()).noneMatch(name -> name.equals(TEST_INDEX_NAME)),
                    "Expected original index to be replaced with alias");

        Map.Entry<String, List<AliasMetadata>> newIndex = getTestIndex().getAliases().entrySet().stream()
                .filter(e -> e.getValue().stream().anyMatch(aliasMeta -> aliasMeta.alias().equals(TEST_INDEX_NAME)))
                .findFirst().get();

        String afterCreationDate = getTestIndex().getSetting(newIndex.getKey(), "index.creation_date");
        assertNotEquals(beforeCreationDate, afterCreationDate, "Expected reindex to result in different timestamp");
        assertEquals(expectedShards, getTestIndex().getSetting(newIndex.getKey(), "index.number_of_shards"),
                "Expected number of shards: " + expectedShards);
    }

    @Test
    public void testSettingsNoReindex() throws Exception {
        GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
        List<ESIndexBuilder> noReindexBuilders = List.of(
                new ESIndexBuilder(_searchClient,
                        testDefaultBuilder.getNumShards(),
                        testDefaultBuilder.getNumReplicas() + 1,
                        testDefaultBuilder.getNumRetries(),
                        testDefaultBuilder.getRefreshIntervalSeconds(),
                        Map.of(),
                        true, false,
                        new ElasticSearchConfiguration(), gitVersion),
                new ESIndexBuilder(_searchClient,
                        testDefaultBuilder.getNumShards(),
                        testDefaultBuilder.getNumReplicas(),
                        testDefaultBuilder.getNumRetries(),
                        testDefaultBuilder.getRefreshIntervalSeconds() + 10,
                        Map.of(),
                        true, false,
                        new ElasticSearchConfiguration(), gitVersion),
               new ESIndexBuilder(_searchClient,
                                testDefaultBuilder.getNumShards() + 1,
                                testDefaultBuilder.getNumReplicas(),
                                testDefaultBuilder.getNumRetries(),
                                testDefaultBuilder.getRefreshIntervalSeconds(),
                                Map.of(),
                                false, false,
                       new ElasticSearchConfiguration(), gitVersion),
                new ESIndexBuilder(_searchClient,
                        testDefaultBuilder.getNumShards(),
                        testDefaultBuilder.getNumReplicas() + 1,
                        testDefaultBuilder.getNumRetries(),
                        testDefaultBuilder.getRefreshIntervalSeconds(),
                        Map.of(),
                        false, false,
                        new ElasticSearchConfiguration(), gitVersion)
        );

        for (ESIndexBuilder builder : noReindexBuilders) {
            // Set test defaults
            testDefaultBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
            assertEquals("0", getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"));
            assertEquals("0s", getTestIndex().getSetting(TEST_INDEX_NAME, "index.refresh_interval"));
            String beforeCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");

            // build index with builder
            builder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
            assertTrue(Arrays.asList(getTestIndex().getIndices()).contains(TEST_INDEX_NAME),
                    "Expected original index to remain");
            String afterCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");

            assertEquals(beforeCreationDate, afterCreationDate, "Expected no difference in index timestamp");
            assertEquals(String.valueOf(builder.getNumReplicas()), getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"));
            assertEquals(builder.getRefreshIntervalSeconds() + "s", getTestIndex().getSetting(TEST_INDEX_NAME, "index.refresh_interval"));

            wipe();
        }
    }

}

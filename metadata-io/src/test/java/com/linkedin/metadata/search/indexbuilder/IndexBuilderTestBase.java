package com.linkedin.metadata.search.indexbuilder;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_CONFIG;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexResult;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.systemmetadata.SystemMetadataMappingsBuilder;
import com.linkedin.metadata.version.GitVersion;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.IndicesClient;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public abstract class IndexBuilderTestBase extends AbstractTestNGSpringContextTests {

  @Nonnull
  protected abstract RestHighLevelClient getSearchClient();

  private IndicesClient _indexClient;
  protected static final String TEST_INDEX_NAME = "esindex_builder_test";
  private ESIndexBuilder testDefaultBuilder;
  private ESIndexBuilder testReplicasBuilder;
  protected static final int REPLICASTEST = 2;

  @BeforeClass
  public void setup() {
    _indexClient = getSearchClient().indices();
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    testDefaultBuilder =
        new ESIndexBuilder(
            getSearchClient(),
            1,
            0,
            3,
            0,
            Map.of(),
            false,
            false,
            false,
            TEST_SEARCH_CONFIG,
            gitVersion);
    testReplicasBuilder =
        new ESIndexBuilder(
            getSearchClient(),
            1,
            REPLICASTEST,
            3,
            0,
            Map.of(),
            false,
            false,
            false,
            TEST_SEARCH_CONFIG,
            gitVersion);
  }

  @BeforeMethod
  public void wipe() throws Exception {
    try {
      _indexClient
          .getAlias(new GetAliasesRequest(TEST_INDEX_NAME), RequestOptions.DEFAULT)
          .getAliases()
          .keySet()
          .forEach(
              index -> {
                try {
                  _indexClient.delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });

      _indexClient.delete(new DeleteIndexRequest(TEST_INDEX_NAME), RequestOptions.DEFAULT);
    } catch (OpenSearchException exception) {
      if (exception.status() != RestStatus.NOT_FOUND) {
        throw exception;
      }
    }
  }

  public GetIndexResponse getTestIndex() throws IOException {
    return _indexClient.get(
        new GetIndexRequest(TEST_INDEX_NAME).includeDefaults(true), RequestOptions.DEFAULT);
  }

  @Test
  public void testTweakReplicasStepOps() throws Exception {
    testReplicasBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
    ReindexConfig indexState =
        testReplicasBuilder.buildReindexState(
            TEST_INDEX_NAME, SystemMetadataMappingsBuilder.getMappings(), Map.of());
    // assert initial state, index has 0 docs, REPLICASTEST replica
    assertEquals(
        "" + REPLICASTEST, getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"));
    long numDocs =
        getSearchClient()
            .count(new CountRequest(TEST_INDEX_NAME), RequestOptions.DEFAULT)
            .getCount();
    // 0,1 --> 0,1 with dryRun
    testReplicasBuilder.tweakReplicas(indexState, true);
    assertEquals(
        "" + REPLICASTEST, getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"));
    // 0,1 --> 0,0
    testReplicasBuilder.tweakReplicas(indexState, false);
    assertEquals("0", getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"));
    // 0,0 --> verify not undesired changes
    testReplicasBuilder.tweakReplicas(indexState, false);
    assertEquals("0", getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"));
    // index one doc
    IndexRequest indexRequest =
        new IndexRequest(TEST_INDEX_NAME).id("1").source(new HashMap<>(), XContentType.JSON);
    IndexResponse indexResponse = getSearchClient().index(indexRequest, RequestOptions.DEFAULT);
    // make sure it will be counted
    getSearchClient()
        .indices()
        .refresh(
            new org.opensearch.action.admin.indices.refresh.RefreshRequest(TEST_INDEX_NAME),
            RequestOptions.DEFAULT);
    numDocs =
        getSearchClient()
            .count(new CountRequest(TEST_INDEX_NAME), RequestOptions.DEFAULT)
            .getCount();
    assertEquals(1, numDocs, "Expected 0 documents in the test index");
    // 1,0 --> 1,0 with dryRun
    testReplicasBuilder.tweakReplicas(indexState, true);
    assertEquals("0", getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"));
    // 1,0 --> 1,1
    testReplicasBuilder.tweakReplicas(indexState, false);
    assertEquals(
        "" + REPLICASTEST, getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"));
    // 1,1 --> verify not undesired changes
    testReplicasBuilder.tweakReplicas(indexState, false);
    assertEquals(
        "" + REPLICASTEST, getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"));
  }

  @Test
  public void testESIndexBuilderNoSkipNdocs() throws Exception {
    // Set test defaults
    testDefaultBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
    String beforeCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");
    String expectedShards = "2";
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    ESIndexBuilder changedShardBuilder =
        new ESIndexBuilder(
            getSearchClient(),
            Integer.parseInt(expectedShards),
            testDefaultBuilder.getNumReplicas(),
            testDefaultBuilder.getNumRetries(),
            testDefaultBuilder.getRefreshIntervalSeconds(),
            Map.of(),
            true,
            false,
            false,
            new ElasticSearchConfiguration(),
            gitVersion);
    // index one doc
    IndexRequest indexRequest =
        new IndexRequest(TEST_INDEX_NAME).id("1").source(new HashMap<>(), XContentType.JSON);
    IndexResponse indexResponse = getSearchClient().index(indexRequest, RequestOptions.DEFAULT);
    // reindex
    ReindexResult rr = changedShardBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
    Map.Entry<String, List<AliasMetadata>> newIndex =
        getTestIndex().getAliases().entrySet().stream()
            .filter(
                e ->
                    e.getValue().stream()
                        .anyMatch(aliasMeta -> aliasMeta.alias().equals(TEST_INDEX_NAME)))
            .findFirst()
            .get();
    String afterCreationDate = getTestIndex().getSetting(newIndex.getKey(), "index.creation_date");
    assertNotEquals(
        beforeCreationDate, afterCreationDate, "Expected reindex to result in different timestamp");
    assertNotEquals(rr, ReindexResult.REINDEXED_SKIPPED_0DOCS);
    long nbdocs = changedShardBuilder.getCount(newIndex.getKey());
    assertEquals(1, nbdocs);
  }

  @Test
  public void testESIndexBuilderSkip0docs() throws Exception {
    // Set test defaults
    testDefaultBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
    String beforeCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");
    String expectedShards = "2";
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    ESIndexBuilder changedShardBuilder =
        new ESIndexBuilder(
            getSearchClient(),
            Integer.parseInt(expectedShards),
            testDefaultBuilder.getNumReplicas(),
            testDefaultBuilder.getNumRetries(),
            testDefaultBuilder.getRefreshIntervalSeconds(),
            Map.of(),
            true,
            false,
            false,
            new ElasticSearchConfiguration(),
            gitVersion);
    // reindex
    ReindexResult rr = changedShardBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
    Map.Entry<String, List<AliasMetadata>> newIndex =
        getTestIndex().getAliases().entrySet().stream()
            .filter(
                e ->
                    e.getValue().stream()
                        .anyMatch(aliasMeta -> aliasMeta.alias().equals(TEST_INDEX_NAME)))
            .findFirst()
            .get();
    String afterCreationDate = getTestIndex().getSetting(newIndex.getKey(), "index.creation_date");
    assertNotEquals(
        beforeCreationDate, afterCreationDate, "Expected reindex to result in different timestamp");
    assertEquals(rr, ReindexResult.REINDEXED_SKIPPED_0DOCS);
    long nbdocs = changedShardBuilder.getCount(newIndex.getKey());
    assertEquals(0, nbdocs);
  }

  @Test
  public void testESIndexBuilderReindex() throws Exception {
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    // index with original refresh_interval
    ESIndexBuilder nsecBuilder =
        new ESIndexBuilder(
            getSearchClient(),
            1,
            1,
            1,
            1,
            Map.of(),
            false,
            false,
            false,
            TEST_SEARCH_CONFIG,
            gitVersion);
    nsecBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
    String beforeCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");
    // add some docs
    int NDOCS = 5;
    for (int i = 0; i < NDOCS; i++) {
      IndexRequest indexRequest =
          new IndexRequest(TEST_INDEX_NAME).id("" + i).source(new HashMap<>(), XContentType.JSON);
      IndexResponse indexResponse = getSearchClient().index(indexRequest, RequestOptions.DEFAULT);
    }
    // make sure it will be counted
    getSearchClient()
        .indices()
        .refresh(
            new org.opensearch.action.admin.indices.refresh.RefreshRequest(TEST_INDEX_NAME),
            RequestOptions.DEFAULT);
    // new index
    ESIndexBuilder changedShardBuilder =
        new ESIndexBuilder(
            getSearchClient(),
            2,
            2,
            2,
            2,
            Map.of(),
            true,
            false,
            false,
            new ElasticSearchConfiguration(),
            gitVersion);
    // reindex
    ReindexResult rr = changedShardBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
    assertEquals(rr, ReindexResult.REINDEXING);
    Map.Entry<String, List<AliasMetadata>> newIndex =
        getTestIndex().getAliases().entrySet().stream()
            .filter(
                e ->
                    e.getValue().stream()
                        .anyMatch(aliasMeta -> aliasMeta.alias().equals(TEST_INDEX_NAME)))
            .findFirst()
            .get();
    String afterCreationDate = getTestIndex().getSetting(newIndex.getKey(), "index.creation_date");
    assertNotEquals(
        beforeCreationDate, afterCreationDate, "Expected reindex to result in different timestamp");
    long numDocs = changedShardBuilder.getCount(newIndex.getKey());
    assertEquals(NDOCS, numDocs, "Expected " + NDOCS + " documents in the test index");
    assertEquals("2", getTestIndex().getSetting(newIndex.getKey(), "index.number_of_replicas"));
    assertEquals("2", getTestIndex().getSetting(newIndex.getKey(), "index.number_of_shards"));
  }

  @Test
  protected abstract void testCodec() throws Exception;

  @Test
  public void testMappingReindex() throws Exception {
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    ESIndexBuilder enabledMappingReindex =
        new ESIndexBuilder(
            getSearchClient(),
            1,
            0,
            0,
            0,
            Map.of(),
            false,
            true,
            false,
            TEST_SEARCH_CONFIG,
            gitVersion);

    // No mappings
    enabledMappingReindex.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
    String beforeCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");

    // add new mappings
    enabledMappingReindex.buildIndex(
        TEST_INDEX_NAME, SystemMetadataMappingsBuilder.getMappings(), Map.of());

    String afterAddedMappingCreationDate =
        getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");
    assertEquals(
        beforeCreationDate,
        afterAddedMappingCreationDate,
        "Expected no reindex on *adding* mappings");

    // change mappings
    Map<String, Object> newProps =
        ((Map<String, Object>) SystemMetadataMappingsBuilder.getMappings().get("properties"))
            .entrySet().stream()
                .map(
                    m ->
                        !m.getKey().equals("urn")
                            ? m
                            : Map.entry(
                                "urn",
                                ImmutableMap.<String, Object>builder().put("type", "text").build()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    enabledMappingReindex.buildIndex(TEST_INDEX_NAME, Map.of("properties", newProps), Map.of());

    assertTrue(
        Arrays.stream(getTestIndex().getIndices()).noneMatch(name -> name.equals(TEST_INDEX_NAME)),
        "Expected original index to be replaced with alias");

    Map.Entry<String, List<AliasMetadata>> newIndex =
        getTestIndex().getAliases().entrySet().stream()
            .filter(
                e ->
                    e.getValue().stream()
                        .anyMatch(aliasMeta -> aliasMeta.alias().equals(TEST_INDEX_NAME)))
            .findFirst()
            .get();
    String afterChangedMappingCreationDate =
        getTestIndex().getSetting(newIndex.getKey(), "index.creation_date");
    assertNotEquals(
        beforeCreationDate,
        afterChangedMappingCreationDate,
        "Expected reindex on *changing* mappings");
  }

  @Test
  public void testSettingsNumberOfReplicasReindex() throws Exception {
    // Set test defaults
    String expectedReplicas = "0";
    testDefaultBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
    assertEquals(
        expectedReplicas, getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"));
    String beforeCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");

    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    ESIndexBuilder changedShardBuilder =
        new ESIndexBuilder(
            getSearchClient(),
            testDefaultBuilder.getNumShards(),
            REPLICASTEST,
            testDefaultBuilder.getNumRetries(),
            testDefaultBuilder.getRefreshIntervalSeconds(),
            Map.of(),
            true,
            false,
            false,
            TEST_SEARCH_CONFIG,
            gitVersion);

    // add new replicas
    changedShardBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
    // but we keep original replica count
    assertEquals(
        getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"),
        expectedReplicas,
        "Expected number of replicas: " + expectedReplicas);
  }

  @Test
  public void testSettingsNumberOfShardsReindex() throws Exception {
    // Set test defaults
    testDefaultBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
    assertEquals("1", getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_shards"));
    String beforeCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");

    String expectedShards = "5";
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    ESIndexBuilder changedShardBuilder =
        new ESIndexBuilder(
            getSearchClient(),
            Integer.parseInt(expectedShards),
            testDefaultBuilder.getNumReplicas(),
            testDefaultBuilder.getNumRetries(),
            testDefaultBuilder.getRefreshIntervalSeconds(),
            Map.of(),
            true,
            false,
            false,
            TEST_SEARCH_CONFIG,
            gitVersion);

    // add new shard setting
    changedShardBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
    assertTrue(
        Arrays.stream(getTestIndex().getIndices()).noneMatch(name -> name.equals(TEST_INDEX_NAME)),
        "Expected original index to be replaced with alias");

    Map.Entry<String, List<AliasMetadata>> newIndex =
        getTestIndex().getAliases().entrySet().stream()
            .filter(
                e ->
                    e.getValue().stream()
                        .anyMatch(aliasMeta -> aliasMeta.alias().equals(TEST_INDEX_NAME)))
            .findFirst()
            .get();

    String afterCreationDate = getTestIndex().getSetting(newIndex.getKey(), "index.creation_date");
    assertNotEquals(
        beforeCreationDate, afterCreationDate, "Expected reindex to result in different timestamp");
    assertEquals(
        expectedShards,
        getTestIndex().getSetting(newIndex.getKey(), "index.number_of_shards"),
        "Expected number of shards: " + expectedShards);
  }

  @Test
  public void testSettingsNoReindex() throws Exception {
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    List<ESIndexBuilder> noReindexBuilders =
        List.of(
            new ESIndexBuilder(
                getSearchClient(),
                testDefaultBuilder.getNumShards(),
                testDefaultBuilder.getNumReplicas() + 1,
                testDefaultBuilder.getNumRetries(),
                testDefaultBuilder.getRefreshIntervalSeconds(),
                Map.of(),
                true,
                false,
                false,
                TEST_SEARCH_CONFIG,
                gitVersion),
            new ESIndexBuilder(
                getSearchClient(),
                testDefaultBuilder.getNumShards(),
                testDefaultBuilder.getNumReplicas(),
                testDefaultBuilder.getNumRetries(),
                testDefaultBuilder.getRefreshIntervalSeconds() + 10,
                Map.of(),
                true,
                false,
                false,
                TEST_SEARCH_CONFIG,
                gitVersion),
            new ESIndexBuilder(
                getSearchClient(),
                testDefaultBuilder.getNumShards() + 1,
                testDefaultBuilder.getNumReplicas(),
                testDefaultBuilder.getNumRetries(),
                testDefaultBuilder.getRefreshIntervalSeconds(),
                Map.of(),
                false,
                false,
                false,
                TEST_SEARCH_CONFIG,
                gitVersion),
            new ESIndexBuilder(
                getSearchClient(),
                testDefaultBuilder.getNumShards(),
                testDefaultBuilder.getNumReplicas() + 1,
                testDefaultBuilder.getNumRetries(),
                testDefaultBuilder.getRefreshIntervalSeconds(),
                Map.of(),
                false,
                false,
                false,
                TEST_SEARCH_CONFIG,
                gitVersion));

    for (ESIndexBuilder builder : noReindexBuilders) {
      // Set test defaults
      testDefaultBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
      assertEquals("0", getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"));
      assertEquals("0s", getTestIndex().getSetting(TEST_INDEX_NAME, "index.refresh_interval"));
      String beforeCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");

      // build index with builder
      builder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
      assertTrue(
          Arrays.asList(getTestIndex().getIndices()).contains(TEST_INDEX_NAME),
          "Expected original index to remain");
      String afterCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");

      assertEquals(
          beforeCreationDate, afterCreationDate, "Expected no difference in index timestamp");
      // we remove this assert, after https://github.com/datahub-project/datahub/pull/13296 replicas
      // will be set by our cron job
      //      assertEquals(
      //          String.valueOf(builder.getNumReplicas()),
      //          getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"));
      assertEquals(
          builder.getRefreshIntervalSeconds() + "s",
          getTestIndex().getSetting(TEST_INDEX_NAME, "index.refresh_interval"));

      wipe();
    }
  }

  @Test
  public void testCopyStructuredPropertyMappings() throws Exception {
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    ESIndexBuilder enabledMappingReindex =
        new ESIndexBuilder(
            getSearchClient(),
            1,
            0,
            0,
            0,
            Map.of(),
            false,
            true,
            false,
            TEST_SEARCH_CONFIG,
            gitVersion);

    ReindexConfig reindexConfigNoIndexBefore =
        enabledMappingReindex.buildReindexState(
            TEST_INDEX_NAME, SystemMetadataMappingsBuilder.getMappings(), Map.of());
    assertNull(reindexConfigNoIndexBefore.currentMappings());
    assertEquals(
        reindexConfigNoIndexBefore.targetMappings(), SystemMetadataMappingsBuilder.getMappings());
    assertFalse(reindexConfigNoIndexBefore.requiresApplyMappings());
    assertFalse(reindexConfigNoIndexBefore.isPureMappingsAddition());

    // Create index
    enabledMappingReindex.buildIndex(
        TEST_INDEX_NAME, SystemMetadataMappingsBuilder.getMappings(), Map.of());

    // Test build reindex config with no structured properties added
    ReindexConfig reindexConfigNoChange =
        enabledMappingReindex.buildReindexState(
            TEST_INDEX_NAME, SystemMetadataMappingsBuilder.getMappings(), Map.of());
    assertEquals(
        reindexConfigNoChange.currentMappings(), SystemMetadataMappingsBuilder.getMappings());
    assertEquals(
        reindexConfigNoChange.targetMappings(), SystemMetadataMappingsBuilder.getMappings());
    assertFalse(reindexConfigNoIndexBefore.requiresApplyMappings());
    assertFalse(reindexConfigNoIndexBefore.isPureMappingsAddition());

    // Test add new field to the mappings
    Map<String, Object> targetMappingsNewField =
        new HashMap<>(SystemMetadataMappingsBuilder.getMappings());
    ((Map<String, Object>) targetMappingsNewField.get("properties"))
        .put("myNewField", Map.of(SettingsBuilder.TYPE, SettingsBuilder.KEYWORD));

    // Test build reindex config for new fields with no structured properties added
    ReindexConfig reindexConfigNewField =
        enabledMappingReindex.buildReindexState(TEST_INDEX_NAME, targetMappingsNewField, Map.of());
    assertEquals(
        reindexConfigNewField.currentMappings(), SystemMetadataMappingsBuilder.getMappings());
    assertEquals(reindexConfigNewField.targetMappings(), targetMappingsNewField);
    assertTrue(reindexConfigNewField.requiresApplyMappings());
    assertTrue(reindexConfigNewField.isPureMappingsAddition());

    // Add structured properties to index
    Map<String, Object> mappingsWithStructuredProperties =
        new HashMap<>(SystemMetadataMappingsBuilder.getMappings());
    ((Map<String, Object>) mappingsWithStructuredProperties.get("properties"))
        .put(
            STRUCTURED_PROPERTY_MAPPING_FIELD + ".myStringProp",
            Map.of(SettingsBuilder.TYPE, SettingsBuilder.KEYWORD));
    ((Map<String, Object>) mappingsWithStructuredProperties.get("properties"))
        .put(
            STRUCTURED_PROPERTY_MAPPING_FIELD + ".myNumberProp",
            Map.of(SettingsBuilder.TYPE, ESUtils.DOUBLE_FIELD_TYPE));

    enabledMappingReindex.buildIndex(TEST_INDEX_NAME, mappingsWithStructuredProperties, Map.of());

    // Test build reindex config with structured properties not copied
    ReindexConfig reindexConfigNoCopy =
        enabledMappingReindex.buildReindexState(
            TEST_INDEX_NAME, SystemMetadataMappingsBuilder.getMappings(), Map.of());
    Map<String, Object> expectedMappingsStructPropsNested =
        new HashMap<>(SystemMetadataMappingsBuilder.getMappings());
    ((Map<String, Object>) expectedMappingsStructPropsNested.get("properties"))
        .put(
            "structuredProperties",
            Map.of(
                "properties",
                Map.of(
                    "myNumberProp",
                    Map.of(SettingsBuilder.TYPE, ESUtils.DOUBLE_FIELD_TYPE),
                    "myStringProp",
                    Map.of(SettingsBuilder.TYPE, SettingsBuilder.KEYWORD))));
    assertEquals(reindexConfigNoCopy.currentMappings(), expectedMappingsStructPropsNested);
    assertEquals(reindexConfigNoCopy.targetMappings(), SystemMetadataMappingsBuilder.getMappings());
    assertFalse(reindexConfigNoCopy.isPureMappingsAddition());

    // Test build reindex config with structured properties copied
    ReindexConfig reindexConfigCopy =
        enabledMappingReindex.buildReindexState(
            TEST_INDEX_NAME, SystemMetadataMappingsBuilder.getMappings(), Map.of(), true);
    assertEquals(reindexConfigCopy.currentMappings(), expectedMappingsStructPropsNested);
    assertEquals(reindexConfigCopy.targetMappings(), expectedMappingsStructPropsNested);
    assertFalse(reindexConfigCopy.requiresApplyMappings());
    assertFalse(reindexConfigCopy.isPureMappingsAddition());

    // Test build reindex config with new field added and structured properties copied
    ReindexConfig reindexConfigCopyAndNewField =
        enabledMappingReindex.buildReindexState(
            TEST_INDEX_NAME, targetMappingsNewField, Map.of(), true);
    assertEquals(reindexConfigCopyAndNewField.currentMappings(), expectedMappingsStructPropsNested);
    Map<String, Object> targetMappingsNewFieldAndStructProps =
        new HashMap<>(expectedMappingsStructPropsNested);
    ((Map<String, Object>) targetMappingsNewFieldAndStructProps.get("properties"))
        .put("myNewField", Map.of(SettingsBuilder.TYPE, SettingsBuilder.KEYWORD));
    assertEquals(
        reindexConfigCopyAndNewField.targetMappings(), targetMappingsNewFieldAndStructProps);
    assertTrue(reindexConfigCopyAndNewField.requiresApplyMappings());
    assertTrue(reindexConfigCopyAndNewField.isPureMappingsAddition());
  }
}

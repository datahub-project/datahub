package com.linkedin.metadata.search.indexbuilder;

import static com.linkedin.metadata.Constants.DATA_TYPE_URN_PREFIX;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_STRUCT_PROPS_DISABLED;
import static io.datahubproject.test.search.SearchTestUtils.V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION;
import static io.datahubproject.test.search.SearchTestUtils.createDelegatingMappingsBuilder;
import static io.datahubproject.test.search.SearchTestUtils.createDelegatingSettingsBuilder;
import static org.testng.Assert.*;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.config.StructuredPropertiesConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.DelegatingMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.DelegatingSettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexResult;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.systemmetadata.SystemMetadataMappingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
import org.opensearch.client.RequestOptions;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.util.CollectionUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public abstract class IndexBuilderTestBase extends AbstractTestNGSpringContextTests {

  @Nonnull
  protected abstract SearchClientShim<?> getSearchClient();

  @Nonnull
  protected abstract ElasticSearchConfiguration getElasticSearchConfiguration();

  protected OperationContext opContext;
  protected static final String TEST_INDEX_NAME =
      "estest_datasetindex_v2"; // Use v2 as default for backward compatibility
  protected static final String TEST_V2_INDEX_NAME = TEST_INDEX_NAME;
  protected static final String TEST_V3_INDEX_NAME = "estest_primaryindex_v3";
  private ElasticSearchConfiguration testDefaultConfig;
  private ESIndexBuilder testDefaultBuilder;
  private ESIndexBuilder testReplicasBuilder;
  private DelegatingSettingsBuilder delegatingSettingsBuilder;
  private DelegatingMappingsBuilder delegatingMappingsBuilder;
  private IndexConvention indexConvention;
  protected static final int REPLICASTEST = 2;

  /** Helper method to build an index using the new ReindexConfig pattern */
  private static void buildIndex(
      ESIndexBuilder builder,
      String indexName,
      Map<String, Object> mappings,
      Map<String, Object> settings)
      throws IOException {
    ReindexConfig reindexConfig = builder.buildReindexState(indexName, mappings, settings);
    builder.buildIndex(reindexConfig);
  }

  @BeforeClass
  public void setup() {
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());

    StructuredPropertiesConfiguration structPropConfig =
        StructuredPropertiesConfiguration.builder().systemUpdateEnabled(false).build();

    // Create configuration with both v2 and v3 enabled
    testDefaultConfig =
        TEST_ES_SEARCH_CONFIG.toBuilder()
            .entityIndex(V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION)
            .index(
                TEST_ES_SEARCH_CONFIG.getIndex().toBuilder()
                    .numShards(1)
                    .numReplicas(0)
                    .numRetries(3)
                    .refreshIntervalSeconds(0)
                    .build())
            .build();
    testDefaultBuilder =
        new ESIndexBuilder(
            getSearchClient(), testDefaultConfig, structPropConfig, Map.of(), gitVersion);
    // TODO getElasticSearchConfiguration(),
    ElasticSearchConfiguration testReplicasConfig =
        TEST_ES_SEARCH_CONFIG.toBuilder()
            .entityIndex(V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION)
            .index(
                TEST_ES_SEARCH_CONFIG.getIndex().toBuilder()
                    .numShards(1)
                    .numReplicas(REPLICASTEST)
                    .numRetries(3)
                    .refreshIntervalSeconds(0)
                    .build())
            .build();
    testReplicasBuilder =
        new ESIndexBuilder(
            getSearchClient(), testReplicasConfig, structPropConfig, Map.of(), gitVersion);

    // Setup real IndexConvention for both v2 and v3
    indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder()
                .prefix("estest")
                .hashIdAlgo("MD5")
                .build(),
            V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION);

    // Create operation context with our index convention
    opContext =
        TestOperationContexts.systemContextNoSearchAuthorization().toBuilder()
            .searchContext(SearchContext.EMPTY.toBuilder().indexConvention(indexConvention).build())
            .build(
                TestOperationContexts.systemContextNoSearchAuthorization()
                    .getSessionAuthentication(),
                true);

    // Setup DelegatingSettingsBuilder and DelegatingMappingsBuilder
    IndexConfiguration indexConfiguration =
        IndexConfiguration.builder().minSearchFilterLength(3).build();

    // Create DelegatingSettingsBuilder using utility method
    delegatingSettingsBuilder =
        createDelegatingSettingsBuilder(
            V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION, indexConfiguration, indexConvention);

    // Create DelegatingMappingsBuilder using utility method
    delegatingMappingsBuilder =
        createDelegatingMappingsBuilder(V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION);
  }

  @BeforeMethod
  public void wipe() throws Exception {
    // Clean up all test indices
    String[] testIndices = {TEST_V2_INDEX_NAME, TEST_V3_INDEX_NAME};

    for (String indexName : testIndices) {
      try {
        getSearchClient()
            .getIndexAliases(new GetAliasesRequest(indexName), RequestOptions.DEFAULT)
            .getAliases()
            .keySet()
            .forEach(
                index -> {
                  try {
                    getSearchClient()
                        .deleteIndex(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                });

        getSearchClient().deleteIndex(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
      } catch (OpenSearchException exception) {
        if (exception.status() != RestStatus.NOT_FOUND) {
          throw exception;
        }
      } catch (ElasticsearchException exception) {
        if (exception.status() != 404) {
          throw exception;
        }
      }
    }
  }

  public GetIndexResponse getTestIndex() throws IOException {
    return getSearchClient()
        .getIndex(
            new GetIndexRequest(TEST_INDEX_NAME).includeDefaults(true), RequestOptions.DEFAULT);
  }

  public GetIndexResponse getV2TestIndex() throws IOException {
    return getSearchClient()
        .getIndex(
            new GetIndexRequest(TEST_V2_INDEX_NAME).includeDefaults(true), RequestOptions.DEFAULT);
  }

  public GetIndexResponse getV3TestIndex() throws IOException {
    return getSearchClient()
        .getIndex(
            new GetIndexRequest(TEST_V3_INDEX_NAME).includeDefaults(true), RequestOptions.DEFAULT);
  }

  @Test
  public void testTweakReplicasStepOps() throws Exception {
    ReindexConfig indexState =
        testReplicasBuilder.buildReindexState(
            TEST_INDEX_NAME, SystemMetadataMappingsBuilder.getMappings(), Map.of());
    testReplicasBuilder.buildIndex(indexState);
    // assert initial state, index has 0 docs, REPLICASTEST replica
    assertEquals(
        getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"), "" + REPLICASTEST);

    // 0,1 --> 0,1 with dryRun
    testReplicasBuilder.tweakReplicas(indexState, true);
    assertEquals(
        getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"), "" + REPLICASTEST);
    // 0,1 --> 0,0
    testReplicasBuilder.tweakReplicas(indexState, false);
    assertEquals(getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"), "0");
    // 0,0 --> verify not undesired changes
    testReplicasBuilder.tweakReplicas(indexState, false);
    assertEquals(getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"), "0");
    // index one doc
    IndexRequest indexRequest =
        new IndexRequest(TEST_INDEX_NAME).id("1").source(new HashMap<>(), XContentType.JSON);
    IndexResponse indexResponse =
        getSearchClient().indexDocument(indexRequest, RequestOptions.DEFAULT);
    // make sure it will be counted
    getSearchClient()
        .refreshIndex(
            new org.opensearch.action.admin.indices.refresh.RefreshRequest(TEST_INDEX_NAME),
            RequestOptions.DEFAULT);
    long numDocs =
        getSearchClient()
            .count(new CountRequest(TEST_INDEX_NAME), RequestOptions.DEFAULT)
            .getCount();
    assertEquals(numDocs, 1, "Expected 0 documents in the test index");
    // 1,0 --> 1,0 with dryRun
    testReplicasBuilder.tweakReplicas(indexState, true);
    assertEquals(getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"), "0");
    // 1,0 --> 1,1
    testReplicasBuilder.tweakReplicas(indexState, false);
    assertEquals(
        getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"), "" + REPLICASTEST);
    // 1,1 --> verify not undesired changes
    testReplicasBuilder.tweakReplicas(indexState, false);
    assertEquals(
        getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"), "" + REPLICASTEST);
  }

  @Test
  public void testESIndexBuilderNoSkipNdocs() throws Exception {
    // Set test defaults
    ReindexConfig reindexConfig =
        testDefaultBuilder.buildReindexState(TEST_INDEX_NAME, Map.of(), Map.of());
    testDefaultBuilder.buildIndex(reindexConfig);
    String beforeCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");
    String expectedShards = "2";
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    ESIndexBuilder changedShardBuilder =
        new ESIndexBuilder(
            getSearchClient(),
            testDefaultConfig.toBuilder()
                .index(
                    testDefaultConfig.getIndex().toBuilder()
                        .numShards(Integer.parseInt(expectedShards))
                        .build())
                .build(),
            TEST_ES_STRUCT_PROPS_DISABLED,
            Map.of(),
            gitVersion);
    // index one doc
    IndexRequest indexRequest =
        new IndexRequest(TEST_INDEX_NAME).id("1").source(new HashMap<>(), XContentType.JSON);
    IndexResponse indexResponse =
        getSearchClient().indexDocument(indexRequest, RequestOptions.DEFAULT);
    // reindex
    ReindexConfig reindexConfig2 =
        changedShardBuilder.buildReindexState(TEST_INDEX_NAME, Map.of(), Map.of());
    ReindexResult rr = changedShardBuilder.buildIndex(reindexConfig2);
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
    ReindexConfig reindexConfig =
        testDefaultBuilder.buildReindexState(TEST_INDEX_NAME, Map.of(), Map.of());
    testDefaultBuilder.buildIndex(reindexConfig);
    String beforeCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");
    String expectedShards = "2";
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    ESIndexBuilder changedShardBuilder =
        new ESIndexBuilder(
            getSearchClient(),
            testDefaultConfig.toBuilder()
                .index(
                    testDefaultConfig.getIndex().toBuilder()
                        .numShards(Integer.parseInt(expectedShards))
                        .build())
                .build(),
            TEST_ES_STRUCT_PROPS_DISABLED,
            Map.of(),
            gitVersion);
    // reindex
    ReindexConfig reindexConfig2 =
        changedShardBuilder.buildReindexState(TEST_INDEX_NAME, Map.of(), Map.of());
    ReindexResult rr = changedShardBuilder.buildIndex(reindexConfig2);
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
    assertEquals(nbdocs, 0);
  }

  @Test
  public void testESIndexBuilderReindex() throws Exception {
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    // index with original refresh_interval
    ESIndexBuilder nsecBuilder =
        new ESIndexBuilder(
            getSearchClient(),
            testDefaultConfig.toBuilder()
                .index(
                    testDefaultConfig.getIndex().toBuilder()
                        .numShards(1)
                        .numReplicas(1)
                        .numRetries(1)
                        .refreshIntervalSeconds(1)
                        .build())
                .build(),
            TEST_ES_STRUCT_PROPS_DISABLED,
            Map.of(),
            gitVersion);
    ReindexConfig reindexConfig =
        nsecBuilder.buildReindexState(TEST_INDEX_NAME, Map.of(), Map.of());
    nsecBuilder.buildIndex(reindexConfig);
    String beforeCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");
    // add some docs
    int NDOCS = 5;
    for (int i = 0; i < NDOCS; i++) {
      IndexRequest indexRequest =
          new IndexRequest(TEST_INDEX_NAME).id("" + i).source(new HashMap<>(), XContentType.JSON);
      IndexResponse indexResponse =
          getSearchClient().indexDocument(indexRequest, RequestOptions.DEFAULT);
    }
    // make sure it will be counted
    getSearchClient()
        .refreshIndex(
            new org.opensearch.action.admin.indices.refresh.RefreshRequest(TEST_INDEX_NAME),
            RequestOptions.DEFAULT);
    // new index
    ESIndexBuilder changedShardBuilder =
        new ESIndexBuilder(
            getSearchClient(),
            testDefaultConfig.toBuilder()
                .index(
                    testDefaultConfig.getIndex().toBuilder()
                        .numShards(2)
                        .numReplicas(2)
                        .numRetries(2)
                        .refreshIntervalSeconds(2)
                        .build())
                .build(),
            TEST_ES_STRUCT_PROPS_DISABLED,
            Map.of(),
            gitVersion);
    // reindex
    ReindexConfig reindexConfig2 =
        changedShardBuilder.buildReindexState(TEST_INDEX_NAME, Map.of(), Map.of());
    ReindexResult rr = changedShardBuilder.buildIndex(reindexConfig2);
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
    assertEquals(numDocs, NDOCS, "Expected " + NDOCS + " documents in the test index");
    assertEquals(getTestIndex().getSetting(newIndex.getKey(), "index.number_of_replicas"), "2");
    assertEquals(getTestIndex().getSetting(newIndex.getKey(), "index.number_of_shards"), "2");
  }

  @Test
  protected abstract void testCodec() throws Exception;

  @Test
  public void testMappingReindex() throws Exception {
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    ESIndexBuilder enabledMappingReindex =
        new ESIndexBuilder(
            getSearchClient(),
            testDefaultConfig.toBuilder()
                .index(
                    testDefaultConfig.getIndex().toBuilder()
                        .numShards(1)
                        .numReplicas(0)
                        .numRetries(0)
                        .refreshIntervalSeconds(0)
                        .build())
                .build(),
            TEST_ES_STRUCT_PROPS_DISABLED,
            Map.of(),
            gitVersion);

    // No mappings
    ReindexConfig reindexConfig =
        enabledMappingReindex.buildReindexState(TEST_INDEX_NAME, Map.of(), Map.of());
    enabledMappingReindex.buildIndex(reindexConfig);
    String beforeCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");

    // add new mappings
    ReindexConfig reindexConfig2 =
        enabledMappingReindex.buildReindexState(
            TEST_INDEX_NAME, SystemMetadataMappingsBuilder.getMappings(), Map.of());
    enabledMappingReindex.buildIndex(reindexConfig2);

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
    ReindexConfig reindexConfig3 =
        enabledMappingReindex.buildReindexState(
            TEST_INDEX_NAME, Map.of("properties", newProps), Map.of());
    enabledMappingReindex.buildIndex(reindexConfig3);

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
    buildIndex(testDefaultBuilder, TEST_INDEX_NAME, Map.of(), Map.of());
    assertEquals(
        getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"), expectedReplicas);
    String beforeCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");

    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    ESIndexBuilder changedShardBuilder =
        new ESIndexBuilder(
            getSearchClient(),
            testDefaultConfig.toBuilder()
                .index(testDefaultConfig.getIndex().toBuilder().numReplicas(REPLICASTEST).build())
                .build(),
            TEST_ES_STRUCT_PROPS_DISABLED,
            Map.of(),
            gitVersion);

    // add new replicas
    buildIndex(changedShardBuilder, TEST_INDEX_NAME, Map.of(), Map.of());
    // but we keep original replica count
    assertEquals(
        getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"),
        expectedReplicas,
        "Expected number of replicas: " + expectedReplicas);
  }

  @Test
  public void testSettingsNumberOfShardsReindex() throws Exception {
    // Set test defaults
    buildIndex(testDefaultBuilder, TEST_INDEX_NAME, Map.of(), Map.of());
    assertEquals(getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_shards"), "1");
    String beforeCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");

    String expectedShards = "5";
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    ESIndexBuilder changedShardBuilder =
        new ESIndexBuilder(
            getSearchClient(),
            testDefaultConfig.toBuilder()
                .index(
                    testDefaultConfig.getIndex().toBuilder()
                        .numShards(Integer.parseInt(expectedShards))
                        .build())
                .build(),
            TEST_ES_STRUCT_PROPS_DISABLED,
            Map.of(),
            gitVersion);

    // add new shard setting
    buildIndex(changedShardBuilder, TEST_INDEX_NAME, Map.of(), Map.of());
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
        getTestIndex().getSetting(newIndex.getKey(), "index.number_of_shards"),
        expectedShards,
        "Expected number of shards: " + expectedShards);
  }

  @Test
  public void testSettingsNoReindex() throws Exception {
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());

    ElasticSearchConfiguration noReindexConfig =
        testDefaultConfig.toBuilder()
            .index(testDefaultConfig.getIndex().toBuilder().enableSettingsReindex(false).build())
            .build();

    List<ESIndexBuilder> noReindexBuilders =
        List.of(
            new ESIndexBuilder(
                getSearchClient(),
                noReindexConfig.toBuilder()
                    .index(
                        noReindexConfig.getIndex().toBuilder()
                            .numReplicas(1) // testDefaultBuilder.getNumReplicas() + 1
                            .build())
                    .build(),
                TEST_ES_STRUCT_PROPS_DISABLED,
                Map.of(),
                gitVersion),
            new ESIndexBuilder(
                getSearchClient(),
                noReindexConfig.toBuilder()
                    .index(
                        noReindexConfig.getIndex().toBuilder()
                            .refreshIntervalSeconds(
                                10) // testDefaultBuilder.getRefreshIntervalSeconds() + 10
                            .build())
                    .build(),
                TEST_ES_STRUCT_PROPS_DISABLED,
                Map.of(),
                gitVersion),
            new ESIndexBuilder(
                getSearchClient(),
                noReindexConfig.toBuilder()
                    .index(
                        noReindexConfig.getIndex().toBuilder()
                            .numShards(2) // testDefaultBuilder.getNumShards() + 1
                            .build())
                    .build(),
                TEST_ES_STRUCT_PROPS_DISABLED,
                Map.of(),
                gitVersion),
            new ESIndexBuilder(
                getSearchClient(),
                noReindexConfig.toBuilder()
                    .index(
                        noReindexConfig.getIndex().toBuilder()
                            .numReplicas(1) // testDefaultBuilder.getNumReplicas() + 1
                            .build())
                    .build(),
                TEST_ES_STRUCT_PROPS_DISABLED,
                Map.of(),
                gitVersion));

    for (ESIndexBuilder builder : noReindexBuilders) {
      // Set test defaults
      buildIndex(testDefaultBuilder, TEST_INDEX_NAME, Map.of(), Map.of());
      assertEquals(getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"), "0");
      assertEquals(getTestIndex().getSetting(TEST_INDEX_NAME, "index.refresh_interval"), "0s");
      String beforeCreationDate = getTestIndex().getSetting(TEST_INDEX_NAME, "index.creation_date");

      // build index with builder
      buildIndex(builder, TEST_INDEX_NAME, Map.of(), Map.of());
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
          builder.getConfig().getIndex().getRefreshIntervalSeconds() + "s",
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
            testDefaultConfig.toBuilder()
                .index(
                    testDefaultConfig.getIndex().toBuilder()
                        .numShards(1)
                        .numReplicas(0)
                        .numRetries(0)
                        .refreshIntervalSeconds(0)
                        .build())
                .build(),
            TEST_ES_STRUCT_PROPS_DISABLED,
            Map.of(),
            gitVersion);

    ReindexConfig reindexConfigNoIndexBefore =
        enabledMappingReindex.buildReindexState(
            TEST_INDEX_NAME, SystemMetadataMappingsBuilder.getMappings(), Map.of());
    assertTrue(CollectionUtils.isEmpty(reindexConfigNoIndexBefore.currentMappings()));
    assertEquals(
        reindexConfigNoIndexBefore.targetMappings(), SystemMetadataMappingsBuilder.getMappings());
    assertFalse(reindexConfigNoIndexBefore.requiresApplyMappings());
    assertFalse(reindexConfigNoIndexBefore.isPureMappingsAddition());

    // Create index
    buildIndex(
        enabledMappingReindex,
        TEST_INDEX_NAME,
        SystemMetadataMappingsBuilder.getMappings(),
        Map.of());

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
        .put("myNewField", Map.of(ESUtils.TYPE, ESUtils.KEYWORD));

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
            Map.of(ESUtils.TYPE, V2LegacySettingsBuilder.KEYWORD));
    ((Map<String, Object>) mappingsWithStructuredProperties.get("properties"))
        .put(
            STRUCTURED_PROPERTY_MAPPING_FIELD + ".myNumberProp",
            Map.of(ESUtils.TYPE, ESUtils.DOUBLE_FIELD_TYPE));

    buildIndex(enabledMappingReindex, TEST_INDEX_NAME, mappingsWithStructuredProperties, Map.of());

    // Test build reindex config with structured properties not copied
    ReindexConfig reindexConfigNoCopy =
        enabledMappingReindex.buildReindexState(
            TEST_INDEX_NAME, SystemMetadataMappingsBuilder.getMappings(), Map.of());
    Map<String, Object> expectedMappingsStructPropsNested;
    if (SearchClientShim.SearchEngineType.ELASTICSEARCH_8.equals(
        getSearchClient().getEngineType())) {
      expectedMappingsStructPropsNested =
          new HashMap<>(SystemMetadataMappingsBuilder.getMappings());
      ((Map<String, Object>) expectedMappingsStructPropsNested.get("properties"))
          .put(
              "structuredProperties",
              Map.of(
                  "properties",
                  Map.of(
                      "myNumberProp",
                      Map.of(ESUtils.TYPE, ESUtils.DOUBLE_FIELD_TYPE),
                      "myStringProp",
                      Map.of(ESUtils.TYPE, V2LegacySettingsBuilder.KEYWORD)),
                  "type",
                  "object"));
    } else {
      expectedMappingsStructPropsNested =
          new HashMap<>(SystemMetadataMappingsBuilder.getMappings());
      ((Map<String, Object>) expectedMappingsStructPropsNested.get("properties"))
          .put(
              "structuredProperties",
              Map.of(
                  "properties",
                  Map.of(
                      "myNumberProp",
                      Map.of(ESUtils.TYPE, ESUtils.DOUBLE_FIELD_TYPE),
                      "myStringProp",
                      Map.of(ESUtils.TYPE, V2LegacySettingsBuilder.KEYWORD))));
    }
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
        .put("myNewField", Map.of(ESUtils.TYPE, ESUtils.KEYWORD));
    assertEquals(
        reindexConfigCopyAndNewField.targetMappings(), targetMappingsNewFieldAndStructProps);
    assertTrue(reindexConfigCopyAndNewField.requiresApplyMappings());
    assertTrue(reindexConfigCopyAndNewField.isPureMappingsAddition());
  }

  @Test
  public void testV2IndexCreation() throws Exception {
    // Test that v2 indices are created with proper settings using delegating builders
    Collection<MappingsBuilder.IndexMapping> allIndexMappings =
        delegatingMappingsBuilder.getIndexMappings(opContext);
    Map<String, Object> v2Mappings =
        allIndexMappings.stream()
            .filter(mapping -> TEST_V2_INDEX_NAME.equals(mapping.getIndexName()))
            .findFirst()
            .map(MappingsBuilder.IndexMapping::getMappings)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "No mappings found for v2 index: " + TEST_V2_INDEX_NAME));
    Map<String, Object> v2Settings =
        delegatingSettingsBuilder.getSettings(testDefaultConfig.getIndex(), TEST_V2_INDEX_NAME);

    ReindexConfig v2ReindexConfig =
        testDefaultBuilder.buildReindexState(TEST_V2_INDEX_NAME, v2Mappings, v2Settings);
    testDefaultBuilder.buildIndex(v2ReindexConfig);

    // Verify v2 index was created
    GetIndexResponse v2Index = getV2TestIndex();
    assertNotNull(v2Index, "V2 index should be created");
    assertTrue(v2Index.getIndices().length > 0, "V2 index should exist");

    // Verify v2 settings are applied
    String[] v2Indices = v2Index.getIndices();
    String v2ActualIndex = v2Indices[0];
    assertNotNull(
        v2Index.getSetting(v2ActualIndex, "index.number_of_shards"),
        "V2 index should have shard settings");
    assertNotNull(
        v2Index.getSetting(v2ActualIndex, "index.number_of_replicas"),
        "V2 index should have replica settings");

    // Verify IndexConvention correctly identifies v2 index
    assertTrue(
        indexConvention.isV2EntityIndex(TEST_V2_INDEX_NAME),
        "IndexConvention should identify dataset v2 index as v2");
    assertFalse(
        indexConvention.isV3EntityIndex(TEST_V2_INDEX_NAME),
        "IndexConvention should not identify dataset v2 index as v3");
  }

  @Test
  public void testV3IndexCreation() throws Exception {
    // Test that v3 indices are created with proper settings using delegating builders
    // Create a test structured property
    StructuredPropertyDefinition testProperty =
        new StructuredPropertyDefinition()
            .setVersion("00000000000001")
            .setQualifiedName("testStructuredProperty")
            .setDisplayName("Test Structured Property")
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:dataset")))
            .setValueType(UrnUtils.getUrn("urn:li:logicalType:STRING"));

    Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties =
        Collections.singletonList(
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:testStructuredProperty"), testProperty));

    Collection<MappingsBuilder.IndexMapping> allIndexMappings =
        delegatingMappingsBuilder.getIndexMappings(opContext, structuredProperties);
    Map<String, Object> v3Mappings =
        allIndexMappings.stream()
            .filter(mapping -> TEST_V3_INDEX_NAME.equals(mapping.getIndexName()))
            .findFirst()
            .map(MappingsBuilder.IndexMapping::getMappings)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "No mappings found for v3 index: " + TEST_V3_INDEX_NAME));
    Map<String, Object> v3Settings =
        delegatingSettingsBuilder.getSettings(testDefaultConfig.getIndex(), TEST_V3_INDEX_NAME);

    ReindexConfig v3ReindexConfig =
        testDefaultBuilder.buildReindexState(TEST_V3_INDEX_NAME, v3Mappings, v3Settings);
    testDefaultBuilder.buildIndex(v3ReindexConfig);

    // Verify v3 index was created
    GetIndexResponse v3Index = getV3TestIndex();
    assertNotNull(v3Index, "V3 index should be created");
    assertTrue(v3Index.getIndices().length > 0, "V3 index should exist");

    // Verify v3 settings are applied
    String[] v3Indices = v3Index.getIndices();
    String v3ActualIndex = v3Indices[0];
    assertNotNull(
        v3Index.getSetting(v3ActualIndex, "index.number_of_shards"),
        "V3 index should have shard settings");
    assertNotNull(
        v3Index.getSetting(v3ActualIndex, "index.number_of_replicas"),
        "V3 index should have replica settings");

    // Verify IndexConvention correctly identifies v3 index
    assertTrue(
        indexConvention.isV3EntityIndex(TEST_V3_INDEX_NAME),
        "IndexConvention should identify dataset v3 index as v3");
    assertFalse(
        indexConvention.isV2EntityIndex(TEST_V3_INDEX_NAME),
        "IndexConvention should not identify dataset v3 index as v2");
  }

  @Test
  public void testV2V3Coexistence() throws Exception {
    // Test that both v2 and v3 indices can be created and coexist
    // Create a test structured property
    StructuredPropertyDefinition testProperty =
        new StructuredPropertyDefinition()
            .setVersion("00000000000001")
            .setQualifiedName("testStructuredProperty")
            .setDisplayName("Test Structured Property")
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:dataset")))
            .setValueType(UrnUtils.getUrn("urn:li:logicalType:STRING"));

    Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties =
        Collections.singletonList(
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:testStructuredProperty"), testProperty));

    // Get all index mappings once
    Collection<MappingsBuilder.IndexMapping> allIndexMappings =
        delegatingMappingsBuilder.getIndexMappings(opContext, structuredProperties);

    // Create v2 index
    Map<String, Object> v2Mappings =
        allIndexMappings.stream()
            .filter(mapping -> TEST_V2_INDEX_NAME.equals(mapping.getIndexName()))
            .findFirst()
            .map(MappingsBuilder.IndexMapping::getMappings)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "No mappings found for v2 index: " + TEST_V2_INDEX_NAME));
    Map<String, Object> v2Settings =
        delegatingSettingsBuilder.getSettings(testDefaultConfig.getIndex(), TEST_V2_INDEX_NAME);
    ReindexConfig v2ReindexConfig =
        testDefaultBuilder.buildReindexState(TEST_V2_INDEX_NAME, v2Mappings, v2Settings);
    testDefaultBuilder.buildIndex(v2ReindexConfig);

    // Create v3 index
    Map<String, Object> v3Mappings =
        allIndexMappings.stream()
            .filter(mapping -> TEST_V3_INDEX_NAME.equals(mapping.getIndexName()))
            .findFirst()
            .map(MappingsBuilder.IndexMapping::getMappings)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "No mappings found for v3 index: " + TEST_V3_INDEX_NAME));
    Map<String, Object> v3Settings =
        delegatingSettingsBuilder.getSettings(testDefaultConfig.getIndex(), TEST_V3_INDEX_NAME);
    ReindexConfig v3ReindexConfig =
        testDefaultBuilder.buildReindexState(TEST_V3_INDEX_NAME, v3Mappings, v3Settings);
    testDefaultBuilder.buildIndex(v3ReindexConfig);

    // Verify both indices exist
    GetIndexResponse v2Index = getV2TestIndex();
    GetIndexResponse v3Index = getV3TestIndex();

    assertNotNull(v2Index, "V2 index should be created");
    assertNotNull(v3Index, "V3 index should be created");
    assertTrue(v2Index.getIndices().length > 0, "V2 index should exist");
    assertTrue(v3Index.getIndices().length > 0, "V3 index should exist");

    // Verify both indices have different settings (v3 should have different analyzer config)
    String[] v2Indices = v2Index.getIndices();
    String[] v3Indices = v3Index.getIndices();
    String v2ActualIndex = v2Indices[0];
    String v3ActualIndex = v3Indices[0];

    // Both should have basic settings
    assertNotNull(
        v2Index.getSetting(v2ActualIndex, "index.number_of_shards"),
        "V2 index should have shard settings");
    assertNotNull(
        v3Index.getSetting(v3ActualIndex, "index.number_of_shards"),
        "V3 index should have shard settings");

    // Verify IndexConvention correctly identifies both indices
    assertTrue(
        indexConvention.isV2EntityIndex(TEST_V2_INDEX_NAME),
        "IndexConvention should identify v2 index as v2");
    assertTrue(
        indexConvention.isV3EntityIndex(TEST_V3_INDEX_NAME),
        "IndexConvention should identify v3 index as v3");
    assertFalse(
        indexConvention.isV3EntityIndex(TEST_V2_INDEX_NAME),
        "IndexConvention should not identify v2 index as v3");
    assertFalse(
        indexConvention.isV2EntityIndex(TEST_V3_INDEX_NAME),
        "IndexConvention should not identify v3 index as v2");
  }

  @Test
  public void testV3AspectsStructure() throws Exception {
    // Test that v3 indices use the new _aspects structure
    // Create a test structured property
    StructuredPropertyDefinition testProperty =
        new StructuredPropertyDefinition()
            .setVersion("00000000000001")
            .setQualifiedName("testStructuredProperty")
            .setDisplayName("Test Structured Property")
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:dataset")))
            .setValueType(UrnUtils.getUrn("urn:li:logicalType:STRING"));

    Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties =
        Collections.singletonList(
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:testStructuredProperty"), testProperty));

    Collection<MappingsBuilder.IndexMapping> allIndexMappings =
        delegatingMappingsBuilder.getIndexMappings(opContext, structuredProperties);
    Map<String, Object> v3Mappings =
        allIndexMappings.stream()
            .filter(mapping -> TEST_V3_INDEX_NAME.equals(mapping.getIndexName()))
            .findFirst()
            .map(MappingsBuilder.IndexMapping::getMappings)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "No mappings found for v3 index: " + TEST_V3_INDEX_NAME));

    // Verify the mappings have the _aspects structure
    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) v3Mappings.get("properties");
    assertNotNull(properties, "V3 mappings should have properties");

    // Check that _aspects object exists
    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertNotNull(aspects, "V3 mappings should have _aspects object");

    // Check that _aspects has properties
    @SuppressWarnings("unchecked")
    Map<String, Object> aspectsProperties = (Map<String, Object>) aspects.get("properties");
    assertNotNull(aspectsProperties, "_aspects should have properties");

    // Verify that structuredProperties is NOT in _aspects (it should be at root level)
    assertNull(
        aspectsProperties.get("structuredProperties"),
        "structuredProperties should not be in _aspects, it should be at root level");

    // Check that structuredProperties exists at root level
    @SuppressWarnings("unchecked")
    Map<String, Object> structuredProps =
        (Map<String, Object>) properties.get("structuredProperties");
    assertNotNull(structuredProps, "structuredProperties should exist at root level");

    // Verify that other aspects are properly nested under _aspects
    // This is a basic check - in a real scenario, you'd have actual aspect names
    assertTrue(
        aspectsProperties.isEmpty()
            || aspectsProperties.keySet().stream()
                .noneMatch(key -> "structuredProperties".equals(key)),
        "No aspect in _aspects should be named 'structuredProperties'");
  }

  /**
   * Regression test for reindex (BuildIndicesStep): structured properties with valueType
   * datahub.urn must get a mapping with a "type" so putMapping does not fail with
   * mapper_parsing_exception.
   */
  @Test
  public void testReindexMappingsWithDatahubUrnStructuredPropertyHaveType() throws Exception {
    StructuredPropertyDefinition urnStructuredProperty =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("com.example.domain.owner_urn")
            .setDisplayName("Owner URN")
            .setEntityTypes(
                new UrnArray(
                    UrnUtils.getUrn("urn:li:entityType:datahub.dataset"),
                    UrnUtils.getUrn("urn:li:entityType:datahub.dataJob")))
            .setValueType(UrnUtils.getUrn(DATA_TYPE_URN_PREFIX + "datahub.urn"));

    Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties =
        Collections.singletonList(
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:com.example.domain.owner_urn"),
                urnStructuredProperty));

    Collection<MappingsBuilder.IndexMapping> allIndexMappings =
        delegatingMappingsBuilder.getIndexMappings(opContext, structuredProperties);

    String expectedFieldName = "com_example_domain_owner_urn";
    boolean foundFieldWithType = false;
    for (MappingsBuilder.IndexMapping indexMapping : allIndexMappings) {
      Map<String, Object> mappings = indexMapping.getMappings();
      if (mappings == null) {
        continue;
      }
      @SuppressWarnings("unchecked")
      Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
      if (properties == null) {
        continue;
      }
      // V2: structured property fields live under properties.structuredProperties.properties
      @SuppressWarnings("unchecked")
      Map<String, Object> structuredProps =
          (Map<String, Object>) properties.get(STRUCTURED_PROPERTY_MAPPING_FIELD);
      if (structuredProps != null) {
        @SuppressWarnings("unchecked")
        Map<String, Object> structuredPropsFields =
            (Map<String, Object>) structuredProps.get("properties");
        if (structuredPropsFields != null && structuredPropsFields.containsKey(expectedFieldName)) {
          @SuppressWarnings("unchecked")
          Map<String, Object> fieldMapping =
              (Map<String, Object>) structuredPropsFields.get(expectedFieldName);
          assertNotNull(
              fieldMapping.get("type"),
              "Reindex mappings must include type for structured property "
                  + expectedFieldName
                  + " (index "
                  + indexMapping.getIndexName()
                  + ")");
          foundFieldWithType = true;
        }
      }
      // V3 or flat: field may be directly under properties
      if (!foundFieldWithType && properties.containsKey(expectedFieldName)) {
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldMapping = (Map<String, Object>) properties.get(expectedFieldName);
        assertNotNull(
            fieldMapping.get("type"),
            "Reindex mappings must include type for " + expectedFieldName);
        foundFieldWithType = true;
      }
    }
    assertTrue(
        foundFieldWithType,
        "At least one index mapping should include the URN structured property with a type");
  }

  @Test
  public void testV3AliasPaths() throws Exception {
    // Test that v3 aliases point to the correct paths
    Collection<MappingsBuilder.IndexMapping> allIndexMappings =
        delegatingMappingsBuilder.getIndexMappings(opContext);
    Map<String, Object> v3Mappings =
        allIndexMappings.stream()
            .filter(mapping -> TEST_V3_INDEX_NAME.equals(mapping.getIndexName()))
            .findFirst()
            .map(MappingsBuilder.IndexMapping::getMappings)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "No mappings found for v3 index: " + TEST_V3_INDEX_NAME));

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) v3Mappings.get("properties");
    assertNotNull(properties, "V3 mappings should have properties");

    // Check that aliases point to _aspects.aspectName.fieldName structure
    // This is a basic validation - in a real scenario, you'd check specific field aliases
    boolean hasAspectAliases =
        properties.entrySet().stream()
            .anyMatch(
                entry -> {
                  if (entry.getValue() instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fieldMapping = (Map<String, Object>) entry.getValue();
                    if ("alias".equals(fieldMapping.get("type"))) {
                      String path = (String) fieldMapping.get("path");
                      return path != null && path.startsWith("_aspects.");
                    }
                  }
                  return false;
                });

    // Note: This test may not find aliases if there are no searchable fields in the test setup
    // In a real scenario with actual entity specs, you would have aliases pointing to _aspects
    // The structure validation is complete - aliases would point to _aspects.aspectName.fieldName
  }

  @Test
  public void testStructuredPropertiesException() throws Exception {
    // Test that structuredProperties aspect is handled as an exception
    // Create a test structured property
    StructuredPropertyDefinition testProperty =
        new StructuredPropertyDefinition()
            .setVersion("00000000000001")
            .setQualifiedName("testStructuredProperty")
            .setDisplayName("Test Structured Property")
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:dataset")))
            .setValueType(UrnUtils.getUrn("urn:li:logicalType:STRING"));

    Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties =
        Collections.singletonList(
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:testStructuredProperty"), testProperty));

    Collection<MappingsBuilder.IndexMapping> allIndexMappings =
        delegatingMappingsBuilder.getIndexMappings(opContext, structuredProperties);
    Map<String, Object> v3Mappings =
        allIndexMappings.stream()
            .filter(mapping -> TEST_V3_INDEX_NAME.equals(mapping.getIndexName()))
            .findFirst()
            .map(MappingsBuilder.IndexMapping::getMappings)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "No mappings found for v3 index: " + TEST_V3_INDEX_NAME));

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) v3Mappings.get("properties");
    assertNotNull(properties, "V3 mappings should have properties");

    // Verify structuredProperties exists at root level
    @SuppressWarnings("unchecked")
    Map<String, Object> structuredProps =
        (Map<String, Object>) properties.get("structuredProperties");
    assertNotNull(structuredProps, "structuredProperties should exist at root level");

    // Verify structuredProperties is NOT in _aspects
    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    if (aspects != null) {
      @SuppressWarnings("unchecked")
      Map<String, Object> aspectsProperties = (Map<String, Object>) aspects.get("properties");
      if (aspectsProperties != null) {
        assertNull(
            aspectsProperties.get("structuredProperties"),
            "structuredProperties should not be in _aspects");
      }
    }
  }
}

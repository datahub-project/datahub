package com.linkedin.metadata.search;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.system.cron.steps.TweakReplicasStep;
import com.linkedin.metadata.search.indexbuilder.IndexBuilderTestBase;
import com.linkedin.metadata.search.opensearch.OpenSearchSuite;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.IndicesClient;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.core.CountRequest;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Import({OpenSearchSuite.class, SearchTestContainerConfiguration.class})
// @SpringBootTest(classes = {UpgradeCliApplication.class,
// UpgradeCliApplicationTestConfiguration.class},
//        args = {"-u", "SystemUpdateCron", "-a", "stepType=TweakReplicasStep"})
public class TweakReplicasStepTest extends IndexBuilderTestBase {

  private static final String TEST_INDEX_NAME = "esindex_builder_test";
  @Autowired private RestHighLevelClient _searchClient;
  private IndicesClient _indexClient;
  @Autowired private TweakReplicasStep step;
  private Map<String, Optional<String>> parsedArgs;

  @Mock private UpgradeReport mockReport;
  @Mock private UpgradeContext mockContext;
  @Mock private OperationContext mockOpContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    TweakReplicasStep = new TweakReplicasStep();
    parsedArgs = new HashMap<>();
    when(mockContext.parsedArgs()).thenReturn(parsedArgs);
    when(mockContext.report()).thenReturn(mockReport);
    when(mockContext.opContext()).thenReturn(mockOpContext);

    // Setup default result for entityService
    RestoreIndicesResult mockResult = new RestoreIndicesResult();
    mockResult.rowsMigrated = 0;
    mockResult.ignored = 0;

    when(mockEntityService.restoreIndices(eq(mockOpContext), any(RestoreIndicesArgs.class), any()))
        .thenReturn(Collections.singletonList(mockResult));
  }

  public RestHighLevelClient getSearchClient() {
    return _searchClient;
  }

  @Test
  public void testTweakReplicasStepChanges() throws Exception {
    testDefaultBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
    // assert initial state, index has 0 docs, 1 replica
    assertEquals("1", getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"));
    long numDocs =
        getSearchClient()
            .count(new CountRequest(TEST_INDEX_NAME), RequestOptions.DEFAULT)
            .getCount();
    assertEquals(0, numDocs, "Expected 0 documents in the test index");
    // 1 replica with 0 docs --> 0 replica
    step.executable().apply(mockContext);
    assertEquals("0", getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"));
    // verify not undesired changes
    step.executable().apply(mockContext);
    assertEquals(0, getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"));
    // index one doc
    Map<String, Object> document = new HashMap<>();
    document.put("name", "somedoc");
    IndexRequest indexRequest =
        new IndexRequest(TEST_INDEX_NAME).id("1").source(document, XContentType.JSON);
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
    step.executable().apply(mockContext);
    assertEquals(1, getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"));
    // verify not undesired changes
    step.executable().apply(mockContext);
    assertEquals(1, getTestIndex().getSetting(TEST_INDEX_NAME, "index.number_of_replicas"));
  }

  //    assertTrue(getSearchClient().get
  //            Arrays.stream(getTestIndex().getIndices()).noneMatch(name ->
  // name.equals(TEST_INDEX_NAME)),
  //    String expectedShards = "0";
  //    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
  //    ESIndexBuilder changedShardBuilder =
  //            new ESIndexBuilder(
  //                    getSearchClient(),
  //                    Integer.parseInt(expectedShards),
  //                    testDefaultBuilder.getNumReplicas(),
  //                    testDefaultBuilder.getNumRetries(),
  //                    testDefaultBuilder.getRefreshIntervalSeconds(),
  //                    Map.of(),
  //                    true,
  //                    false,
  //                    false,
  //                    new ElasticSearchConfiguration(),
  //                    gitVersion);
  //
  //    // add new shard setting
  //    changedShardBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
  //    assertTrue(
  //            Arrays.stream(getTestIndex().getIndices()).noneMatch(name ->
  // name.equals(TEST_INDEX_NAME)),
  //            "Expected original index to be replaced with alias");
  //
  //    Map.Entry<String, List<AliasMetadata>> newIndex =
  //            getTestIndex().getAliases().entrySet().stream()
  //                    .filter(
  //                            e ->
  //                                    e.getValue().stream()
  //                                            .anyMatch(aliasMeta ->
  // aliasMeta.alias().equals(TEST_INDEX_NAME)))
  //                    .findFirst()
  //                    .get();
  //
  //    String afterCreationDate = getTestIndex().getSetting(newIndex.getKey(),
  // "index.creation_date");
  //    assertNotEquals(
  //            beforeCreationDate, afterCreationDate, "Expected reindex to result in different
  // timestamp");
  //    assertEquals(
  //            expectedShards,
  //            getTestIndex().getSetting(newIndex.getKey(), "index.number_of_shards"),
  //            "Expected number of shards: " + expectedShards);
  //  }

  //  @Test
  //  public void testESIndexBuilderCreation() throws Exception {
  //    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
  //    ESIndexBuilder customIndexBuilder =
  //            new ESIndexBuilder(
  //                    getSearchClient(),
  //                    2,
  //                    0,
  //                    1,
  //                    0,
  //                    Map.of(),
  //                    false,
  //                    false,
  //                    false,
  //                    new ElasticSearchConfiguration(),
  //                    gitVersion);
  //    customIndexBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
  //    GetIndexResponse resp = getTestIndex();
  //
  //    assertEquals("2", resp.getSetting(TEST_INDEX_NAME, "index.number_of_shards"));
  //    assertEquals("0", resp.getSetting(TEST_INDEX_NAME, "index.number_of_replicas"));
  //    assertEquals("0s", resp.getSetting(TEST_INDEX_NAME, "index.refresh_interval"));
  //  }

}

package com.linkedin.metadata.systemmetadata;

import static io.datahubproject.test.search.SearchTestUtils.TEST_SYSTEM_METADATA_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.syncAfterWrite;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.IngestionRunSummary;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.mxe.SystemMetadata;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public abstract class SystemMetadataServiceTestBase extends AbstractTestNGSpringContextTests {

  @Nonnull
  protected abstract RestHighLevelClient getSearchClient();

  @Nonnull
  protected abstract ESBulkProcessor getBulkProcessor();

  @Nonnull
  protected abstract ESIndexBuilder getIndexBuilder();

  private final IndexConvention _indexConvention =
      new IndexConventionImpl(
          IndexConventionImpl.IndexConventionConfig.builder()
              .prefix("es_system_metadata_service_test")
              .hashIdAlgo("MD5")
              .build());

  private ElasticSearchSystemMetadataService _client;

  @BeforeClass
  public void setup() {
    _client = buildService();
    _client.reindexAll(Collections.emptySet());
  }

  @BeforeMethod
  public void wipe() throws Exception {
    syncAfterWrite(getBulkProcessor());
    _client.clear();
    syncAfterWrite(getBulkProcessor());
  }

  @Nonnull
  private ElasticSearchSystemMetadataService buildService() {
    ESSystemMetadataDAO dao =
        new ESSystemMetadataDAO(
            getSearchClient(),
            _indexConvention,
            getBulkProcessor(),
            1,
            TEST_SYSTEM_METADATA_SERVICE_CONFIG);
    return new ElasticSearchSystemMetadataService(
        getBulkProcessor(),
        _indexConvention,
        dao,
        getIndexBuilder(),
        "MD5",
        TEST_SYSTEM_METADATA_SERVICE_CONFIG);
  }

  @Test
  public void testListRuns() throws Exception {
    SystemMetadata metadata1 = new SystemMetadata();
    metadata1.setRunId("abc-123");
    metadata1.setLastObserved(Long.valueOf(120));

    SystemMetadata metadata2 = new SystemMetadata();
    metadata2.setRunId("abc-456");
    metadata2.setLastObserved(Long.valueOf(240));

    _client.insert(metadata1, "urn:li:chart:1", "chartKey");
    _client.insert(metadata1, "urn:li:chart:1", "ChartInfo");
    _client.insert(metadata1, "urn:li:chart:1", "Ownership");

    _client.insert(metadata2, "urn:li:chart:2", "chartKey");
    _client.insert(metadata2, "urn:li:chart:2", "Ownership");

    syncAfterWrite(getBulkProcessor());

    List<IngestionRunSummary> runs = _client.listRuns(0, 20, false);

    assertEquals(runs.size(), 2);
    assertEquals(runs.get(0).getRows(), Long.valueOf(2));
    assertEquals(runs.get(1).getRows(), Long.valueOf(3));
  }

  @Test
  public void testOverwriteRuns() throws Exception {
    SystemMetadata metadata1 = new SystemMetadata();
    metadata1.setRunId("abc-123");
    metadata1.setLastObserved(Long.valueOf(120L));

    SystemMetadata metadata2 = new SystemMetadata();
    metadata2.setRunId("abc-456");
    metadata2.setLastObserved(Long.valueOf(240));

    _client.insert(metadata1, "urn:li:chart:1", "chartKey");
    _client.insert(metadata1, "urn:li:chart:1", "ChartInfo");
    _client.insert(metadata1, "urn:li:chart:1", "Ownership");

    _client.insert(metadata2, "urn:li:chart:1", "ChartInfo");
    _client.insert(metadata2, "urn:li:chart:1", "Ownership");

    _client.insert(metadata2, "urn:li:chart:2", "chartKey");
    _client.insert(metadata2, "urn:li:chart:2", "Ownership");

    syncAfterWrite(getBulkProcessor());

    List<IngestionRunSummary> runs = _client.listRuns(0, 20, false);

    assertEquals(runs.size(), 2);
    assertEquals(runs.get(0).getRows(), Long.valueOf(4));
    assertEquals(runs.get(1).getRows(), Long.valueOf(1));
  }

  @Test
  public void testFindByRunId() throws Exception {
    SystemMetadata metadata1 = new SystemMetadata();
    metadata1.setRunId("abc-123");
    metadata1.setLastObserved(Long.valueOf(120L));

    SystemMetadata metadata2 = new SystemMetadata();
    metadata2.setRunId("abc-456");
    metadata2.setLastObserved(Long.valueOf(240L));

    _client.insert(metadata1, "urn:li:chart:1", "chartKey");
    _client.insert(metadata1, "urn:li:chart:1", "ChartInfo");
    _client.insert(metadata1, "urn:li:chart:1", "Ownership");

    _client.insert(metadata2, "urn:li:chart:1", "ChartInfo");
    _client.insert(metadata2, "urn:li:chart:1", "Ownership");

    _client.insert(metadata2, "urn:li:chart:2", "chartKey");
    _client.insert(metadata2, "urn:li:chart:2", "Ownership");

    syncAfterWrite(getBulkProcessor());

    List<AspectRowSummary> rows = _client.findByRunId("abc-456", false, 0, null);

    assertEquals(rows.size(), 4);
    rows.forEach(row -> assertEquals(row.getRunId(), "abc-456"));
  }

  @Test
  public void testDelete() throws Exception {
    SystemMetadata metadata1 = new SystemMetadata();
    metadata1.setRunId("abc-123");
    metadata1.setLastObserved(Long.valueOf(120L));

    SystemMetadata metadata2 = new SystemMetadata();
    metadata2.setRunId("abc-456");
    metadata2.setLastObserved(Long.valueOf(240L));

    _client.insert(metadata1, "urn:li:chart:1", "chartKey");
    _client.insert(metadata1, "urn:li:chart:1", "ChartInfo");
    _client.insert(metadata1, "urn:li:chart:1", "Ownership");

    _client.insert(metadata2, "urn:li:chart:1", "ChartInfo");
    _client.insert(metadata2, "urn:li:chart:1", "Ownership");

    _client.insert(metadata2, "urn:li:chart:2", "chartKey");
    _client.insert(metadata2, "urn:li:chart:2", "Ownership");

    syncAfterWrite(getBulkProcessor());

    _client.deleteUrn("urn:li:chart:1");

    syncAfterWrite(getBulkProcessor());

    List<AspectRowSummary> rows = _client.findByRunId("abc-456", false, 0, null);

    assertEquals(rows.size(), 2);
    rows.forEach(row -> assertEquals(row.getRunId(), "abc-456"));
  }

  @Test
  public void testInsertNullData() throws Exception {
    _client.insert(null, "urn:li:chart:1", "chartKey");

    syncAfterWrite(getBulkProcessor());

    List<IngestionRunSummary> runs = _client.listRuns(0, 20, false);

    assertEquals(runs.size(), 0);
  }

  @Test
  public void testRaw() throws Exception {
    // Create test data with various system metadata
    SystemMetadata metadata1 = new SystemMetadata();
    metadata1.setRunId("abc-123");
    metadata1.setLastObserved(Long.valueOf(120L));
    metadata1.setRegistryName("test-registry");
    metadata1.setRegistryVersion("1.0.0");

    SystemMetadata metadata2 = new SystemMetadata();
    metadata2.setRunId("abc-456");
    metadata2.setLastObserved(Long.valueOf(240L));
    metadata2.setRegistryName("test-registry");
    metadata2.setRegistryVersion("2.0.0");

    // Insert test data
    _client.insert(metadata1, "urn:li:chart:1", "chartKey");
    _client.insert(metadata1, "urn:li:chart:1", "ChartInfo");
    _client.insert(metadata1, "urn:li:chart:1", "Ownership");

    _client.insert(metadata2, "urn:li:chart:2", "chartKey");
    _client.insert(metadata2, "urn:li:chart:2", "ChartInfo");

    _client.insert(metadata1, "urn:li:dataset:3", "DatasetKey");
    _client.insert(metadata1, "urn:li:dataset:3", "DatasetProperties");

    syncAfterWrite(getBulkProcessor());

    // Test 1: Query for specific URN with specific aspects
    Map<String, Set<String>> urnAspects1 = new HashMap<>();
    urnAspects1.put("urn:li:chart:1", new HashSet<>(Arrays.asList("chartKey", "ChartInfo")));

    Map<Urn, Map<String, Map<String, Object>>> result1 = _client.raw(null, urnAspects1);

    assertEquals(result1.size(), 1);
    assertTrue(result1.containsKey(UrnUtils.getUrn("urn:li:chart:1")));

    Map<String, Map<String, Object>> chart1Aspects = result1.get(UrnUtils.getUrn("urn:li:chart:1"));
    assertEquals(chart1Aspects.size(), 2);
    assertTrue(chart1Aspects.containsKey("chartKey"));
    assertTrue(chart1Aspects.containsKey("ChartInfo"));

    // Verify content of returned documents
    Map<String, Object> chartKeyDoc = chart1Aspects.get("chartKey");
    assertEquals(chartKeyDoc.get("urn"), "urn:li:chart:1");
    assertEquals(chartKeyDoc.get("aspect"), "chartKey");
    assertEquals(chartKeyDoc.get("runId"), "abc-123");
    assertEquals(chartKeyDoc.get("lastUpdated"), 120);
    assertEquals(chartKeyDoc.get("registryName"), "test-registry");
    assertEquals(chartKeyDoc.get("registryVersion"), "1.0.0");

    // Test 2: Query for multiple URNs
    Map<String, Set<String>> urnAspects2 = new HashMap<>();
    urnAspects2.put("urn:li:chart:1", new HashSet<>(Arrays.asList("Ownership")));
    urnAspects2.put("urn:li:chart:2", new HashSet<>(Arrays.asList("chartKey", "ChartInfo")));
    urnAspects2.put("urn:li:dataset:3", new HashSet<>(Arrays.asList("DatasetKey")));

    Map<Urn, Map<String, Map<String, Object>>> result2 = _client.raw(null, urnAspects2);

    assertEquals(result2.size(), 3);
    assertTrue(result2.containsKey(UrnUtils.getUrn("urn:li:chart:1")));
    assertTrue(result2.containsKey(UrnUtils.getUrn("urn:li:chart:2")));
    assertTrue(result2.containsKey(UrnUtils.getUrn("urn:li:dataset:3")));

    // Verify chart:1 has only Ownership
    assertEquals(result2.get(UrnUtils.getUrn("urn:li:chart:1")).size(), 1);
    assertTrue(result2.get(UrnUtils.getUrn("urn:li:chart:1")).containsKey("Ownership"));

    // Verify chart:2 has both aspects
    assertEquals(result2.get(UrnUtils.getUrn("urn:li:chart:2")).size(), 2);
    assertTrue(result2.get(UrnUtils.getUrn("urn:li:chart:2")).containsKey("chartKey"));
    assertTrue(result2.get(UrnUtils.getUrn("urn:li:chart:2")).containsKey("ChartInfo"));

    // Verify dataset:3 has DatasetKey
    assertEquals(result2.get(UrnUtils.getUrn("urn:li:dataset:3")).size(), 1);
    assertTrue(result2.get(UrnUtils.getUrn("urn:li:dataset:3")).containsKey("DatasetKey"));

    // Test 3: Query for non-existent URN
    Map<String, Set<String>> urnAspects3 = new HashMap<>();
    urnAspects3.put("urn:li:chart:999", new HashSet<>(Arrays.asList("chartKey")));

    Map<Urn, Map<String, Map<String, Object>>> result3 = _client.raw(null, urnAspects3);
    assertTrue(result3.isEmpty());

    // Test 4: Query for existing URN but non-existent aspect
    Map<String, Set<String>> urnAspects4 = new HashMap<>();
    urnAspects4.put("urn:li:chart:1", new HashSet<>(Arrays.asList("NonExistentAspect")));

    Map<Urn, Map<String, Map<String, Object>>> result4 = _client.raw(null, urnAspects4);
    assertTrue(result4.isEmpty());

    // Test 5: Empty input map
    Map<String, Set<String>> urnAspects5 = new HashMap<>();
    Map<Urn, Map<String, Map<String, Object>>> result5 = _client.raw(null, urnAspects5);
    assertTrue(result5.isEmpty());

    // Test 6: Null input
    Map<Urn, Map<String, Map<String, Object>>> result6 = _client.raw(null, null);
    assertTrue(result6.isEmpty());

    // Test 7: URN with null aspects set
    Map<String, Set<String>> urnAspects7 = new HashMap<>();
    urnAspects7.put("urn:li:chart:1", null);

    Map<Urn, Map<String, Map<String, Object>>> result7 = _client.raw(null, urnAspects7);
    assertTrue(result7.isEmpty());

    // Test 8: URN with empty aspects set
    Map<String, Set<String>> urnAspects8 = new HashMap<>();
    urnAspects8.put("urn:li:chart:1", new HashSet<>());

    Map<Urn, Map<String, Map<String, Object>>> result8 = _client.raw(null, urnAspects8);
    assertTrue(result8.isEmpty());
  }
}

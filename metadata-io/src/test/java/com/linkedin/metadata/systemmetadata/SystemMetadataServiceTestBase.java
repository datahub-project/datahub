package com.linkedin.metadata.systemmetadata;

import static io.datahubproject.test.search.SearchTestUtils.syncAfterWrite;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.IngestionRunSummary;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.mxe.SystemMetadata;
import java.util.Collections;
import java.util.List;
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
        new ESSystemMetadataDAO(getSearchClient(), _indexConvention, getBulkProcessor(), 1);
    return new ElasticSearchSystemMetadataService(
        getBulkProcessor(), _indexConvention, dao, getIndexBuilder(), "MD5");
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

    List<AspectRowSummary> rows = _client.findByRunId("abc-456", false, 0, ESUtils.MAX_RESULT_SIZE);

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

    List<AspectRowSummary> rows = _client.findByRunId("abc-456", false, 0, ESUtils.MAX_RESULT_SIZE);

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
}

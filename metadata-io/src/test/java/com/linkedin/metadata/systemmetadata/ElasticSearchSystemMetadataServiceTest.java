package com.linkedin.metadata.systemmetadata;

import com.linkedin.metadata.ElasticTestUtils;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.IngestionRunSummary;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchServiceTest;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.mxe.SystemMetadata;
import org.elasticsearch.client.RestHighLevelClient;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.List;

import static com.linkedin.metadata.DockerTestUtils.checkContainerEngine;
import static com.linkedin.metadata.ElasticSearchTestUtils.syncAfterWrite;
import static com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService.INDEX_NAME;
import static org.testng.Assert.assertEquals;


public class ElasticSearchSystemMetadataServiceTest {

  private ElasticsearchContainer _elasticsearchContainer;
  private RestHighLevelClient _searchClient;
  private final IndexConvention _indexConvention = new IndexConventionImpl(null);
  private final String _indexName = _indexConvention.getIndexName(INDEX_NAME);
  private ElasticSearchSystemMetadataService _client;

  @BeforeClass
  public void setup() {
    _elasticsearchContainer = ElasticTestUtils.getNewElasticsearchContainer();
    checkContainerEngine(_elasticsearchContainer.getDockerClient());
    _elasticsearchContainer.start();
    _searchClient = ElasticTestUtils.buildRestClient(_elasticsearchContainer);
    _client = buildService();
    _client.configure();
  }

  @BeforeMethod
  public void wipe() throws Exception {
    _client.clear();
    syncAfterWrite(_searchClient, _indexName);
  }

  @Nonnull
  private ElasticSearchSystemMetadataService buildService() {
    ESSystemMetadataDAO dao = new ESSystemMetadataDAO(_searchClient, _indexConvention,
        ElasticSearchServiceTest.getBulkProcessor(_searchClient));
    return new ElasticSearchSystemMetadataService(_searchClient, _indexConvention, dao,
        ElasticSearchServiceTest.getIndexBuilder(_searchClient));
  }

  @AfterClass
  public void tearDown() {
    _elasticsearchContainer.stop();
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

    syncAfterWrite(_searchClient, _indexName);

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

    syncAfterWrite(_searchClient, _indexName);

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

    syncAfterWrite(_searchClient, _indexName);

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

    syncAfterWrite(_searchClient, _indexName);

    _client.deleteUrn("urn:li:chart:1");

    syncAfterWrite(_searchClient, _indexName);

    List<AspectRowSummary> rows = _client.findByRunId("abc-456", false, 0, ESUtils.MAX_RESULT_SIZE);

    assertEquals(rows.size(), 2);
    rows.forEach(row -> assertEquals(row.getRunId(), "abc-456"));
  }

  @Test
  public void testInsertNullData() throws Exception {
    _client.insert(null, "urn:li:chart:1", "chartKey");

    syncAfterWrite(_searchClient, _indexName);

    List<IngestionRunSummary> runs = _client.listRuns(0, 20, false);

    assertEquals(runs.size(), 0);
  }
}

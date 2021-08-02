package com.linkedin.metadata.systemmetadata;

import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.IngestionRunSummary;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.mxe.SystemMetadata;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ElasticSearchSystemMetadataServiceTest {

  private ElasticsearchContainer _elasticsearchContainer;
  private RestHighLevelClient _searchClient;
  private IndexConvention _indexConvention;
  private ElasticSearchSystemMetadataService _client;

  private static final String IMAGE_NAME = "docker.elastic.co/elasticsearch/elasticsearch:7.9.3";
  private static final int HTTP_PORT = 9200;

  @BeforeMethod
  public void wipe() throws URISyntaxException {
    _client.clear();
  }

  @BeforeTest
  public void setup() {
    _indexConvention = new IndexConventionImpl(null);
    _elasticsearchContainer = new ElasticsearchContainer(IMAGE_NAME);
    _elasticsearchContainer.start();
    _searchClient = buildRestClient();
    _client = buildService();
    _client.configure();
  }

  @Nonnull
  private RestHighLevelClient buildRestClient() {
    final RestClientBuilder builder =
        RestClient.builder(new HttpHost("localhost", _elasticsearchContainer.getMappedPort(HTTP_PORT), "http"))
            .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultIOReactorConfig(
                IOReactorConfig.custom().setIoThreadCount(1).build()));

    builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.
        setConnectionRequestTimeout(3000));

    return new RestHighLevelClient(builder);
  }

  @Nonnull
  private ElasticSearchSystemMetadataService buildService() {
    ESSystemMetadataDAO dao = new ESSystemMetadataDAO(_searchClient, _indexConvention, 1, 1, 1, 1);
    return new ElasticSearchSystemMetadataService(_searchClient, _indexConvention, dao);
  }

  @AfterTest
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

    TimeUnit.SECONDS.sleep(5);

    List<IngestionRunSummary> runs = _client.listRuns(0, 20);

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

    TimeUnit.SECONDS.sleep(5);

    List<IngestionRunSummary> runs = _client.listRuns(0, 20);

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

    TimeUnit.SECONDS.sleep(5);

    List<AspectRowSummary> rows = _client.findByRunId("abc-456");

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

    TimeUnit.SECONDS.sleep(5);

    _client.deleteUrn("urn:li:chart:1");

    TimeUnit.SECONDS.sleep(5);

    List<AspectRowSummary> rows = _client.findByRunId("abc-456");

    assertEquals(rows.size(), 2);
    rows.forEach(row -> assertEquals(row.getRunId(), "abc-456"));
  }
}

package com.linkedin.metadata.graph;

import com.linkedin.metadata.ElasticSearchTestUtils;
import com.linkedin.metadata.graph.elastic.ESGraphQueryDAO;
import com.linkedin.metadata.graph.elastic.ESGraphWriteDAO;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;

import javax.annotation.Nonnull;

import static com.linkedin.metadata.graph.elastic.ElasticSearchGraphService.INDEX_NAME;

public class ElasticSearchGraphServiceTest extends GraphServiceTestBase {

  private ElasticsearchContainer _elasticsearchContainer;
  private RestHighLevelClient _searchClient;
  private final IndexConvention _indexConvention = new IndexConventionImpl(null);
  private final String _indexName = _indexConvention.getIndexName(INDEX_NAME);
  private ElasticSearchGraphService _client;

  private static final String IMAGE_NAME = "docker.elastic.co/elasticsearch/elasticsearch:7.9.3";
  private static final int HTTP_PORT = 9200;

  @BeforeMethod
  public void wipe() throws Exception {
    _client.clear();
    syncAfterWrite();
  }

  @BeforeTest
  public void setup() {
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
  private ElasticSearchGraphService buildService() {
    ESGraphQueryDAO readDAO = new ESGraphQueryDAO(_searchClient, _indexConvention);
    ESGraphWriteDAO writeDAO = new ESGraphWriteDAO(_searchClient, _indexConvention, 1, 1, 1, 1);
    return new ElasticSearchGraphService(_searchClient, _indexConvention, writeDAO, readDAO);
  }

  @AfterTest
  public void tearDown() {
    _elasticsearchContainer.stop();
  }

  @Override
  protected @Nonnull GraphService getGraphService() {
    return _client;
  }

  @Override
  protected void syncAfterWrite() throws Exception {
    ElasticSearchTestUtils.syncAfterWrite(_searchClient, _indexName);
  }

}

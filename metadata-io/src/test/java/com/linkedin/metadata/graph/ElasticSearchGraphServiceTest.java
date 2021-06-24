package com.linkedin.metadata.graph;

import com.linkedin.common.urn.Urn;

import com.linkedin.metadata.graph.elastic.ESGraphQueryDAO;
import com.linkedin.metadata.graph.elastic.ESGraphWriteDAO;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import java.net.URISyntaxException;
import java.util.ArrayList;
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

import static com.linkedin.metadata.dao.utils.QueryUtils.*;
import static org.testng.Assert.*;


public class ElasticSearchGraphServiceTest {

  private ElasticsearchContainer _elasticsearchContainer;
  private RestHighLevelClient _searchClient;
  private IndexConvention _indexConvention;
  private ElasticSearchGraphService _client;

  private static final String IMAGE_NAME = "docker.elastic.co/elasticsearch/elasticsearch:7.9.3";
  private static final int HTTP_PORT = 9200;

  @BeforeMethod
  public void wipe() throws URISyntaxException {
    _client.removeNode(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"));
    _client.removeNode(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"));
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
  private ElasticSearchGraphService buildService() {
    ESGraphQueryDAO readDAO = new ESGraphQueryDAO(_searchClient, _indexConvention);
    ESGraphWriteDAO writeDAO = new ESGraphWriteDAO(_searchClient, _indexConvention, 1, 1, 1, 1);
    return new ElasticSearchGraphService(_searchClient, _indexConvention, writeDAO, readDAO);
  }

  @AfterTest
  public void tearDown() {
    _elasticsearchContainer.stop();
  }

  @Test
  public void testAddEdge() throws Exception {
    Edge edge1 = new Edge(
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
        "DownstreamOf");

    _client.addEdge(edge1);
    TimeUnit.SECONDS.sleep(5);

    List<String> edgeTypes = new ArrayList<>();
    edgeTypes.add("DownstreamOf");
    RelationshipFilter relationshipFilter = new RelationshipFilter();
    relationshipFilter.setDirection(RelationshipDirection.OUTGOING);
    relationshipFilter.setCriteria(EMPTY_FILTER.getCriteria());

    List<String> relatedUrns = _client.findRelatedUrns(
        "",
        newFilter("urn", "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        "",
        EMPTY_FILTER,
        edgeTypes,
        relationshipFilter,
        0,
        10);

    assertEquals(relatedUrns.size(), 1);
  }

  @Test
  public void testAddEdgeReverse() throws Exception {
    Edge edge1 = new Edge(
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        "DownstreamOf");

    _client.addEdge(edge1);
    TimeUnit.SECONDS.sleep(5);

    List<String> edgeTypes = new ArrayList<>();
    edgeTypes.add("DownstreamOf");
    RelationshipFilter relationshipFilter = new RelationshipFilter();
    relationshipFilter.setDirection(RelationshipDirection.INCOMING);
    relationshipFilter.setCriteria(EMPTY_FILTER.getCriteria());

    List<String> relatedUrns = _client.findRelatedUrns(
        "",
        newFilter("urn", "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        "",
        EMPTY_FILTER,
        edgeTypes,
        relationshipFilter,
        0,
        10);

    assertEquals(relatedUrns.size(), 1);
  }

  @Test
  public void testRemoveEdgesFromNode() throws Exception {
    Edge edge1 = new Edge(
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        "DownstreamOf");

    _client.addEdge(edge1);
    TimeUnit.SECONDS.sleep(5);

    List<String> edgeTypes = new ArrayList<>();
    edgeTypes.add("DownstreamOf");
    RelationshipFilter relationshipFilter = new RelationshipFilter();
    relationshipFilter.setDirection(RelationshipDirection.INCOMING);
    relationshipFilter.setCriteria(EMPTY_FILTER.getCriteria());

    List<String> relatedUrns = _client.findRelatedUrns(
        "",
        newFilter("urn", "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        "",
        EMPTY_FILTER,
        edgeTypes,
        relationshipFilter,
        0,
        10);

    assertEquals(relatedUrns.size(), 1);

    _client.removeEdgesFromNode(Urn.createFromString(
        "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        edgeTypes,
        relationshipFilter);
    TimeUnit.SECONDS.sleep(5);

    List<String> relatedUrnsPostDelete = _client.findRelatedUrns(
        "",
        newFilter("urn", "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"),
        "",
        EMPTY_FILTER,
        edgeTypes,
        relationshipFilter,
        0,
        10);

    assertEquals(relatedUrnsPostDelete.size(), 0);
  }
}

package com.linkedin.metadata.search.elasticsearch;

import com.datahub.test.Snapshot;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.ElasticTestUtils;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Collections;

import static com.linkedin.metadata.DockerTestUtils.checkContainerEngine;
import static com.linkedin.metadata.ElasticSearchTestUtils.syncAfterWrite;
import static org.testng.Assert.assertEquals;


public class ElasticSearchServiceTest {

  private ElasticsearchContainer _elasticsearchContainer;
  private RestHighLevelClient _searchClient;
  private EntityRegistry _entityRegistry;
  private IndexConvention _indexConvention;
  private SettingsBuilder _settingsBuilder;
  private ElasticSearchService _elasticSearchService;

  private static final String ENTITY_NAME = "testEntity";

  @BeforeClass
  public void setup() {
    _entityRegistry = new SnapshotEntityRegistry(new Snapshot());
    _indexConvention = new IndexConventionImpl(null);
    _elasticsearchContainer = ElasticTestUtils.getNewElasticsearchContainer();
    _settingsBuilder = new SettingsBuilder(Collections.emptyList(), null);
    checkContainerEngine(_elasticsearchContainer.getDockerClient());
    _elasticsearchContainer.start();
    _searchClient = ElasticTestUtils.buildRestClient(_elasticsearchContainer);
    _elasticSearchService = buildService();
    _elasticSearchService.configure();
  }

  @BeforeMethod
  public void wipe() throws Exception {
    _elasticSearchService.clear();
    syncAfterWrite(_searchClient);
  }

  public static BulkProcessor getBulkProcessor(RestHighLevelClient searchClient) {
    return BulkProcessor.builder((request, bulkListener) -> {
      searchClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
    }, BulkListener.getInstance())
        .setBulkActions(1)
        .setFlushInterval(TimeValue.timeValueSeconds(1))
        .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1000), 1))
        .build();
  }

  public static ESIndexBuilder getIndexBuilder(RestHighLevelClient searchClient) {
    return new ESIndexBuilder(searchClient, 1, 1, 3);
  }

  @Nonnull
  private ElasticSearchService buildService() {
    EntityIndexBuilders indexBuilders =
        new EntityIndexBuilders(getIndexBuilder(_searchClient), _entityRegistry, _indexConvention, _settingsBuilder);
    ESSearchDAO searchDAO = new ESSearchDAO(_entityRegistry, _searchClient, _indexConvention);
    ESBrowseDAO browseDAO = new ESBrowseDAO(_entityRegistry, _searchClient, _indexConvention);
    ESWriteDAO writeDAO =
        new ESWriteDAO(_entityRegistry, _searchClient, _indexConvention, getBulkProcessor(_searchClient));
    return new ElasticSearchService(indexBuilders, searchDAO, browseDAO, writeDAO);
  }

  @AfterClass
  public void tearDown() {
    _elasticsearchContainer.stop();
  }

  @Test
  public void testElasticSearchService() throws Exception {
    SearchResult searchResult = _elasticSearchService.search(ENTITY_NAME, "test", null, null, 0, 10);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    BrowseResult browseResult = _elasticSearchService.browse(ENTITY_NAME, "", null, 0, 10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 0);
    assertEquals(_elasticSearchService.docCount(ENTITY_NAME), 0);
    assertEquals(_elasticSearchService.aggregateByValue(ENTITY_NAME, "textField", null, 10).size(), 0);

    Urn urn = new TestEntityUrn("test", "testUrn", "VALUE_1");
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set("urn", JsonNodeFactory.instance.textNode(urn.toString()));
    document.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document.set("foreignKey", JsonNodeFactory.instance.textNode("urn:li:tag:Node.Value"));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document.toString(), urn.toString());
    syncAfterWrite(_searchClient);

    searchResult = _elasticSearchService.search(ENTITY_NAME, "test", null, null, 0, 10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    searchResult = _elasticSearchService.search(ENTITY_NAME, "foreignKey:Node", null, null, 0, 10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    browseResult = _elasticSearchService.browse(ENTITY_NAME, "", null, 0, 10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 1);
    assertEquals(browseResult.getGroups().get(0).getName(), "a");
    browseResult = _elasticSearchService.browse(ENTITY_NAME, "/a", null, 0, 10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 1);
    assertEquals(browseResult.getGroups().get(0).getName(), "b");
    assertEquals(_elasticSearchService.docCount(ENTITY_NAME), 1);
    assertEquals(_elasticSearchService.aggregateByValue(ENTITY_NAME, "textFieldOverride", null, 10),
        ImmutableMap.of("textFieldOverride", 1L));

    Urn urn2 = new TestEntityUrn("test", "testUrn2", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("random"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride2"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/b/c"));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document2.toString(), urn2.toString());
    syncAfterWrite(_searchClient);

    searchResult = _elasticSearchService.search(ENTITY_NAME, "test", null, null, 0, 10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    browseResult = _elasticSearchService.browse(ENTITY_NAME, "", null, 0, 10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 2);
    assertEquals(browseResult.getGroups().get(0).getName(), "a");
    assertEquals(browseResult.getGroups().get(1).getName(), "b");
    browseResult = _elasticSearchService.browse(ENTITY_NAME, "/a", null, 0, 10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 1);
    assertEquals(browseResult.getGroups().get(0).getName(), "b");
    assertEquals(_elasticSearchService.docCount(ENTITY_NAME), 2);
    assertEquals(_elasticSearchService.aggregateByValue(ENTITY_NAME, "textFieldOverride", null, 10),
        ImmutableMap.of("textFieldOverride", 1L, "textFieldOverride2", 1L));

    _elasticSearchService.deleteDocument(ENTITY_NAME, urn.toString());
    _elasticSearchService.deleteDocument(ENTITY_NAME, urn2.toString());
    syncAfterWrite(_searchClient);
    searchResult = _elasticSearchService.search(ENTITY_NAME, "test", null, null, 0, 10);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    browseResult = _elasticSearchService.browse(ENTITY_NAME, "", null, 0, 10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 0);
    assertEquals(_elasticSearchService.docCount(ENTITY_NAME), 0);
    assertEquals(_elasticSearchService.aggregateByValue(ENTITY_NAME, "textField", null, 10).size(), 0);
  }
}

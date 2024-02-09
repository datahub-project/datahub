package com.linkedin.metadata.search;

import static com.linkedin.metadata.Constants.ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH;
import static io.datahubproject.test.search.SearchTestUtils.syncAfterWrite;
import static org.testng.Assert.assertEquals;

import com.datahub.test.Snapshot;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import java.util.List;
import javax.annotation.Nonnull;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public abstract class TestEntityTestBase extends AbstractTestNGSpringContextTests {

  @Nonnull
  protected abstract RestHighLevelClient getSearchClient();

  @Nonnull
  protected abstract ESBulkProcessor getBulkProcessor();

  @Nonnull
  protected abstract ESIndexBuilder getIndexBuilder();

  @Nonnull
  protected abstract SearchConfiguration getSearchConfiguration();

  @Nonnull
  protected abstract CustomSearchConfiguration getCustomSearchConfiguration();

  private EntityRegistry _entityRegistry;
  private IndexConvention _indexConvention;
  private SettingsBuilder _settingsBuilder;
  private ElasticSearchService _elasticSearchService;

  private static final String ENTITY_NAME = "testEntity";

  @BeforeClass
  public void setup() {
    _entityRegistry = new SnapshotEntityRegistry(new Snapshot());
    _indexConvention = new IndexConventionImpl("es_service_test");
    _settingsBuilder = new SettingsBuilder(null);
    _elasticSearchService = buildService();
    _elasticSearchService.configure();
  }

  @BeforeClass
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @BeforeMethod
  public void wipe() throws Exception {
    _elasticSearchService.clear();
  }

  @Nonnull
  private ElasticSearchService buildService() {
    EntityIndexBuilders indexBuilders =
        new EntityIndexBuilders(
            getIndexBuilder(), _entityRegistry, _indexConvention, _settingsBuilder);
    ESSearchDAO searchDAO =
        new ESSearchDAO(
            _entityRegistry,
            getSearchClient(),
            _indexConvention,
            false,
            ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH,
            getSearchConfiguration(),
            null);
    ESBrowseDAO browseDAO =
        new ESBrowseDAO(
            _entityRegistry,
            getSearchClient(),
            _indexConvention,
            getSearchConfiguration(),
            getCustomSearchConfiguration());
    ESWriteDAO writeDAO =
        new ESWriteDAO(_entityRegistry, getSearchClient(), _indexConvention, getBulkProcessor(), 1);
    return new ElasticSearchService(indexBuilders, searchDAO, browseDAO, writeDAO);
  }

  @Test
  public void testElasticSearchServiceStructuredQuery() throws Exception {
    SearchResult searchResult =
        _elasticSearchService.search(
            List.of(ENTITY_NAME), "test", null, null, 0, 10, new SearchFlags().setFulltext(false));
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    BrowseResult browseResult = _elasticSearchService.browse(ENTITY_NAME, "", null, 0, 10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 0);
    assertEquals(_elasticSearchService.docCount(ENTITY_NAME), 0);
    assertEquals(
        _elasticSearchService
            .aggregateByValue(ImmutableList.of(ENTITY_NAME), "textField", null, 10)
            .size(),
        0);

    Urn urn = new TestEntityUrn("test", "urn1", "VALUE_1");
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set("urn", JsonNodeFactory.instance.textNode(urn.toString()));
    document.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document.set("foreignKey", JsonNodeFactory.instance.textNode("urn:li:tag:Node.Value"));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document.toString(), urn.toString());
    syncAfterWrite(getBulkProcessor());

    searchResult =
        _elasticSearchService.search(
            List.of(ENTITY_NAME), "test", null, null, 0, 10, new SearchFlags().setFulltext(false));
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    searchResult =
        _elasticSearchService.search(
            List.of(ENTITY_NAME),
            "foreignKey:Node",
            null,
            null,
            0,
            10,
            new SearchFlags().setFulltext(false));
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    browseResult = _elasticSearchService.browse(ENTITY_NAME, "", null, 0, 10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 1);
    assertEquals(browseResult.getGroups().get(0).getName(), "a");
    browseResult = _elasticSearchService.browse(ENTITY_NAME, "/a", null, 0, 10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 1);
    assertEquals(browseResult.getGroups().get(0).getName(), "b");
    assertEquals(_elasticSearchService.docCount(ENTITY_NAME), 1);
    assertEquals(
        _elasticSearchService.aggregateByValue(
            ImmutableList.of(ENTITY_NAME), "textFieldOverride", null, 10),
        ImmutableMap.of("textFieldOverride", 1L));

    Urn urn2 = new TestEntityUrn("test2", "urn2", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("random"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride2"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/b/c"));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document2.toString(), urn2.toString());
    syncAfterWrite(getBulkProcessor());

    searchResult =
        _elasticSearchService.search(
            List.of(ENTITY_NAME), "test2", null, null, 0, 10, new SearchFlags().setFulltext(false));
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn2);
    browseResult = _elasticSearchService.browse(ENTITY_NAME, "", null, 0, 10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 2);
    assertEquals(browseResult.getGroups().get(0).getName(), "a");
    assertEquals(browseResult.getGroups().get(1).getName(), "b");
    browseResult = _elasticSearchService.browse(ENTITY_NAME, "/a", null, 0, 10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 1);
    assertEquals(browseResult.getGroups().get(0).getName(), "b");
    assertEquals(_elasticSearchService.docCount(ENTITY_NAME), 2);
    assertEquals(
        _elasticSearchService.aggregateByValue(
            ImmutableList.of(ENTITY_NAME), "textFieldOverride", null, 10),
        ImmutableMap.of("textFieldOverride", 1L, "textFieldOverride2", 1L));

    _elasticSearchService.deleteDocument(ENTITY_NAME, urn.toString());
    _elasticSearchService.deleteDocument(ENTITY_NAME, urn2.toString());
    syncAfterWrite(getBulkProcessor());
    searchResult =
        _elasticSearchService.search(
            List.of(ENTITY_NAME), "test2", null, null, 0, 10, new SearchFlags().setFulltext(false));
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    browseResult = _elasticSearchService.browse(ENTITY_NAME, "", null, 0, 10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 0);
    assertEquals(_elasticSearchService.docCount(ENTITY_NAME), 0);
    assertEquals(
        _elasticSearchService
            .aggregateByValue(ImmutableList.of(ENTITY_NAME), "textField", null, 10)
            .size(),
        0);
  }

  @Test
  public void testElasticSearchServiceFulltext() throws Exception {
    SearchResult searchResult =
        _elasticSearchService.search(
            List.of(ENTITY_NAME), "test", null, null, 0, 10, new SearchFlags().setFulltext(true));
    assertEquals(searchResult.getNumEntities().intValue(), 0);

    Urn urn = new TestEntityUrn("test", "urn1", "VALUE_1");
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set("urn", JsonNodeFactory.instance.textNode(urn.toString()));
    document.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document.set("foreignKey", JsonNodeFactory.instance.textNode("urn:li:tag:Node.Value"));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document.toString(), urn.toString());
    syncAfterWrite(getBulkProcessor());

    searchResult =
        _elasticSearchService.search(
            List.of(ENTITY_NAME), "test", null, null, 0, 10, new SearchFlags().setFulltext(true));
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);

    assertEquals(_elasticSearchService.docCount(ENTITY_NAME), 1);
    assertEquals(
        _elasticSearchService.aggregateByValue(
            ImmutableList.of(ENTITY_NAME), "textFieldOverride", null, 10),
        ImmutableMap.of("textFieldOverride", 1L));

    Urn urn2 = new TestEntityUrn("test2", "urn2", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("random"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride2"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/b/c"));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document2.toString(), urn2.toString());
    syncAfterWrite(getBulkProcessor());

    searchResult =
        _elasticSearchService.search(
            List.of(ENTITY_NAME), "test2", null, null, 0, 10, new SearchFlags().setFulltext(true));
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn2);

    assertEquals(_elasticSearchService.docCount(ENTITY_NAME), 2);
    assertEquals(
        _elasticSearchService.aggregateByValue(
            ImmutableList.of(ENTITY_NAME), "textFieldOverride", null, 10),
        ImmutableMap.of("textFieldOverride", 1L, "textFieldOverride2", 1L));

    _elasticSearchService.deleteDocument(ENTITY_NAME, urn.toString());
    _elasticSearchService.deleteDocument(ENTITY_NAME, urn2.toString());
    syncAfterWrite(getBulkProcessor());
    searchResult =
        _elasticSearchService.search(
            List.of(ENTITY_NAME), "test2", null, null, 0, 10, new SearchFlags().setFulltext(true));
    assertEquals(searchResult.getNumEntities().intValue(), 0);

    assertEquals(_elasticSearchService.docCount(ENTITY_NAME), 0);
    assertEquals(
        _elasticSearchService
            .aggregateByValue(ImmutableList.of(ENTITY_NAME), "textField", null, 10)
            .size(),
        0);
  }
}

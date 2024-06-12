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
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
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

  private SettingsBuilder settingsBuilder;
  private ElasticSearchService elasticSearchService;
  private OperationContext opContext;

  private static final String ENTITY_NAME = "testEntity";

  @BeforeClass
  public void setup() {
    opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            new SnapshotEntityRegistry(new Snapshot()), new IndexConventionImpl("es_service_test"));
    settingsBuilder = new SettingsBuilder(null);
    elasticSearchService = buildService();
    elasticSearchService.configure();
  }

  @BeforeMethod
  public void wipe() throws Exception {
    syncAfterWrite(getBulkProcessor());
    elasticSearchService.clear(opContext);
    syncAfterWrite(getBulkProcessor());
  }

  @Nonnull
  private ElasticSearchService buildService() {
    EntityIndexBuilders indexBuilders =
        new EntityIndexBuilders(
            getIndexBuilder(),
            opContext.getEntityRegistry(),
            opContext.getSearchContext().getIndexConvention(),
            settingsBuilder);
    ESSearchDAO searchDAO =
        new ESSearchDAO(
            getSearchClient(),
            false,
            ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH,
            getSearchConfiguration(),
            null);
    ESBrowseDAO browseDAO =
        new ESBrowseDAO(
            getSearchClient(), getSearchConfiguration(), getCustomSearchConfiguration());
    ESWriteDAO writeDAO = new ESWriteDAO(getSearchClient(), getBulkProcessor(), 1);
    ElasticSearchService searchService =
        new ElasticSearchService(indexBuilders, searchDAO, browseDAO, writeDAO);
    return searchService;
  }

  @Test
  public void testElasticSearchServiceStructuredQuery() throws Exception {
    SearchResult searchResult =
        elasticSearchService.search(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            List.of(ENTITY_NAME),
            "test",
            null,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    BrowseResult browseResult =
        elasticSearchService.browse(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            ENTITY_NAME,
            "",
            null,
            0,
            10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 0);
    assertEquals(
        elasticSearchService.docCount(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)), ENTITY_NAME),
        0);
    assertEquals(
        elasticSearchService
            .aggregateByValue(
                opContext.withSearchFlags(flags -> flags.setFulltext(false)),
                ImmutableList.of(ENTITY_NAME),
                "textField",
                null,
                10)
            .size(),
        0);

    Urn urn = new TestEntityUrn("test", "urn1", "VALUE_1");
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set("urn", JsonNodeFactory.instance.textNode(urn.toString()));
    document.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document.set("foreignKey", JsonNodeFactory.instance.textNode("urn:li:tag:Node.Value"));
    elasticSearchService.upsertDocument(
        opContext, ENTITY_NAME, document.toString(), urn.toString());
    syncAfterWrite(getBulkProcessor());

    searchResult =
        elasticSearchService.search(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            List.of(ENTITY_NAME),
            "test",
            null,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    searchResult =
        elasticSearchService.search(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            List.of(ENTITY_NAME),
            "foreignKey:Node",
            null,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    browseResult =
        elasticSearchService.browse(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            ENTITY_NAME,
            "",
            null,
            0,
            10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 1);
    assertEquals(browseResult.getGroups().get(0).getName(), "a");
    browseResult =
        elasticSearchService.browse(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            ENTITY_NAME,
            "/a",
            null,
            0,
            10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 1);
    assertEquals(browseResult.getGroups().get(0).getName(), "b");
    assertEquals(
        elasticSearchService.docCount(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)), ENTITY_NAME),
        1);
    assertEquals(
        elasticSearchService.aggregateByValue(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            ImmutableList.of(ENTITY_NAME),
            "textFieldOverride",
            null,
            10),
        ImmutableMap.of("textFieldOverride", 1L));

    Urn urn2 = new TestEntityUrn("test2", "urn2", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("random"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride2"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/b/c"));
    elasticSearchService.upsertDocument(
        opContext, ENTITY_NAME, document2.toString(), urn2.toString());
    syncAfterWrite(getBulkProcessor());

    searchResult =
        elasticSearchService.search(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            List.of(ENTITY_NAME),
            "test2",
            null,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn2);
    browseResult =
        elasticSearchService.browse(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            ENTITY_NAME,
            "",
            null,
            0,
            10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 2);
    assertEquals(browseResult.getGroups().get(0).getName(), "a");
    assertEquals(browseResult.getGroups().get(1).getName(), "b");
    browseResult =
        elasticSearchService.browse(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            ENTITY_NAME,
            "/a",
            null,
            0,
            10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 1);
    assertEquals(browseResult.getGroups().get(0).getName(), "b");
    assertEquals(
        elasticSearchService.docCount(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)), ENTITY_NAME),
        2);
    assertEquals(
        elasticSearchService.aggregateByValue(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            ImmutableList.of(ENTITY_NAME),
            "textFieldOverride",
            null,
            10),
        ImmutableMap.of("textFieldOverride", 1L, "textFieldOverride2", 1L));

    elasticSearchService.deleteDocument(opContext, ENTITY_NAME, urn.toString());
    elasticSearchService.deleteDocument(opContext, ENTITY_NAME, urn2.toString());
    syncAfterWrite(getBulkProcessor());
    searchResult =
        elasticSearchService.search(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            List.of(ENTITY_NAME),
            "test2",
            null,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    browseResult =
        elasticSearchService.browse(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            ENTITY_NAME,
            "",
            null,
            0,
            10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 0);
    assertEquals(
        elasticSearchService.docCount(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)), ENTITY_NAME),
        0);
    assertEquals(
        elasticSearchService
            .aggregateByValue(
                opContext.withSearchFlags(flags -> flags.setFulltext(false)),
                ImmutableList.of(ENTITY_NAME),
                "textField",
                null,
                10)
            .size(),
        0);
  }

  @Test
  public void testElasticSearchServiceFulltext() throws Exception {
    SearchResult searchResult =
        elasticSearchService.search(
            opContext.withSearchFlags(flags -> flags.setFulltext(true)),
            List.of(ENTITY_NAME),
            "test",
            null,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 0);

    Urn urn = new TestEntityUrn("test", "urn1", "VALUE_1");
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set("urn", JsonNodeFactory.instance.textNode(urn.toString()));
    document.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document.set("foreignKey", JsonNodeFactory.instance.textNode("urn:li:tag:Node.Value"));
    elasticSearchService.upsertDocument(
        opContext, ENTITY_NAME, document.toString(), urn.toString());
    syncAfterWrite(getBulkProcessor());

    searchResult =
        elasticSearchService.search(
            opContext.withSearchFlags(flags -> flags.setFulltext(true)),
            List.of(ENTITY_NAME),
            "test",
            null,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);

    assertEquals(
        elasticSearchService.docCount(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)), ENTITY_NAME),
        1);
    assertEquals(
        elasticSearchService.aggregateByValue(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            ImmutableList.of(ENTITY_NAME),
            "textFieldOverride",
            null,
            10),
        ImmutableMap.of("textFieldOverride", 1L));

    Urn urn2 = new TestEntityUrn("test2", "urn2", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("random"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride2"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/b/c"));
    elasticSearchService.upsertDocument(
        opContext, ENTITY_NAME, document2.toString(), urn2.toString());
    syncAfterWrite(getBulkProcessor());

    searchResult =
        elasticSearchService.search(
            opContext.withSearchFlags(flags -> flags.setFulltext(true)),
            List.of(ENTITY_NAME),
            "test2",
            null,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn2);

    assertEquals(
        elasticSearchService.docCount(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)), ENTITY_NAME),
        2);
    assertEquals(
        elasticSearchService.aggregateByValue(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            ImmutableList.of(ENTITY_NAME),
            "textFieldOverride",
            null,
            10),
        ImmutableMap.of("textFieldOverride", 1L, "textFieldOverride2", 1L));

    elasticSearchService.deleteDocument(opContext, ENTITY_NAME, urn.toString());
    elasticSearchService.deleteDocument(opContext, ENTITY_NAME, urn2.toString());
    syncAfterWrite(getBulkProcessor());
    searchResult =
        elasticSearchService.search(
            opContext.withSearchFlags(flags -> flags.setFulltext(true)),
            List.of(ENTITY_NAME),
            "test2",
            null,
            null,
            0,
            10);
    assertEquals(searchResult.getNumEntities().intValue(), 0);

    assertEquals(
        elasticSearchService.docCount(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)), ENTITY_NAME),
        0);
    assertEquals(
        elasticSearchService
            .aggregateByValue(
                opContext.withSearchFlags(flags -> flags.setFulltext(false)),
                ImmutableList.of(ENTITY_NAME),
                "textField",
                null,
                10)
            .size(),
        0);
  }
}

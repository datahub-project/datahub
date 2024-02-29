package com.linkedin.metadata.search;

import static com.linkedin.metadata.Constants.ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH;
import static io.datahubproject.test.search.SearchTestUtils.syncAfterWrite;
import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
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
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import javax.annotation.Nonnull;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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

  @Autowired
  @Qualifier("snapshotRegistryAspectRetriever")
  AspectRetriever aspectRetriever;

  private IndexConvention indexConvention;
  private SettingsBuilder settingsBuilder;
  private ElasticSearchService elasticSearchService;
  private OperationContext opContext;

  private static final String ENTITY_NAME = "testEntity";

  @BeforeClass
  public void setup() {
    indexConvention = new IndexConventionImpl("es_service_test");
    opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            aspectRetriever.getEntityRegistry(), indexConvention);
    settingsBuilder = new SettingsBuilder(null);
    elasticSearchService = buildService();
    elasticSearchService.configure();
  }

  @BeforeClass
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @BeforeMethod
  public void wipe() throws Exception {
    elasticSearchService.clear();
  }

  @Nonnull
  private ElasticSearchService buildService() {
    EntityIndexBuilders indexBuilders =
        new EntityIndexBuilders(
            getIndexBuilder(),
            aspectRetriever.getEntityRegistry(),
            indexConvention,
            settingsBuilder);
    ESSearchDAO searchDAO =
        new ESSearchDAO(
            getSearchClient(),
            indexConvention,
            false,
            ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH,
            getSearchConfiguration(),
            null);
    ESBrowseDAO browseDAO =
        new ESBrowseDAO(
            getSearchClient(),
            indexConvention,
            getSearchConfiguration(),
            getCustomSearchConfiguration());
    ESWriteDAO writeDAO =
        new ESWriteDAO(
            aspectRetriever.getEntityRegistry(),
            getSearchClient(),
            indexConvention,
            getBulkProcessor(),
            1);
    ElasticSearchService searchService =
        new ElasticSearchService(indexBuilders, searchDAO, browseDAO, writeDAO);
    searchService.postConstruct(aspectRetriever);
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
    elasticSearchService.upsertDocument(ENTITY_NAME, document.toString(), urn.toString());
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
    elasticSearchService.upsertDocument(ENTITY_NAME, document2.toString(), urn2.toString());
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

    elasticSearchService.deleteDocument(ENTITY_NAME, urn.toString());
    elasticSearchService.deleteDocument(ENTITY_NAME, urn2.toString());
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
    elasticSearchService.upsertDocument(ENTITY_NAME, document.toString(), urn.toString());
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
    elasticSearchService.upsertDocument(ENTITY_NAME, document2.toString(), urn2.toString());
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

    elasticSearchService.deleteDocument(ENTITY_NAME, urn.toString());
    elasticSearchService.deleteDocument(ENTITY_NAME, urn2.toString());
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

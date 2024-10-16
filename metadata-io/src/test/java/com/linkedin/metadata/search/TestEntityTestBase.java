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
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public abstract class TestEntityTestBase extends AbstractTestNGSpringContextTests {

  private static final String BROWSE_V2_DELIMITER = "␟";

  @Nonnull
  protected abstract RestHighLevelClient getSearchClient();

  @Nonnull
  protected abstract ESBulkProcessor getBulkProcessor();

  @Nonnull
  protected abstract ESIndexBuilder getIndexBuilder();

  @Nonnull
  protected abstract SearchConfiguration getSearchConfiguration();

  private SettingsBuilder settingsBuilder;
  private ElasticSearchService elasticSearchService;
  private OperationContext opContext;

  private static final String ENTITY_NAME = "testEntity";

  @BeforeClass
  public void setup() {
    opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            new SnapshotEntityRegistry(new Snapshot()),
            new IndexConventionImpl(
                IndexConventionImpl.IndexConventionConfig.builder()
                    .prefix("es_service_test")
                    .hashIdAlgo("MD5")
                    .build()));
    settingsBuilder = new SettingsBuilder(null);
    elasticSearchService = buildService();
    elasticSearchService.reindexAll(Collections.emptySet());
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
            null,
            QueryFilterRewriteChain.EMPTY);
    ESBrowseDAO browseDAO =
        new ESBrowseDAO(
            getSearchClient(), getSearchConfiguration(), null, QueryFilterRewriteChain.EMPTY);
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
    BrowseResultV2 browseResultV2 =
        elasticSearchService.browseV2(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            ENTITY_NAME,
            "",
            null,
            "*",
            0,
            10);
    assertEquals(browseResultV2.getMetadata().getTotalNumEntities().longValue(), 0);

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
    document.set(
        "browsePathV2",
        JsonNodeFactory.instance.textNode(
            BROWSE_V2_DELIMITER + "a" + BROWSE_V2_DELIMITER + "b" + BROWSE_V2_DELIMITER + "c"));
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
    browseResultV2 =
        elasticSearchService.browseV2(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            ENTITY_NAME,
            "",
            null,
            "*",
            0,
            10);
    assertEquals(browseResultV2.getMetadata().getTotalNumEntities().longValue(), 1);
    assertEquals(browseResultV2.getGroups().get(0).getName(), "a");
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
    browseResultV2 =
        elasticSearchService.browseV2(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            ENTITY_NAME,
            BROWSE_V2_DELIMITER + "a",
            null,
            "*",
            0,
            10);
    assertEquals(browseResultV2.getMetadata().getTotalNumEntities().longValue(), 1);
    assertEquals(browseResultV2.getGroups().get(0).getName(), "b");
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
    document2.set(
        "browsePathV2",
        JsonNodeFactory.instance.textNode(BROWSE_V2_DELIMITER + "b" + BROWSE_V2_DELIMITER + "c"));
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
    browseResultV2 =
        elasticSearchService.browseV2(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            ENTITY_NAME,
            "",
            null,
            "*",
            0,
            10);
    assertEquals(browseResultV2.getMetadata().getTotalNumEntities().longValue(), 2);
    assertEquals(browseResultV2.getGroups().get(0).getName(), "a");
    assertEquals(browseResultV2.getGroups().get(1).getName(), "b");
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
    browseResultV2 =
        elasticSearchService.browseV2(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            ENTITY_NAME,
            BROWSE_V2_DELIMITER + "a",
            null,
            "*",
            0,
            10);
    assertEquals(browseResultV2.getMetadata().getTotalNumEntities().longValue(), 1);
    assertEquals(browseResultV2.getGroups().get(0).getName(), "b");
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
    browseResultV2 =
        elasticSearchService.browseV2(
            opContext.withSearchFlags(flags -> flags.setFulltext(false)),
            ENTITY_NAME,
            "",
            null,
            "*",
            0,
            10);
    assertEquals(browseResultV2.getMetadata().getTotalNumEntities().longValue(), 0);
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

  @Test
  public void testElasticSearchServiceDefaults() throws Exception {
    SearchResult searchResult =
        elasticSearchService.search(opContext, List.of(ENTITY_NAME), "test", null, null, 0, 10);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    BrowseResult browseResult =
        elasticSearchService.browse(opContext, ENTITY_NAME, "", null, 0, 10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 0);
    BrowseResultV2 browseResultV2 =
        elasticSearchService.browseV2(opContext, ENTITY_NAME, "", null, "*", 0, 10);
    assertEquals(browseResultV2.getMetadata().getTotalNumEntities().longValue(), 0);

    assertEquals(elasticSearchService.docCount(opContext, ENTITY_NAME), 0);
    assertEquals(
        elasticSearchService
            .aggregateByValue(opContext, ImmutableList.of(ENTITY_NAME), "textField", null, 10)
            .size(),
        0);

    Urn urn = new TestEntityUrn("test", "urn1", "VALUE_1");
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set("urn", JsonNodeFactory.instance.textNode(urn.toString()));
    document.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document.set("textFieldOverride", JsonNodeFactory.instance.textNode("user_id"));
    document.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document.set(
        "browsePathV2",
        JsonNodeFactory.instance.textNode(
            BROWSE_V2_DELIMITER + "a" + BROWSE_V2_DELIMITER + "b" + BROWSE_V2_DELIMITER + "c"));
    document.set("foreignKey", JsonNodeFactory.instance.textNode("urn:li:tag:Node.Value"));
    elasticSearchService.upsertDocument(
        opContext, ENTITY_NAME, document.toString(), urn.toString());
    syncAfterWrite(getBulkProcessor());

    searchResult =
        elasticSearchService.search(opContext, List.of(ENTITY_NAME), "test", null, null, 0, 10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    searchResult =
        elasticSearchService.search(
            opContext, List.of(ENTITY_NAME), "foreignKey:Node", null, null, 0, 10);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    browseResult = elasticSearchService.browse(opContext, ENTITY_NAME, "", null, 0, 10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 1);
    assertEquals(browseResult.getGroups().get(0).getName(), "a");
    browseResultV2 = elasticSearchService.browseV2(opContext, ENTITY_NAME, "", null, "*", 0, 10);
    assertEquals(browseResultV2.getMetadata().getTotalNumEntities().longValue(), 1);
    assertEquals(browseResultV2.getGroups().get(0).getName(), "a");
    browseResult = elasticSearchService.browse(opContext, ENTITY_NAME, "/a", null, 0, 10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 1);
    assertEquals(browseResult.getGroups().get(0).getName(), "b");
    browseResultV2 =
        elasticSearchService.browseV2(
            opContext, ENTITY_NAME, BROWSE_V2_DELIMITER + "a", null, "*", 0, 10);
    assertEquals(browseResultV2.getMetadata().getTotalNumEntities().longValue(), 1);
    assertEquals(browseResultV2.getGroups().get(0).getName(), "b");
    assertEquals(elasticSearchService.docCount(opContext, ENTITY_NAME), 1);
    assertEquals(
        elasticSearchService.aggregateByValue(
            opContext, ImmutableList.of(ENTITY_NAME), "textFieldOverride", null, 10),
        ImmutableMap.of("user_id", 1L));

    Urn urn2 = new TestEntityUrn("test2", "urn2", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("random"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("user id"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/b/c"));
    document2.set(
        "browsePathV2",
        JsonNodeFactory.instance.textNode(BROWSE_V2_DELIMITER + "b" + BROWSE_V2_DELIMITER + "c"));
    elasticSearchService.upsertDocument(
        opContext, ENTITY_NAME, document2.toString(), urn2.toString());
    syncAfterWrite(getBulkProcessor());

    searchResult =
        elasticSearchService.search(opContext, List.of(ENTITY_NAME), "user_id", null, null, 0, 10);
    assertEquals(searchResult.getNumEntities().intValue(), 2);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    browseResult = elasticSearchService.browse(opContext, ENTITY_NAME, "", null, 0, 10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 2);
    assertEquals(browseResult.getGroups().get(0).getName(), "a");
    assertEquals(browseResult.getGroups().get(1).getName(), "b");
    browseResultV2 =
        elasticSearchService.browseV2(opContext, ENTITY_NAME, "", null, "user_id", 0, 10);
    assertEquals(browseResultV2.getMetadata().getTotalNumEntities().longValue(), 2);
    assertEquals(browseResultV2.getGroups().get(0).getName(), "a");
    assertEquals(browseResultV2.getGroups().get(1).getName(), "b");
    browseResult = elasticSearchService.browse(opContext, ENTITY_NAME, "/a", null, 0, 10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 1);
    assertEquals(browseResult.getGroups().get(0).getName(), "b");
    browseResultV2 =
        elasticSearchService.browseV2(
            opContext, ENTITY_NAME, BROWSE_V2_DELIMITER + "a", null, "user_id", 0, 10);
    assertEquals(browseResultV2.getMetadata().getTotalNumEntities().longValue(), 1);
    assertEquals(browseResultV2.getGroups().get(0).getName(), "b");
    assertEquals(elasticSearchService.docCount(opContext, ENTITY_NAME), 2);
    assertEquals(
        elasticSearchService.aggregateByValue(
            opContext, ImmutableList.of(ENTITY_NAME), "textFieldOverride", null, 10),
        ImmutableMap.of("user_id", 1L, "user id", 1L));

    elasticSearchService.deleteDocument(opContext, ENTITY_NAME, urn.toString());
    elasticSearchService.deleteDocument(opContext, ENTITY_NAME, urn2.toString());
    syncAfterWrite(getBulkProcessor());
    searchResult =
        elasticSearchService.search(opContext, List.of(ENTITY_NAME), "*", null, null, 0, 10);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    browseResult = elasticSearchService.browse(opContext, ENTITY_NAME, "", null, 0, 10);
    assertEquals(browseResult.getMetadata().getTotalNumEntities().longValue(), 0);
    browseResultV2 = elasticSearchService.browseV2(opContext, ENTITY_NAME, "", null, "*", 0, 10);
    assertEquals(browseResultV2.getMetadata().getTotalNumEntities().longValue(), 0);
    assertEquals(elasticSearchService.docCount(opContext, ENTITY_NAME), 0);
    assertEquals(
        elasticSearchService
            .aggregateByValue(opContext, ImmutableList.of(ENTITY_NAME), "textField", null, 10)
            .size(),
        0);
  }
}

package com.linkedin.metadata.search.query;

import static com.linkedin.metadata.Constants.CHART_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_STRUCT_PROPS_DISABLED;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION;
import static io.datahubproject.test.search.SearchTestUtils.createDelegatingMappingsBuilder;
import static io.datahubproject.test.search.SearchTestUtils.createDelegatingSettingsBuilder;
import static io.datahubproject.test.search.SearchTestUtils.syncAfterWrite;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.chart.ChartInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.BrowsePathEntry;
import com.linkedin.common.BrowsePathEntryArray;
import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.ChangeAuditStamps;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.browse.BrowseResultGroupV2;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.AutoCompleteEntity;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.FilterValue;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.SearchWriteAccess;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.service.UpdateIndicesV3Strategy;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.metadata.utils.elasticsearch.ConfiguredIndexPrefixResolver;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import com.linkedin.metadata.utils.elasticsearch.V3IndexKeys;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.test.metadata.aspect.batch.TestMCL;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Keyword (non-vector) reads with Search V3 keyword reads on and the V2 entity indices disabled,
 * run against each engine's test container. Entities are written through the V3 update-indices
 * strategy, so no V2 entity index exists under the test prefix: a read that still resolved a V2
 * index would fail with index_not_found, or come back empty through a V2 wildcard.
 *
 * <p>Legacy browse ({@code browse}, {@code getBrowsePaths}) is not covered: it reads browsePaths
 * fields that only exist on V2 mappings.
 */
public abstract class KeywordSearchV3TestBase extends AbstractTestNGSpringContextTests {

  private static final Urn HIVE = UrnUtils.getUrn("urn:li:dataPlatform:hive");
  private static final Urn POSTGRES = UrnUtils.getUrn("urn:li:dataPlatform:postgres");
  private static final Urn ORDERS =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,sales.orders,PROD)");
  private static final Urn CUSTOMERS =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:postgres,sales.customers,PROD)");
  private static final Urn ORDERS_CHART = UrnUtils.getUrn("urn:li:chart:(looker,orders_by_region)");
  private static final List<String> ENTITY_TYPES = List.of(DATASET_ENTITY_NAME, CHART_ENTITY_NAME);
  private static final String BROWSE_DELIMITER = "␟";

  private final List<String> createdIndices = new ArrayList<>();
  private OperationContext opContext;
  private ElasticSearchService searchService;

  @Nonnull
  protected abstract SearchClientShim<?> getSearchClient();

  @Nonnull
  protected abstract ESBulkProcessor getBulkProcessor();

  @BeforeClass
  public void setUp() throws Exception {
    EntityIndexConfiguration entityIndex =
        V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION.toBuilder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(false).build())
            .v3(
                V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION.getV3().toBuilder()
                    .keywordReadEnabled(true)
                    .build())
            .build();
    ElasticSearchConfiguration config =
        TEST_ES_SEARCH_CONFIG.toBuilder().entityIndex(entityIndex).build();
    IndexConvention indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
            new ConfiguredIndexPrefixResolver("keywordv3"),
            entityIndex);
    EntityRegistry entityRegistry = TestOperationContexts.defaultEntityRegistry();
    MappingsBuilder mappingsBuilder = createDelegatingMappingsBuilder(entityIndex);
    opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            SearchContext.builder()
                .indexConvention(indexConvention)
                .searchClusterAccess(SearchClusterAccess.fixed(getSearchClient()))
                .searchableFieldTypes(
                    ESUtils.buildSearchableFieldTypes(entityRegistry, mappingsBuilder))
                .searchableFieldPaths(ESUtils.buildSearchableFieldPaths(entityRegistry))
                .build());

    ESIndexBuilder indexBuilder =
        new ESIndexBuilder(
            getSearchClient(),
            config,
            TEST_ES_STRUCT_PROPS_DISABLED,
            Map.of(),
            new GitVersion("0.0.0-test", "123456", Optional.empty()));
    SettingsBuilder settingsBuilder =
        createDelegatingSettingsBuilder(entityIndex, config.getIndex(), indexConvention);
    searchService =
        new ElasticSearchService(
            indexBuilder,
            TEST_SEARCH_SERVICE_CONFIG,
            config,
            mappingsBuilder,
            settingsBuilder,
            new ESSearchDAO(
                false, config, null, QueryFilterRewriteChain.EMPTY, TEST_SEARCH_SERVICE_CONFIG),
            new ESBrowseDAO(
                config, null, QueryFilterRewriteChain.EMPTY, TEST_SEARCH_SERVICE_CONFIG),
            new ESWriteDAO(
                config,
                getSearchClient(),
                getBulkProcessor(),
                SearchWriteAccess.fixed(getBulkProcessor())));

    // Only the seeded entity types' V3 indices; the registry would build one per entity type
    Map<String, Map<String, Object>> v3Mappings =
        mappingsBuilder.getIndexMappings(opContext).stream()
            .collect(
                Collectors.toMap(
                    MappingsBuilder.IndexMapping::getIndexName,
                    MappingsBuilder.IndexMapping::getMappings));
    for (String entityType : ENTITY_TYPES) {
      String indexName =
          indexConvention.getEntityIndexNameV3(
              opContext, V3IndexKeys.resolve(entityRegistry.getEntitySpec(entityType)));
      indexBuilder.buildIndex(
          opContext,
          indexBuilder.buildReindexState(
              opContext,
              indexName,
              v3Mappings.get(indexName),
              settingsBuilder.getSettings(config.getIndex(), indexName)));
      createdIndices.add(indexName);
    }

    new UpdateIndicesV3Strategy(
            entityIndex.getV3(),
            searchService,
            new SearchDocumentTransformer(1000, 1000, 1000, false, ESUtils.KEYWORD_MAXLENGTH),
            mock(TimeseriesAspectService.class),
            null)
        .processBatch(
            opContext,
            Map.of(
                ORDERS,
                events(
                    ORDERS,
                    new DatasetProperties().setName("orders"),
                    browsePaths("prod", "sales")),
                CUSTOMERS,
                events(
                    CUSTOMERS,
                    new DatasetProperties().setName("customers"),
                    browsePaths("prod", "marketing")),
                ORDERS_CHART,
                events(
                    ORDERS_CHART,
                    new ChartInfo()
                        .setTitle("Orders by region")
                        .setDescription("Monthly orders")
                        .setLastModified(new ChangeAuditStamps()),
                    browsePaths("prod", "sales"))),
            false);
    syncAfterWrite(getBulkProcessor());
  }

  @AfterClass(alwaysRun = true)
  public void deleteV3Indices() throws IOException {
    for (String index : createdIndices) {
      getSearchClient()
          .deleteIndex(
              OperationFingerprint.EMPTY, new DeleteIndexRequest(index), RequestOptions.DEFAULT);
    }
  }

  @Test
  public void testNoV2EntityIndexExists() throws IOException {
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    for (String entityType : ENTITY_TYPES) {
      String v2Index = indexConvention.getEntityIndexName(opContext, entityType);
      assertFalse(indexExists(v2Index), v2Index);
    }
    for (String v3Index : createdIndices) {
      assertTrue(indexExists(v3Index), v3Index);
    }
  }

  @Test
  public void testFilter() {
    assertUrns(
        searchService
            .filter(
                opContext,
                DATASET_ENTITY_NAME,
                QueryUtils.newFilter("platform", HIVE.toString()),
                null,
                0,
                10)
            .getEntities(),
        ORDERS);
    // Callers that name the V2 .keyword subfield read the root field
    assertUrns(
        searchService
            .filter(
                opContext,
                DATASET_ENTITY_NAME,
                QueryUtils.newFilter("platform.keyword", HIVE.toString()),
                null,
                0,
                10)
            .getEntities(),
        ORDERS);
    // The UI sends entity type enum names; V3 stores the registry entity name
    SearchResult byType =
        searchService.filter(
            opContext,
            DATASET_ENTITY_NAME,
            QueryUtils.newFilter("_entityType", "DATASET"),
            null,
            0,
            10);
    assertEquals(byType.getNumEntities(), 2);
    // Facet extraction gets the same value as the query, so the Type facet does not list the
    // filter value a second time next to its bucket
    assertEquals(
        byType.getMetadata().getAggregations().stream()
            .filter(agg -> agg.getName().equals("_entityType"))
            .flatMap(agg -> agg.getFilterValues().stream().map(FilterValue::getValue))
            .collect(Collectors.toList()),
        List.of(DATASET_ENTITY_NAME));
  }

  @Test
  public void testAggregateByValue() {
    assertEquals(
        searchService.aggregateByValue(
            opContext, List.of(DATASET_ENTITY_NAME), "platform", null, 10),
        Map.of(HIVE.toString(), 1L, POSTGRES.toString(), 1L));
  }

  @Test
  public void testDocCount() {
    assertEquals(searchService.docCount(opContext, DATASET_ENTITY_NAME, null), 2L);
    assertEquals(searchService.docCount(opContext, CHART_ENTITY_NAME, null), 1L);
    // Callers pass registry keys, which are lower-cased (glossaryterm for glossaryTerm), so the
    // stored entity type must match ignoring case
    assertEquals(searchService.docCount(opContext, "DATASET", null), 2L);
  }

  @Test
  public void testRawEntity() {
    Map<Urn, Map<String, Object>> raw = searchService.raw(opContext, Set.of(ORDERS, ORDERS_CHART));
    assertEquals(raw.keySet(), Set.of(ORDERS, ORDERS_CHART));
    // _entityType is only stored on V3 documents
    assertEquals(raw.get(ORDERS).get("_entityType"), DATASET_ENTITY_NAME);
    assertEquals(raw.get(ORDERS_CHART).get("_entityType"), CHART_ENTITY_NAME);
  }

  @Test
  public void testExplain() {
    // Callers pass the URN; V3 documents live under a hashed _id
    ExplainResponse explain =
        searchService.explain(
            opContext,
            "orders",
            ORDERS.toString(),
            DATASET_ENTITY_NAME,
            null,
            null,
            null,
            null,
            10,
            List.of());
    assertTrue(explain.isExists());
    assertTrue(explain.isMatch());
  }

  @Test
  public void testSearch() {
    OperationContext fulltext = opContext.withSearchFlags(flags -> flags.setFulltext(true));
    assertUrns(
        searchService
            .search(fulltext, List.of(DATASET_ENTITY_NAME), "orders", null, null, 0, 10)
            .getEntities(),
        ORDERS);

    SearchResult acrossEntities =
        searchService.search(
            fulltext,
            ENTITY_TYPES,
            "orders",
            null,
            null,
            0,
            10,
            List.of("platform", "_entityType"));
    assertUrns(acrossEntities.getEntities(), ORDERS, ORDERS_CHART);
    Map<String, Map<String, Long>> facets =
        acrossEntities.getMetadata().getAggregations().stream()
            .collect(
                Collectors.toMap(
                    AggregationMetadata::getName, AggregationMetadata::getAggregations));
    assertEquals(facets.get("platform"), Map.of(HIVE.toString(), 1L));
    assertEquals(facets.get("_entityType"), Map.of(DATASET_ENTITY_NAME, 1L, CHART_ENTITY_NAME, 1L));
    // Each returned Type value filters back to its entities
    for (Map.Entry<String, Urn> type :
        Map.of(DATASET_ENTITY_NAME, ORDERS, CHART_ENTITY_NAME, ORDERS_CHART).entrySet()) {
      assertUrns(
          searchService
              .search(
                  fulltext,
                  ENTITY_TYPES,
                  "orders",
                  QueryUtils.newFilter("_entityType", type.getKey()),
                  null,
                  0,
                  10)
              .getEntities(),
          type.getValue());
    }

    // Keyword sort on the root name alias: upper case sorts first
    assertEquals(
        searchService
            .search(
                fulltext,
                ENTITY_TYPES,
                "orders",
                null,
                List.of(new SortCriterion().setField("_entityName").setOrder(SortOrder.ASCENDING)),
                0,
                10)
            .getEntities()
            .stream()
            .map(SearchEntity::getEntity)
            .collect(Collectors.toList()),
        List.of(ORDERS_CHART, ORDERS));
  }

  @Test
  public void testScroll() {
    assertUrns(
        searchService
            .fullTextScroll(opContext, ENTITY_TYPES, "*", null, null, null, null, 10, List.of())
            .getEntities(),
        ORDERS,
        CUSTOMERS,
        ORDERS_CHART);
  }

  @Test
  public void testAutoComplete() {
    assertEquals(
        searchService
            .autoComplete(opContext, DATASET_ENTITY_NAME, "ord", null, null, 10)
            .getEntities()
            .stream()
            .map(AutoCompleteEntity::getUrn)
            .collect(Collectors.toList()),
        List.of(ORDERS));
  }

  @Test
  public void testBrowseV2() {
    BrowseResultV2 datasets =
        searchService.browseV2(opContext, DATASET_ENTITY_NAME, "", null, "*", 0, 10);
    assertEquals(datasets.getMetadata().getTotalNumEntities().longValue(), 2L);
    assertEquals(groups(datasets), Map.of("prod", 2L));

    BrowseResultV2 acrossEntities =
        searchService.browseV2(
            opContext, ENTITY_TYPES, BROWSE_DELIMITER + "prod", null, "*", 0, 10);
    assertEquals(acrossEntities.getMetadata().getTotalNumEntities().longValue(), 3L);
    assertEquals(groups(acrossEntities), Map.of("sales", 2L, "marketing", 1L));
  }

  private List<MCLItem> events(Urn urn, RecordTemplate... aspects) {
    EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(urn.getEntityType());
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());
    return Stream.concat(
            Stream.of(EntityKeyUtils.convertUrnToEntityKey(urn, entitySpec.getKeyAspectSpec())),
            Arrays.stream(aspects))
        .map(
            aspect ->
                (MCLItem)
                    TestMCL.builder()
                        .urn(urn)
                        .changeType(ChangeType.UPSERT)
                        .metadataChangeLog(new MetadataChangeLog())
                        .entitySpec(entitySpec)
                        .aspectSpec(
                            entitySpec.getAspectSpec(
                                PegasusUtils.getAspectNameFromSchema(aspect.schema())))
                        .recordTemplate(aspect)
                        .auditStamp(auditStamp)
                        .build())
        .collect(Collectors.toList());
  }

  private static BrowsePathsV2 browsePaths(String... ids) {
    return new BrowsePathsV2()
        .setPath(
            new BrowsePathEntryArray(
                Arrays.stream(ids)
                    .map(id -> new BrowsePathEntry().setId(id))
                    .collect(Collectors.toList())));
  }

  private static Map<String, Long> groups(BrowseResultV2 result) {
    return result.getGroups().stream()
        .collect(Collectors.toMap(BrowseResultGroupV2::getName, BrowseResultGroupV2::getCount));
  }

  private boolean indexExists(String index) throws IOException {
    return getSearchClient()
        .indexExists(opContext, new GetIndexRequest(index), RequestOptions.DEFAULT);
  }

  private static void assertUrns(SearchEntityArray entities, Urn... expected) {
    assertEqualsNoOrder(entities.stream().map(SearchEntity::getEntity).toArray(), expected);
  }
}

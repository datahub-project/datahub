package com.linkedin.metadata.search.query.v3;

import static com.linkedin.metadata.Constants.CHART_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DASHBOARD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_JOB_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_TYPE_URN_PREFIX;
import static com.linkedin.metadata.Constants.GLOSSARY_TERM_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder.STRUCTURED_QUERY_PREFIX;
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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.chart.ChartInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.BrowsePathEntry;
import com.linkedin.common.BrowsePathEntryArray;
import com.linkedin.common.BrowsePaths;
import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.ChangeAuditStamps;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dashboard.DashboardInfo;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.DataHubSearchConfig;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultEntity;
import com.linkedin.metadata.browse.BrowseResultGroup;
import com.linkedin.metadata.browse.BrowseResultGroupV2;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.config.MetadataChangeProposalConfig;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateSource;
import com.linkedin.metadata.config.search.SearchV3Configuration;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.AutoCompleteEntity;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.FilterValue;
import com.linkedin.metadata.search.MatchedField;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.SearchWriteAccess;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntityMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.UpdateIndicesV3Strategy;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.template.MergedSearchTemplate;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCoordinator;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFixtures;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateLoader;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateSurfaces;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplates;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.metadata.utils.elasticsearch.ConfiguredIndexPrefixResolver;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import com.linkedin.test.metadata.aspect.batch.TestMCL;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetMappingsRequest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Keyword (non-vector) reads with Search V3 keyword reads on, run against each engine's test
 * container. Entities are written through the V3 update-indices strategy only. With the V2 entity
 * indices disabled no V2 index exists under the test prefix, so a read that still resolved one
 * would fail with index_not_found, or come back empty through a V2 wildcard. With V2 enabled (see
 * {@link #isV2Enabled()}) the V2 indices exist but stay empty, so a read routed to V2 finds
 * nothing.
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
  // Its indices get no documents
  private static final String EMPTY_ENTITY_TYPE = GLOSSARY_TERM_ENTITY_NAME;
  // A camelCase entity type, and an entity whose _entityName aliases its own name field because
  // no aspect labels a field entityName
  private static final Urn NIGHTLY_JOB =
      UrnUtils.getUrn("urn:li:dataJob:(urn:li:dataFlow:(airflow,nightly,PROD),refresh)");
  private static final List<String> EXTRA_ENTITY_TYPES = List.of(DATA_JOB_ENTITY_NAME, "service");
  private static final String BROWSE_DELIMITER = "␟";
  private static final String SEARCH_TEMPLATE = "urn:li:searchTemplate:search";
  private static final String LIST_TEMPLATE = "urn:li:searchTemplate:list";
  private static final String FULLTEXT_TEMPLATE = "urn:li:searchTemplate:fulltext";
  private static final Urn RETENTION_POLICY =
      UrnUtils.getUrn("urn:li:structuredProperty:retentionPolicy");
  private static final Urn STEWARD_NOTE = UrnUtils.getUrn("urn:li:structuredProperty:stewardNote");
  // No searchConfiguration, so V3 does not analyze it
  private static final Urn INTERNAL_CODE =
      UrnUtils.getUrn("urn:li:structuredProperty:internalCode");
  // Indexed for an explicit field, absent from default search
  private static final Urn HIDDEN_LABEL = UrnUtils.getUrn("urn:li:structuredProperty:hiddenLabel");
  // queryByDefault and the legacy exclusion together: V3 ignores the exclusion
  private static final Urn LEGACY_EXCLUDED =
      UrnUtils.getUrn("urn:li:structuredProperty:legacyExcluded");
  // The ORDERS name and key id
  private static final Set<String> ORDERS_VALUES = Set.of("orders", "sales.orders");
  private static final Urn CUSTOMERS_OWNER = UrnUtils.getUrn("urn:li:corpuser:zelda");
  // Longer than the 100 characters the removed search tier keywords indexed
  private static final String CUSTOMERS_DESCRIPTION =
      "Customer master data joined from the billing, support and marketing systems, refreshed"
          + " nightly and kept for seven years";
  // Dashboards holding the same words, one in its title (shared entityName field, weight 10) and
  // one in its description (shared description field, weight 1). The urn tie-break alone would put
  // the description match first
  private static final Urn TITLE_MATCH = UrnUtils.getUrn("urn:li:dashboard:(looker,revenue_title)");
  private static final Urn DESCRIPTION_MATCH =
      UrnUtils.getUrn("urn:li:dashboard:(looker,revenue_description)");
  // Titled with a synonym of "staging" in the default synonym file, next to a title holding the
  // word itself
  private static final Urn STG_DASHBOARD = UrnUtils.getUrn("urn:li:dashboard:(looker,stg)");
  private static final Urn STAGING_DASHBOARD =
      UrnUtils.getUrn("urn:li:dashboard:(looker,staging_notes)");
  // A one-word title that a query only matches as a substring
  private static final Urn LIFETIME_DASHBOARD = UrnUtils.getUrn("urn:li:dashboard:(looker,ltv)");
  // Letter and digit runs in titles, next to a title sharing their letters
  private static final Urn CARGO_REPORT = UrnUtils.getUrn("urn:li:dashboard:(looker,dash_one)");
  private static final Urn CARGO_OVERVIEW = UrnUtils.getUrn("urn:li:dashboard:(looker,dash_two)");
  private static final Urn MANIFEST = UrnUtils.getUrn("urn:li:dashboard:(looker,dash_three)");
  private static final Urn FREIGHT_BOARD = UrnUtils.getUrn("urn:li:dashboard:(looker,dash_four)");
  private static final Urn FLEET_OVERVIEW = UrnUtils.getUrn("urn:li:dashboard:(looker,dash_five)");
  private static final Urn FLEET_NOTES = UrnUtils.getUrn("urn:li:dashboard:(looker,dash_six)");
  // Words of Indic and Thai scripts hold combining marks (vowel signs), which must not split them
  private static final Urn NON_LATIN = UrnUtils.getUrn("urn:li:dashboard:(looker,non_latin)");
  // A description holding snake_case identifiers, some with only short parts, and one holding a
  // part alone, whose urn comes first on a tie
  private static final Urn IDENTIFIERS =
      UrnUtils.getUrn("urn:li:dashboard:(looker,keyed_pipeline)");
  private static final Urn IDENTIFIER_PART =
      UrnUtils.getUrn("urn:li:dashboard:(looker,crawler_overview)");
  // A name only a stemmed match finds; its urn holds none of its words
  private static final Urn STEMMED_NAME = UrnUtils.getUrn("urn:li:dashboard:(looker,dash_4711)");
  private static final List<String> LONG_QUERY_WORDS =
      List.of(
          "zorbel", "quintax", "valdrin", "morphex", "brintel", "caldris", "fenwold", "glarvon",
          "hestrin", "jorvald", "kelstor", "lumbrix", "nervant", "orzelle", "pyxtral", "quorvex",
          "rendalt", "sylvorn", "tarquel", "umbrisk", "vintrel", "wexmore", "yarlund", "zephrin",
          "brockan", "cindral", "dravish", "elstorm", "fyndell");
  private static final Urn LONG_QUERY_DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,vocabulary,PROD)");
  private static final Urn LONG_QUERY_DASHBOARD =
      UrnUtils.getUrn("urn:li:dashboard:(looker,vocabulary)");

  private final List<String> createdIndices = new ArrayList<>();
  // Kept to create every registry index in testEngineAcceptsEveryRegistryIndex
  private MappingsBuilder mappingsBuilder;
  private MultiEntityMappingsBuilder engineV3MappingsBuilder;
  private ESIndexBuilder indexBuilder;
  private SettingsBuilder settingsBuilder;
  private IndexConfiguration indexConfiguration;
  private OperationContext opContext;
  private ElasticSearchConfiguration config;
  private ElasticSearchService searchService;

  @Nonnull
  protected abstract SearchClientShim<?> getSearchClient();

  @Nonnull
  protected abstract ESBulkProcessor getBulkProcessor();

  /** Keep the V2 entity indices enabled next to V3 keyword reads, as before V2 is turned off. */
  protected boolean isV2Enabled() {
    return false;
  }

  /** The same seeds {@code application.yaml} uses for Search V3 query templates. */
  private static SearchTemplateConfiguration shippedTemplateConfiguration() {
    SearchTemplateConfiguration templates = new SearchTemplateConfiguration();
    templates.setEnabled(true);
    SearchTemplateFixtures.setShippedLimits(templates);
    templates.setDefaults(
        Map.of(
            "fulltext", slot("urn:li:searchTemplate:search"),
            "structured", slot("urn:li:searchTemplate:structured")));
    templates.setSurfaces(
        Map.of(
            SearchTemplateSurfaces.SEARCH_ENTITIES,
            Map.of("fulltext", slot("urn:li:searchTemplate:searchLight")),
            SearchTemplateSurfaces.SCROLL_ENTITIES,
            Map.of("fulltext", slot("urn:li:searchTemplate:search")),
            SearchTemplateSurfaces.AGGREGATE_ENTITIES,
            Map.of("fulltext", slot("urn:li:searchTemplate:search"))));
    return templates;
  }

  private static Map<String, Object> slot(String urn) {
    Map<String, Object> slot = new HashMap<>();
    slot.put("default", urn);
    return slot;
  }

  @BeforeClass
  public void setUp() throws Exception {
    EntityIndexConfiguration entityIndex =
        V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION.toBuilder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(isV2Enabled()).build())
            .v3(
                V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION.getV3().toBuilder()
                    .keywordReadEnabled(true)
                    .build())
            .build();
    config =
        TEST_ES_SEARCH_CONFIG.toBuilder()
            .entityIndex(entityIndex)
            .search(
                TEST_ES_SEARCH_CONFIG.getSearch().toBuilder()
                    .v3(new SearchV3Configuration().setTemplates(shippedTemplateConfiguration()))
                    .build())
            .build();
    SearchTemplates.install(
        new SearchTemplateCoordinator(
            config, null, new SearchTemplateLoader(0, SearchTemplateSource.CLASSPATH)));
    IndexConvention indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
            new ConfiguredIndexPrefixResolver(isV2Enabled() ? "keywordv3dual" : "keywordv3"),
            entityIndex);
    EntityRegistry entityRegistry = TestOperationContexts.defaultEntityRegistry();
    mappingsBuilder = createDelegatingMappingsBuilder(entityIndex);
    // Built as MappingsBuilderFactory builds it, with the engine's own mapping details
    engineV3MappingsBuilder =
        new MultiEntityMappingsBuilder(entityIndex, null, ESUtils.KEYWORD_MAXLENGTH, List.of());
    // Both the document transformer and the filter resolver look the definition up
    DataHubSearchConfig searched = new DataHubSearchConfig().setQueryByDefault(true);
    StructuredPropertyDefinition retentionPolicy =
        new StructuredPropertyDefinition()
            .setQualifiedName(RETENTION_POLICY.getId())
            .setValueType(UrnUtils.getUrn(DATA_TYPE_URN_PREFIX + "string"))
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
            .setSearchConfiguration(searched);
    StructuredPropertyDefinition stewardNote =
        new StructuredPropertyDefinition()
            .setQualifiedName(STEWARD_NOTE.getId())
            .setValueType(UrnUtils.getUrn(DATA_TYPE_URN_PREFIX + "rich_text"))
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
            .setSearchConfiguration(new DataHubSearchConfig().setQueryByDefault(true));
    StructuredPropertyDefinition internalCode =
        new StructuredPropertyDefinition()
            .setQualifiedName(INTERNAL_CODE.getId())
            .setValueType(UrnUtils.getUrn(DATA_TYPE_URN_PREFIX + "string"))
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")));
    StructuredPropertyDefinition hiddenLabel =
        new StructuredPropertyDefinition()
            .setQualifiedName(HIDDEN_LABEL.getId())
            .setValueType(UrnUtils.getUrn(DATA_TYPE_URN_PREFIX + "string"))
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
            .setSearchConfiguration(new DataHubSearchConfig().setQueryByDefault(false));
    StructuredPropertyDefinition legacyExcluded =
        new StructuredPropertyDefinition()
            .setQualifiedName(LEGACY_EXCLUDED.getId())
            .setValueType(UrnUtils.getUrn(DATA_TYPE_URN_PREFIX + "string"))
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
            .setSearchConfiguration(
                new DataHubSearchConfig()
                    .setQueryByDefault(true)
                    .setExcludeFromFullTextSearch(true));
    MockAspectRetriever aspectRetriever =
        new MockAspectRetriever(
            Map.of(
                RETENTION_POLICY, List.of(retentionPolicy, new Status().setRemoved(false)),
                STEWARD_NOTE, List.of(stewardNote, new Status().setRemoved(false)),
                INTERNAL_CODE, List.of(internalCode, new Status().setRemoved(false)),
                HIDDEN_LABEL, List.of(hiddenLabel, new Status().setRemoved(false)),
                LEGACY_EXCLUDED, List.of(legacyExcluded, new Status().setRemoved(false))));
    aspectRetriever.setEntityRegistry(entityRegistry);
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .aspectRetriever(aspectRetriever)
            .cachingAspectRetriever(definitionsWithActiveUsers(aspectRetriever))
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(
                structuredPropertySearchRetriever(
                    List.of(
                        RETENTION_POLICY,
                        STEWARD_NOTE,
                        INTERNAL_CODE,
                        HIDDEN_LABEL,
                        LEGACY_EXCLUDED)))
            .build();
    opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            () -> retrieverContext,
            SearchContext.builder()
                .indexConvention(indexConvention)
                .searchClusterAccess(SearchClusterAccess.fixed(getSearchClient()))
                .searchableFieldTypes(
                    ESUtils.buildSearchableFieldTypes(entityRegistry, mappingsBuilder))
                .searchableFieldPaths(ESUtils.buildSearchableFieldPaths(entityRegistry))
                .build());
    opContext = opContext.withSearchSurface(SearchTemplateSurfaces.SEARCH_ENTITIES);

    indexBuilder =
        new ESIndexBuilder(
            getSearchClient(),
            config,
            TEST_ES_STRUCT_PROPS_DISABLED,
            Map.of(),
            new GitVersion("0.0.0-test", "123456", Optional.empty()));
    indexConfiguration = config.getIndex();
    settingsBuilder =
        createDelegatingSettingsBuilder(entityIndex, indexConfiguration, indexConvention);
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

    // Only the seeded entity types' indices plus the empty one; the registry would build one per
    // entity type
    Map<String, Map<String, Object>> mappings =
        mappingsBuilder
            .getIndexMappings(
                opContext,
                List.of(
                    Pair.of(RETENTION_POLICY, retentionPolicy),
                    Pair.of(STEWARD_NOTE, stewardNote),
                    Pair.of(INTERNAL_CODE, internalCode),
                    Pair.of(HIDDEN_LABEL, hiddenLabel),
                    Pair.of(LEGACY_EXCLUDED, legacyExcluded)))
            .stream()
            .collect(
                Collectors.toMap(
                    MappingsBuilder.IndexMapping::getIndexName,
                    MappingsBuilder.IndexMapping::getMappings));
    for (String entityType :
        Stream.of(
                ENTITY_TYPES,
                List.of(EMPTY_ENTITY_TYPE),
                EXTRA_ENTITY_TYPES,
                List.of(DASHBOARD_ENTITY_NAME))
            .flatMap(List::stream)
            .collect(Collectors.toList())) {
      List<String> indexNames = new ArrayList<>();
      indexNames.add(
          indexConvention.getEntityIndexNameV3(
              opContext, V3IndexKeys.resolve(entityRegistry.getEntitySpec(entityType))));
      if (isV2Enabled()) {
        indexNames.add(indexConvention.getEntityIndexName(opContext, entityType));
      }
      for (String indexName : indexNames) {
        indexBuilder.buildIndex(
            opContext,
            indexBuilder.buildReindexState(
                opContext,
                indexName,
                mappings.get(indexName),
                settingsBuilder.getSettings(config.getIndex(), indexName)));
        createdIndices.add(indexName);
      }
    }

    UpdateIndicesV3Strategy.builder()
        .v3Config(entityIndex.getV3())
        .elasticSearchService(searchService)
        .searchDocumentTransformer(
            new SearchDocumentTransformer(1000, 1000, 1000, false, ESUtils.KEYWORD_MAXLENGTH))
        .timeseriesAspectService(mock(TimeseriesAspectService.class))
        .build()
        .processBatch(
            opContext,
            seedEvents(
                ORDERS,
                events(
                    ORDERS,
                    new DatasetProperties().setName("orders"),
                    // Mixed case: keyword roots are normalized, so facets must read .keyword
                    new SubTypes().setTypeNames(new StringArray("Table")),
                    browsePaths("prod", "sales"),
                    legacyBrowsePaths("/prod/sales"),
                    new StructuredProperties()
                        .setProperties(
                            new StructuredPropertyValueAssignmentArray(
                                new StructuredPropertyValueAssignment()
                                    .setPropertyUrn(RETENTION_POLICY)
                                    .setValues(
                                        new PrimitivePropertyValueArray(
                                            PrimitivePropertyValue.create("90d"))),
                                new StructuredPropertyValueAssignment()
                                    .setPropertyUrn(LEGACY_EXCLUDED)
                                    .setValues(
                                        new PrimitivePropertyValueArray(
                                            PrimitivePropertyValue.create("nightjar")))))),
                CUSTOMERS,
                events(
                    CUSTOMERS,
                    new DatasetProperties()
                        .setName("customers")
                        .setDescription(CUSTOMERS_DESCRIPTION),
                    // editedName shares the name label, so full text and a name-only
                    // configuration both match it
                    new EditableDatasetProperties().setName("quarterly ledger"),
                    // owners is not queried by default
                    new Ownership()
                        .setOwners(
                            new OwnerArray(
                                new Owner()
                                    .setOwner(CUSTOMERS_OWNER)
                                    .setType(OwnershipType.DATAOWNER))),
                    browsePaths("prod", "marketing"),
                    legacyBrowsePaths("/prod/marketing", "/shared/crm"),
                    new StructuredProperties()
                        .setProperties(
                            new StructuredPropertyValueAssignmentArray(
                                new StructuredPropertyValueAssignment()
                                    .setPropertyUrn(STEWARD_NOTE)
                                    .setValues(
                                        new PrimitivePropertyValueArray(
                                            PrimitivePropertyValue.create(
                                                "Approved by the finance stewards"))),
                                new StructuredPropertyValueAssignment()
                                    .setPropertyUrn(INTERNAL_CODE)
                                    .setValues(
                                        new PrimitivePropertyValueArray(
                                            PrimitivePropertyValue.create("kestrel"))),
                                new StructuredPropertyValueAssignment()
                                    .setPropertyUrn(HIDDEN_LABEL)
                                    .setValues(
                                        new PrimitivePropertyValueArray(
                                            PrimitivePropertyValue.create("quail")))))),
                ORDERS_CHART,
                events(
                    ORDERS_CHART,
                    new ChartInfo()
                        .setTitle("Orders by region")
                        .setDescription("Monthly orders")
                        .setLastModified(new ChangeAuditStamps()),
                    // tags is a URN field queried by default
                    new GlobalTags()
                        .setTags(
                            new TagAssociationArray(
                                new TagAssociation().setTag(new TagUrn("Confidential")))),
                    browsePaths("prod", "sales")),
                NIGHTLY_JOB,
                events(NIGHTLY_JOB),
                TITLE_MATCH,
                events(
                    TITLE_MATCH,
                    new DashboardInfo()
                        .setTitle("Quarterly revenue archive")
                        .setDescription("Revenue totals")
                        .setLastModified(new ChangeAuditStamps())),
                DESCRIPTION_MATCH,
                events(
                    DESCRIPTION_MATCH,
                    new DashboardInfo()
                        .setTitle("Revenue totals")
                        .setDescription("Quarterly revenue archive")
                        .setLastModified(new ChangeAuditStamps())),
                STG_DASHBOARD,
                events(
                    STG_DASHBOARD,
                    new DashboardInfo()
                        .setTitle("stg")
                        .setDescription("Load checks for batch2017")
                        .setLastModified(new ChangeAuditStamps())),
                STAGING_DASHBOARD,
                events(
                    STAGING_DASHBOARD,
                    new DashboardInfo()
                        .setTitle("Staging notes")
                        .setDescription("Load checks")
                        .setLastModified(new ChangeAuditStamps())),
                LIFETIME_DASHBOARD,
                events(
                    LIFETIME_DASHBOARD,
                    new DashboardInfo()
                        .setTitle("customerlifetimevalue")
                        .setDescription("Load checks")
                        .setLastModified(new ChangeAuditStamps())),
                CARGO_REPORT,
                events(
                    CARGO_REPORT,
                    new DashboardInfo()
                        .setTitle("Cargo2017 Report")
                        .setDescription("Harbor figures")
                        .setLastModified(new ChangeAuditStamps())),
                CARGO_OVERVIEW,
                events(
                    CARGO_OVERVIEW,
                    new DashboardInfo()
                        .setTitle("Cargo Overview")
                        .setDescription("Harbor figures")
                        .setLastModified(new ChangeAuditStamps())),
                MANIFEST,
                events(
                    MANIFEST,
                    new DashboardInfo()
                        .setTitle("Manifest20240101")
                        .setDescription("Harbor figures")
                        .setLastModified(new ChangeAuditStamps())),
                FREIGHT_BOARD,
                events(
                    FREIGHT_BOARD,
                    new DashboardInfo()
                        .setTitle("Freight Board")
                        .setDescription("Berth plan for cargo2018 vessels")
                        .setLastModified(new ChangeAuditStamps())),
                FLEET_OVERVIEW,
                events(
                    FLEET_OVERVIEW,
                    new DashboardInfo()
                        .setTitle("Fleet Overview")
                        .setDescription("Harbor figures")
                        .setLastModified(new ChangeAuditStamps())),
                FLEET_NOTES,
                events(
                    FLEET_NOTES,
                    new DashboardInfo()
                        .setTitle("Engineering notes")
                        .setDescription("Built on fleet_v2")
                        .setLastModified(new ChangeAuditStamps())),
                NON_LATIN,
                events(
                    NON_LATIN,
                    new DashboardInfo()
                        .setTitle("ग्राहक तालिका")
                        .setDescription("ข้อมูล ลูกค้า")
                        .setLastModified(new ChangeAuditStamps())),
                IDENTIFIERS,
                events(
                    IDENTIFIERS,
                    new DashboardInfo()
                        .setTitle("Pipeline keys")
                        .setDescription("Keyed by glue_id, db_id, pk_fk, dt_ts and s3_id")
                        .setLastModified(new ChangeAuditStamps())),
                IDENTIFIER_PART,
                events(
                    IDENTIFIER_PART,
                    new DashboardInfo()
                        .setTitle("Crawler overview")
                        .setDescription("Glue crawlers by account")
                        .setLastModified(new ChangeAuditStamps())),
                STEMMED_NAME,
                events(
                    STEMMED_NAME,
                    new DashboardInfo()
                        .setTitle("Marketing Overview")
                        .setDescription("Campaign spend")
                        .setLastModified(new ChangeAuditStamps()))),
            false);
    syncAfterWrite(getBulkProcessor());
  }

  private static CachingAspectRetriever definitionsWithActiveUsers(
      MockAspectRetriever definitions) {
    CachingAspectRetriever activeUsers =
        TestOperationContexts.emptyActiveUsersAspectRetriever(definitions::getEntityRegistry);
    return new CachingAspectRetriever() {
      @Nonnull
      @Override
      public Map<Urn, Map<String, Aspect>> getLatestAspectObjects(
          @Nonnull OperationFingerprint context, Set<Urn> urns, Set<String> aspectNames) {
        Map<Urn, Map<String, Aspect>> found =
            new HashMap<>(definitions.getLatestAspectObjects(context, urns, aspectNames));
        Set<Urn> missing = new HashSet<>();
        for (Urn urn : urns) {
          if (!found.containsKey(urn)) {
            missing.add(urn);
          }
        }
        if (!missing.isEmpty()) {
          found.putAll(activeUsers.getLatestAspectObjects(context, missing, aspectNames));
        }
        return found;
      }

      @Nonnull
      @Override
      public Map<Urn, Map<String, SystemAspect>> getLatestSystemAspects(
          @Nonnull OperationFingerprint context, Map<Urn, Set<String>> urnAspectNames) {
        return definitions.getLatestSystemAspects(context, urnAspectNames);
      }

      @Nonnull
      @Override
      public Map<Urn, Boolean> entityExists(@Nonnull OperationFingerprint context, Set<Urn> urns) {
        return definitions.entityExists(context, urns);
      }

      @Nonnull
      @Override
      public EntityRegistry getEntityRegistry() {
        return definitions.getEntityRegistry();
      }
    };
  }

  private static SearchRetriever structuredPropertySearchRetriever(List<Urn> propertyUrns) {
    return new SearchRetriever() {
      @Override
      public ScrollResult scroll(
          List<String> entities,
          Filter filters,
          String scrollId,
          Integer count,
          List<SortCriterion> sortCriteria,
          SearchFlags searchFlags) {
        SearchEntityArray results = new SearchEntityArray();
        if (scrollId == null) {
          propertyUrns.forEach(urn -> results.add(new SearchEntity().setEntity(urn)));
        }
        return new ScrollResult()
            .setEntities(results)
            .setNumEntities(results.size())
            .setPageSize(results.size());
      }
    };
  }

  @AfterClass(alwaysRun = true)
  public void deleteV3Indices() throws IOException {
    SearchTemplates.install(null);
    for (String index : createdIndices) {
      getSearchClient()
          .deleteIndex(
              OperationFingerprint.EMPTY, new DeleteIndexRequest(index), RequestOptions.DEFAULT);
    }
  }

  /**
   * The engine accepts the mapping and settings of every V3 entity index of the registry, not only
   * the ones the other tests seed, and stores them in a form that compares equal on the next
   * system-update, which would otherwise apply or reindex them again every time.
   */
  @Test
  public void testEngineAcceptsEveryRegistryIndex() throws IOException {
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    int created = 0;
    for (MappingsBuilder.IndexMapping mapping :
        engineV3MappingsBuilder.getIndexMappings(opContext)) {
      if (!indexConvention.isV3EntityIndexType(mapping.getIndexName())) {
        continue;
      }
      String index = "accepted_" + mapping.getIndexName();
      Map<String, Object> settings =
          settingsBuilder.getSettings(indexConfiguration, mapping.getIndexName());
      try {
        indexBuilder.buildIndex(
            opContext,
            indexBuilder.buildReindexState(opContext, index, mapping.getMappings(), settings));
        ReindexConfig secondPass =
            indexBuilder.buildReindexState(opContext, index, mapping.getMappings(), settings);
        assertFalse(
            secondPass.requiresApplyMappings(),
            index
                + " mapping changed in the engine: "
                + differingPaths(secondPass.currentMappings(), secondPass.targetMappings()));
        assertFalse(
            secondPass.requiresApplySettings(),
            index
                + " settings changed in the engine: "
                + differingSettings(secondPass.currentSettings(), secondPass.targetSettings()));
      } finally {
        getSearchClient()
            .deleteIndex(
                OperationFingerprint.EMPTY, new DeleteIndexRequest(index), RequestOptions.DEFAULT);
      }
      created++;
    }
    assertTrue(created > 20, "Only " + created + " V3 indices");
  }

  /** Field paths where two mappings differ, compared the way system-update compares them. */
  private static List<String> differingPaths(
      Map<String, Object> current, Map<String, Object> target) {
    List<String> paths = new ArrayList<>();
    collectDifferences(current.get("properties"), target.get("properties"), "", paths);
    return paths.size() > 8 ? paths.subList(0, 8) : paths;
  }

  /** Index settings the target sets that the engine reports differently. */
  private static List<String> differingSettings(
      org.opensearch.common.settings.Settings current, Map<String, Object> target) {
    List<String> paths = new ArrayList<>();
    collectSettingDifferences(current, target, "", paths);
    return paths.size() > 8 ? paths.subList(0, 8) : paths;
  }

  private static void collectSettingDifferences(
      org.opensearch.common.settings.Settings current,
      Object target,
      String key,
      List<String> paths) {
    if (target instanceof Map<?, ?> targetMap) {
      for (Map.Entry<?, ?> entry : targetMap.entrySet()) {
        collectSettingDifferences(
            current,
            entry.getValue(),
            key.isEmpty() ? "" + entry.getKey() : key + "." + entry.getKey(),
            paths);
      }
      return;
    }
    String settingKey = key.startsWith("index.") ? key : "index." + key;
    String actual =
        target instanceof List<?>
            ? String.valueOf(current.getAsList(settingKey))
            : current.get(settingKey);
    if (!java.util.Objects.equals(actual, String.valueOf(target))) {
      paths.add(settingKey + ": " + actual + " != " + target);
    }
  }

  private static void collectDifferences(
      Object current, Object target, String path, List<String> paths) {
    if (current instanceof Map<?, ?> currentMap && target instanceof Map<?, ?> targetMap) {
      java.util.Set<String> keys = new java.util.TreeSet<>();
      currentMap.keySet().forEach(key -> keys.add(String.valueOf(key)));
      targetMap.keySet().forEach(key -> keys.add(String.valueOf(key)));
      for (String key : keys) {
        Object currentValue = currentMap.get(key);
        Object targetValue = targetMap.get(key);
        // The engine reports type object on mapped objects that the generated mapping leaves
        // implicit
        if ("type".equals(key)
            && "object".equals(String.valueOf(currentValue == null ? targetValue : currentValue))
            && (currentValue == null || targetValue == null)) {
          continue;
        }
        collectDifferences(currentValue, targetValue, path + "/" + key, paths);
      }
      return;
    }
    if (!java.util.Objects.equals(String.valueOf(current), String.valueOf(target))) {
      paths.add(path + ": " + current + " != " + target);
    }
  }

  @Test
  public void testV2EntityIndexExistsOnlyWhenEnabled() throws IOException {
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    for (String entityType : ENTITY_TYPES) {
      String v2Index = indexConvention.getEntityIndexName(opContext, entityType);
      assertEquals(indexExists(v2Index), isV2Enabled(), v2Index);
    }
    for (String index : createdIndices) {
      assertTrue(indexExists(index), index);
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
    // Callers that name the V2 .keyword subfield read the same subfield
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
  public void testStructuredPropertyFilter() {
    String field = "structuredProperties." + RETENTION_POLICY.getId();
    assertUrns(
        searchService
            .filter(opContext, DATASET_ENTITY_NAME, QueryUtils.newFilter(field, "90d"), null, 0, 10)
            .getEntities(),
        ORDERS);
    assertEquals(
        searchService.aggregateByValue(
            aggregateOperation(), List.of(DATASET_ENTITY_NAME), field, null, 10),
        Map.of("90d", 1L));
  }

  /**
   * Full-text search matches structured properties whose configuration includes them in the default
   * query. A configured property with queryByDefault false stays filterable, a property with no
   * configuration is not analyzed, and excludeFromFullTextSearch does not keep a configured
   * property out of V3 search.
   */
  @Test
  public void testSearchMatchesStructuredPropertyValues() {
    OperationContext fulltext = opContext.withSearchFlags(flags -> flags.setFulltext(true));
    assertUrns(
        searchService
            .search(fulltext, List.of(DATASET_ENTITY_NAME), "90d", null, null, 0, 10)
            .getEntities(),
        ORDERS);
    assertUrns(
        searchService
            .search(fulltext, List.of(DATASET_ENTITY_NAME), "stewards", null, null, 0, 10)
            .getEntities(),
        CUSTOMERS);
    assertUrns(
        searchService
            .search(fulltext, List.of(DATASET_ENTITY_NAME), "nightjar", null, null, 0, 10)
            .getEntities(),
        ORDERS);
    assertUrns(
        searchService
            .search(fulltext, List.of(DATASET_ENTITY_NAME), "kestrel", null, null, 0, 10)
            .getEntities());
    assertUrns(
        searchService
            .search(fulltext, List.of(DATASET_ENTITY_NAME), "quail", null, null, 0, 10)
            .getEntities());
    assertUrns(
        searchService
            .filter(
                opContext,
                DATASET_ENTITY_NAME,
                QueryUtils.newFilter("structuredProperties." + INTERNAL_CODE.getId(), "kestrel"),
                null,
                0,
                10)
            .getEntities(),
        CUSTOMERS);
    assertUrns(
        searchService
            .filter(
                opContext,
                DATASET_ENTITY_NAME,
                QueryUtils.newFilter("structuredProperties." + HIDDEN_LABEL.getId(), "quail"),
                null,
                0,
                10)
            .getEntities(),
        CUSTOMERS);
  }

  @Test
  public void testLineageUrnFilter() {
    // The filter LineageSearchService sends for a batch of related entities
    DataHubAppConfiguration appConfig = new DataHubAppConfiguration();
    appConfig.setMetadataChangeProposal(new MetadataChangeProposalConfig());
    appConfig
        .getMetadataChangeProposal()
        .setSideEffects(new MetadataChangeProposalConfig.SideEffectsConfig());
    appConfig
        .getMetadataChangeProposal()
        .getSideEffects()
        .setSchemaField(new MetadataChangeProposalConfig.SchemaFieldSideEffectsConfig());
    OperationContext fulltext = opContext.withSearchFlags(flags -> flags.setFulltext(true));

    Filter lineageFilter =
        QueryUtils.buildFilterWithUrns(appConfig, Set.of(ORDERS, ORDERS_CHART), null);
    assertUrns(
        searchService.search(fulltext, ENTITY_TYPES, "*", lineageFilter, null, 0, 10).getEntities(),
        ORDERS,
        ORDERS_CHART);

    // A facet filter picked on the lineage tab is combined with the URN criterion
    Filter facetedLineageFilter =
        QueryUtils.buildFilterWithUrns(
            appConfig,
            Set.of(ORDERS, CUSTOMERS),
            QueryUtils.newFilter("platform", HIVE.toString()));
    assertUrns(
        searchService
            .search(fulltext, List.of(DATASET_ENTITY_NAME), "*", facetedLineageFilter, null, 0, 10)
            .getEntities(),
        ORDERS);
  }

  @Test
  public void testAggregateByValue() {
    assertEquals(
        searchService.aggregateByValue(
            aggregateOperation(), List.of(DATASET_ENTITY_NAME), "platform", null, 10),
        Map.of(HIVE.toString(), 1L, POSTGRES.toString(), 1L));
    // The raw value, not the lower-cased root keyword
    assertEquals(
        searchService.aggregateByValue(
            aggregateOperation(), List.of(DATASET_ENTITY_NAME), "typeNames", null, 10),
        Map.of("Table", 1L));
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
  @SuppressWarnings("unchecked")
  public void testRawEntity() {
    Map<Urn, Map<String, Object>> raw = searchService.raw(opContext, Set.of(ORDERS, ORDERS_CHART));
    assertEquals(raw.keySet(), Set.of(ORDERS, ORDERS_CHART));
    // _entityType is only stored on V3 documents
    assertEquals(raw.get(ORDERS).get("_entityType"), DATASET_ENTITY_NAME);
    assertEquals(raw.get(ORDERS_CHART).get("_entityType"), CHART_ENTITY_NAME);
    // Each aspect's fields sit at the root, as on V2, and under _aspects.<aspect>
    assertEquals(raw.get(ORDERS).get("name"), "orders");
    Map<String, Object> aspects = (Map<String, Object>) raw.get(ORDERS).get("_aspects");
    assertEquals(((Map<String, Object>) aspects.get("datasetProperties")).get("name"), "orders");
    assertEquals(raw.get(ORDERS_CHART).get("title"), "Orders by region");
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
    assertShippedClasspathTemplate(
        fulltext, "orders", "urn:li:searchTemplate:searchLight", "urn:li:searchTemplate:light");
    assertUrns(
        searchService
            .search(fulltext, List.of(DATASET_ENTITY_NAME), "orders", null, null, 0, 10)
            .getEntities(),
        ORDERS);
    assertUrns(
        searchService
            .search(fulltext, List.of(DATASET_ENTITY_NAME), "ORDERS", null, null, 0, 10)
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
            List.of("platform", "_entityType", "typeNames"));
    assertUrns(acrossEntities.getEntities(), ORDERS, ORDERS_CHART);
    Map<String, Map<String, Long>> facets =
        acrossEntities.getMetadata().getAggregations().stream()
            .collect(
                Collectors.toMap(
                    AggregationMetadata::getName, AggregationMetadata::getAggregations));
    assertEquals(facets.get("platform"), Map.of(HIVE.toString(), 1L));
    assertEquals(facets.get("typeNames"), Map.of("Table", 1L));
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

    // A quoted query runs no simple query, so only the exact and prefix matches on the identity
    // fields (name, title, qualified name, id and urn) remain
    assertUrns(
        searchService
            .search(fulltext, ENTITY_TYPES, "\"orders by region\"", null, null, 0, 10)
            .getEntities(),
        ORDERS_CHART);

    // _entityName sorts ignoring case, as on V2, so "orders" comes before "Orders by region"
    // ascending. The urn tie-break alone would put the chart first
    for (Map.Entry<SortOrder, List<Urn>> sort :
        Map.of(
                SortOrder.ASCENDING, List.of(ORDERS, ORDERS_CHART),
                SortOrder.DESCENDING, List.of(ORDERS_CHART, ORDERS))
            .entrySet()) {
      assertEquals(
          searchService
              .search(
                  fulltext,
                  ENTITY_TYPES,
                  "orders",
                  null,
                  List.of(new SortCriterion().setField("_entityName").setOrder(sort.getKey())),
                  0,
                  10)
              .getEntities()
              .stream()
              .map(SearchEntity::getEntity)
              .collect(Collectors.toList()),
          sort.getValue(),
          sort.getKey().toString());
    }
  }

  /** Full-text hits report the source field they matched on ("Matched on" in the UI). */
  @Test
  public void testSearchReportsMatchedFields() {
    SearchEntityArray hits =
        searchService
            .search(
                opContext.withSearchFlags(flags -> flags.setFulltext(true)),
                List.of(DATASET_ENTITY_NAME),
                "orders",
                null,
                null,
                0,
                10)
            .getEntities();
    assertUrns(hits, ORDERS);
    List<String> matchedFields =
        hits.get(0).getMatchedFields().stream()
            .map(MatchedField::getName)
            .collect(Collectors.toList());
    assertTrue(matchedFields.contains("name"), matchedFields.toString());
  }

  /**
   * Long queries over indices rich in similar words stay under OpenSearch's default limit of 1024
   * clauses: a query keeps the words whose clauses fit, and its fuzzy terms share what is left.
   */
  @Test
  public void testLongQueryStaysUnderClauseLimit() throws Exception {
    assertLongQueriesStayUnderClauseLimit(false);
  }

  @Test
  public void testLongScrollQueryStaysUnderClauseLimit() throws Exception {
    assertLongQueriesStayUnderClauseLimit(true);
  }

  private void assertLongQueriesStayUnderClauseLimit(boolean scroll) throws Exception {
    OperationContext fulltext =
        (scroll ? scrollOperation() : pagedSearch())
            .withSearchFlags(flags -> flags.setFulltext(true));
    UpdateIndicesV3Strategy indexer = longQueryIndexer();
    try {
      indexLongQueryVocabulary(indexer);
      // A failed shard only drops that index's results, so each scope checks its own entity
      for (Map.Entry<List<String>, Urn> scope :
          Map.of(
                  List.of(DATASET_ENTITY_NAME),
                  LONG_QUERY_DATASET,
                  List.of(DASHBOARD_ENTITY_NAME),
                  LONG_QUERY_DASHBOARD,
                  ENTITY_TYPES,
                  LONG_QUERY_DATASET)
              .entrySet()) {
        List<String> entityTypes = scope.getKey();
        for (String query : longQueries()) {
          // A clause overflow fails the search. Recall and precision for representative shapes
          // are asserted separately.
          try {
            SearchEntityArray entities;
            if (scroll) {
              entities =
                  searchService
                      .fullTextScroll(
                          fulltext, entityTypes, query, null, null, null, null, 10, List.of())
                      .getEntities();
            } else {
              entities =
                  searchService
                      .search(fulltext, entityTypes, query, null, null, 0, 10)
                      .getEntities();
            }
            assertNotNull(entities, (scroll ? "scroll " : "") + entityTypes + ": " + query);
          } catch (RuntimeException e) {
            throw new AssertionError(
                (scroll ? "scroll " : "") + entityTypes + " query=[" + query + "]", e);
          }
        }
      }
    } finally {
      deleteLongQueryVocabulary(indexer);
    }
  }

  /**
   * Representative shapes must find the vocabulary entity and nothing else. The clause-limit tests
   * only check that the wider matrix does not overflow.
   */
  @Test
  public void testRepresentativeLongQueryRecall() throws Exception {
    UpdateIndicesV3Strategy indexer = longQueryIndexer();
    try {
      indexLongQueryVocabulary(indexer);
      for (boolean scroll : List.of(false, true)) {
        OperationContext fulltext =
            (scroll ? scrollOperation() : pagedSearch())
                .withSearchFlags(flags -> flags.setFulltext(true));
        for (Map.Entry<String, String> query : representativeLongQueries().entrySet()) {
          assertLongQueryRecall(
              fulltext,
              List.of(DATASET_ENTITY_NAME),
              query.getValue(),
              LONG_QUERY_DATASET,
              scroll,
              query.getKey());
          assertLongQueryRecall(
              fulltext,
              List.of(DASHBOARD_ENTITY_NAME),
              query.getValue(),
              LONG_QUERY_DASHBOARD,
              scroll,
              query.getKey());
        }
      }
    } finally {
      deleteLongQueryVocabulary(indexer);
    }
  }

  private UpdateIndicesV3Strategy longQueryIndexer() {
    return UpdateIndicesV3Strategy.builder()
        .v3Config(config.getEntityIndex().getV3())
        .elasticSearchService(searchService)
        .searchDocumentTransformer(
            new SearchDocumentTransformer(1000, 1000, 1000, false, ESUtils.KEYWORD_MAXLENGTH))
        .timeseriesAspectService(mock(TimeseriesAspectService.class))
        .build();
  }

  private void indexLongQueryVocabulary(UpdateIndicesV3Strategy indexer)
      throws InterruptedException, IOException {
    String text = longQueryVocabulary();
    indexer.processBatch(
        opContext,
        Map.of(
            LONG_QUERY_DATASET,
            events(
                LONG_QUERY_DATASET,
                new DatasetProperties().setName(text).setDescription(text).setQualifiedName(text)),
            LONG_QUERY_DASHBOARD,
            events(
                LONG_QUERY_DASHBOARD,
                new DashboardInfo()
                    .setTitle(text)
                    .setDescription(text)
                    .setLastModified(new ChangeAuditStamps()))),
        false);
    syncAfterWrite(getBulkProcessor());
  }

  private void deleteLongQueryVocabulary(UpdateIndicesV3Strategy indexer)
      throws InterruptedException, IOException {
    // Other tests count the datasets and dashboards.
    indexer.processBatch(
        opContext,
        Map.of(
            LONG_QUERY_DATASET,
            keyDeletion(LONG_QUERY_DATASET),
            LONG_QUERY_DASHBOARD,
            keyDeletion(LONG_QUERY_DASHBOARD)),
        false);
    syncAfterWrite(getBulkProcessor());
  }

  private static String longQueryVocabulary() {
    // Close neighbours of every word give each fuzzy term its full expansions.
    StringBuilder vocabulary = new StringBuilder();
    for (String word : LONG_QUERY_WORDS) {
      vocabulary.append(word).append(' ');
      for (char last : "bcdfghjkmnp".toCharArray()) {
        vocabulary.append(word, 0, word.length() - 1).append(last).append(' ');
      }
    }
    return vocabulary.toString().trim();
  }

  private static List<String> longQueries() {
    String[] words = LONG_QUERY_WORDS.toArray(String[]::new);
    return List.of(
        String.join(" ", Arrays.copyOf(words, 3)),
        String.join(" ", Arrays.copyOf(words, 20)),
        Arrays.stream(words).limit(12).map(word -> word + "2017").collect(Collectors.joining(" ")),
        Arrays.stream(words)
            .limit(8)
            .map(word -> word + "-" + word + "-v2")
            .collect(Collectors.joining(" ")),
        words[0] + "'s " + String.join(" ", Arrays.copyOfRange(words, 1, words.length)),
        words[0] + "2017 " + String.join(" ", Arrays.copyOfRange(words, 1, 20)),
        Arrays.stream(words).limit(12).map(word -> "db." + word).collect(Collectors.joining(" ")),
        Arrays.stream(words)
            .limit(8)
            .map(word -> "prod." + word + "." + word)
            .collect(Collectors.joining(" ")),
        String.join(".", words),
        Arrays.stream(words).limit(15).map(word -> word + "2017").collect(Collectors.joining()));
  }

  private static Map<String, String> representativeLongQueries() {
    List<String> queries = longQueries();
    return Map.of(
        "short", queries.get(0),
        "long", queries.get(1),
        "alphanumeric", queries.get(2),
        "dotted", queries.get(6),
        "joined identifier", queries.get(8));
  }

  private void assertLongQueryRecall(
      OperationContext fulltext,
      List<String> entityTypes,
      String query,
      Urn expected,
      boolean scroll,
      String shape) {
    SearchEntityArray entities =
        scroll
            ? searchService
                .fullTextScroll(fulltext, entityTypes, query, null, null, null, null, 10, List.of())
                .getEntities()
            : searchService.search(fulltext, entityTypes, query, null, null, 0, 10).getEntities();
    // The vocabulary words occur on one entity. It must be the only hit, and the first hit.
    assertEquals(
        entities.stream().map(SearchEntity::getEntity).collect(Collectors.toList()),
        List.of(expected),
        (scroll ? "scroll " : "search ") + shape + " " + entityTypes + ": " + query);
  }

  /** Every field queried by default is searched. */
  @Test
  public void testEveryDefaultQueryFieldIsSearched() {
    assertUrns(
        searchService
            .search(
                opContext.withSearchFlags(flags -> flags.setFulltext(true)),
                List.of(DATASET_ENTITY_NAME),
                "ledger",
                null,
                null,
                0,
                10)
            .getEntities(),
        CUSTOMERS);
  }

  /**
   * A tag URN contributes its id to full text and stays filterable by the complete URN. URN
   * structure does not match.
   */
  @Test
  public void testExactUrnLabelDoesNotMatchComponent() {
    assertUrns(
        searchService
            .search(
                opContext.withSearchFlags(flags -> flags.setFulltext(true)),
                List.of(CHART_ENTITY_NAME),
                "confidential",
                null,
                null,
                0,
                10)
            .getEntities(),
        ORDERS_CHART);
    assertUrns(
        searchService
            .search(
                opContext.withSearchFlags(flags -> flags.setFulltext(true)),
                List.of(CHART_ENTITY_NAME),
                "dataPlatform",
                null,
                null,
                0,
                10)
            .getEntities());
    assertUrns(
        searchService
            .filter(
                opContext,
                CHART_ENTITY_NAME,
                QueryUtils.newFilter("tags", "urn:li:tag:Confidential"),
                null,
                0,
                10)
            .getEntities(),
        ORDERS_CHART);
  }

  /**
   * Dataset full text uses the dataset name. The platform field contributes the platform name. The
   * fabric token does not match. {@code dataPlatform} expands to the synonym {@code data platform},
   * and every word is required, so {@code data} in the customers description does not match.
   */
  @Test
  public void testDatasetNameMatchesWithoutPlatformOrFabric() {
    assertUrns(
        searchService
            .search(
                opContext.withSearchFlags(flags -> flags.setFulltext(true)),
                List.of(DATASET_ENTITY_NAME),
                "sales.orders",
                null,
                null,
                0,
                10)
            .getEntities(),
        ORDERS);
    assertUrns(
        searchService
            .search(
                opContext.withSearchFlags(flags -> flags.setFulltext(true)),
                List.of(DATASET_ENTITY_NAME),
                "hive",
                null,
                null,
                0,
                10)
            .getEntities(),
        ORDERS);
    assertUrns(
        searchService
            .search(
                opContext.withSearchFlags(flags -> flags.setFulltext(true)),
                List.of(DATASET_ENTITY_NAME),
                "PROD",
                null,
                null,
                0,
                10)
            .getEntities());
    assertUrns(
        searchService
            .search(
                opContext.withSearchFlags(flags -> flags.setFulltext(true)),
                List.of(DATASET_ENTITY_NAME),
                "dataPlatform",
                null,
                null,
                0,
                10)
            .getEntities());
  }

  /** A queryByDefault: false field is filterable but a plain full-text query skips it. */
  @Test
  public void testSearchSkipsFieldsNotQueriedByDefault() {
    assertEquals(
        searchService
            .search(
                opContext.withSearchFlags(flags -> flags.setFulltext(true)),
                List.of(DATASET_ENTITY_NAME),
                CUSTOMERS_OWNER.getId(),
                null,
                null,
                0,
                10)
            .getNumEntities()
            .intValue(),
        0);
    assertUrns(
        searchService
            .filter(
                opContext,
                DATASET_ENTITY_NAME,
                QueryUtils.newFilter("owners", CUSTOMERS_OWNER.toString()),
                null,
                0,
                10)
            .getEntities(),
        CUSTOMERS);
  }

  @Test
  public void testSearchDropsStopWords() {
    // The title reads "Orders by region": English stop words are dropped at index and query time
    assertUrns(
        searchService
            .search(
                opContext.withSearchFlags(flags -> flags.setFulltext(true)),
                List.of(CHART_ENTITY_NAME),
                "orders of region",
                null,
                null,
                0,
                10)
            .getEntities(),
        ORDERS_CHART);
  }

  /** A structured query on a normalized root field matches ignoring case. */
  @Test
  public void testStructuredQueryIgnoresCase() {
    OperationContext fulltext = opContext.withSearchFlags(flags -> flags.setFulltext(true));
    assertShippedClasspathTemplate(
        fulltext,
        STRUCTURED_QUERY_PREFIX + "name:ORDERS",
        "urn:li:searchTemplate:searchLight",
        "urn:li:searchTemplate:structured");
    assertUrns(
        searchService
            .search(
                fulltext,
                List.of(DATASET_ENTITY_NAME),
                STRUCTURED_QUERY_PREFIX + "name:ORDERS",
                null,
                null,
                0,
                10)
            .getEntities(),
        ORDERS);
  }

  /**
   * A quoted query uses the exact template. Four or more tokens also match a description that
   * contains every term, so quoting this description matches the customers dataset.
   */
  @Test
  public void testQuotedDescriptionPhrase() {
    ExplainResponse explain =
        searchService.explain(
            opContext.withSearchFlags(flags -> flags.setFulltext(true)),
            "\"" + CUSTOMERS_DESCRIPTION + "\"",
            CUSTOMERS.toString(),
            DATASET_ENTITY_NAME,
            null,
            null,
            null,
            null,
            10,
            List.of());
    assertTrue(explain.isMatch());
  }

  /**
   * A match in the entity name ranks above the same words in the description. V3 does not apply
   * the @Searchable boostScore; the shared fields' weights rank the match instead.
   */
  @Test
  public void testSearchableBoostsRankResults() {
    // Multi-word, so the light query searches every field, not only the names
    assertEquals(
        searchService
            .search(
                opContext.withSearchFlags(flags -> flags.setFulltext(true)),
                List.of(DASHBOARD_ENTITY_NAME),
                "quarterly archive",
                null,
                null,
                0,
                10)
            .getEntities()
            .stream()
            .map(SearchEntity::getEntity)
            .collect(Collectors.toList()),
        List.of(TITLE_MATCH, DESCRIPTION_MATCH));
  }

  /**
   * The Stage 1 query puts the intended entity first for each kind of query it adds recall or
   * ranking for. V2's all-terms query finds none of the typo, synonym or substring entities.
   */
  @Test
  public void testStage1TopHits() {
    // Exact name: the dataset named "orders" outranks the chart whose title starts with it
    assertTopHit(ENTITY_TYPES, "orders", ORDERS);
    // Fully qualified name, here the dataset key id
    assertTopHit(ENTITY_TYPES, "sales.orders", ORDERS);
    assertTopHit(ENTITY_TYPES, "SALES.ORDERS", ORDERS);
    assertTopHit(ENTITY_TYPES, ORDERS.toString(), ORDERS);
    // The identity template folds the URN, so a differently cased URN still finds the dataset.
    // A truncated URN does not.
    assertTopHit(ENTITY_TYPES, ORDERS.toString().replace("sales.orders", "SALES.ORDERS"), ORDERS);
    assertTrue(
        searchService
            .search(
                opContext.withSearchFlags(flags -> flags.setFulltext(true)),
                ENTITY_TYPES,
                "urn:li:dataset:(urn:li:dataPlatform:hive,sales",
                null,
                null,
                0,
                10)
            .getEntities()
            .isEmpty());
    // Search operators are plain text: "-archive" does not exclude the title holding it
    assertTrue(
        searchService
            .search(
                opContext.withSearchFlags(flags -> flags.setFulltext(true)),
                List.of(DASHBOARD_ENTITY_NAME),
                "revenue -archive",
                null,
                null,
                0,
                10)
            .getEntities()
            .stream()
            .anyMatch(entity -> entity.getEntity().equals(TITLE_MATCH)));
    // A letter and digit run held only in a description, indexed as one token
    assertTopHit(List.of(DASHBOARD_ENTITY_NAME), "batch2017", STG_DASHBOARD);
    // A long description pasted as the query
    assertTopHit(List.of(DATASET_ENTITY_NAME), CUSTOMERS_DESCRIPTION, CUSTOMERS);
    // One edit away from "customers"
    assertTopHit(List.of(DATASET_ENTITY_NAME), "custmers", CUSTOMERS);
    // "staging" expands to its synonym "stg", an exact title, ahead of the title holding "staging"
    assertTopHit(List.of(DASHBOARD_ENTITY_NAME), "staging", STG_DASHBOARD);
    assertTopHit(List.of(DASHBOARD_ENTITY_NAME), "lifetime", LIFETIME_DASHBOARD);
  }

  private void assertTopHit(List<String> entityTypes, String query, Urn expected) {
    SearchEntityArray hits =
        searchService
            .search(
                opContext.withSearchFlags(flags -> flags.setFulltext(true)),
                entityTypes,
                query,
                null,
                null,
                0,
                10)
            .getEntities();
    assertFalse(hits.isEmpty(), query);
    assertEquals(hits.get(0).getEntity(), expected, query + ": " + hits);
  }

  /**
   * The light query runs first and the full query only when it matches nothing. A single word
   * searches the names first, so a word that a dashboard title holds hides the dashboard holding it
   * only in its description, in the hits, the total and explain alike.
   */
  @Test
  public void testLightFirstRelaxation() {
    OperationContext fulltext = opContext.withSearchFlags(flags -> flags.setFulltext(true));
    BiFunction<String, String, SearchResult> search =
        (entityType, query) ->
            searchService.search(fulltext, List.of(entityType), query, null, null, 0, 10);

    SearchResult nameMatch = search.apply(DASHBOARD_ENTITY_NAME, "archive");
    assertUrns(nameMatch.getEntities(), TITLE_MATCH);
    assertEquals(nameMatch.getNumEntities().intValue(), 1);
    assertTrue(explain("archive", TITLE_MATCH).isMatch());
    assertFalse(explain("archive", DESCRIPTION_MATCH).isMatch());
    // The explain API's default sort by score is relevance too
    assertFalse(
        searchService
            .explain(
                fulltext,
                "archive",
                DESCRIPTION_MATCH.toString(),
                DASHBOARD_ENTITY_NAME,
                null,
                List.of(new SortCriterion().setField("_score").setOrder(SortOrder.DESCENDING)),
                null,
                null,
                10,
                List.of())
            .isMatch());
    // A search that reaches the DAO without hits runs the full query, as in DataHub Cloud, and so
    // does its explain
    assertTrue(
        searchService
            .explain(
                fulltext,
                "archive",
                DESCRIPTION_MATCH.toString(),
                DASHBOARD_ENTITY_NAME,
                null,
                null,
                null,
                null,
                0,
                List.of())
            .isMatch());
    assertEquals(
        searchService
            .search(fulltext, List.of(DASHBOARD_ENTITY_NAME), "archive", null, null, 0, 0)
            .getNumEntities()
            .intValue(),
        2);
    // A filter that leaves the light query nothing falls through to the full query
    assertUrns(
        searchService
            .search(
                fulltext,
                List.of(DASHBOARD_ENTITY_NAME),
                "archive",
                QueryUtils.newFilter("urn", DESCRIPTION_MATCH.toString()),
                null,
                0,
                10)
            .getEntities(),
        DESCRIPTION_MATCH);
    // The total decides, so a later page of a light result stays on the light query
    SearchResult secondPage =
        searchService.search(
            fulltext, List.of(DASHBOARD_ENTITY_NAME), "archive", null, null, 1, 10);
    assertEquals(secondPage.getNumEntities().intValue(), 1);
    assertTrue(secondPage.getEntities().isEmpty());
    // A non-relevance sort skips light and runs fulltext.
    assertUrns(
        searchService
            .search(
                fulltext,
                List.of(DASHBOARD_ENTITY_NAME),
                "archive",
                null,
                List.of(new SortCriterion().setField("_score").setOrder(SortOrder.ASCENDING)),
                0,
                10)
            .getEntities(),
        TITLE_MATCH,
        DESCRIPTION_MATCH);
    assertUrns(
        searchService
            .search(
                fulltext,
                List.of(DASHBOARD_ENTITY_NAME),
                "archive",
                null,
                List.of(new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING)),
                0,
                10)
            .getEntities(),
        TITLE_MATCH,
        DESCRIPTION_MATCH);

    // A typo matches nothing on the light query, so the full fuzzy query runs. Fuzzy matching
    // reaches the stemmed token ("archiv"); 7 characters allow two edits
    assertUrns(
        search.apply(DASHBOARD_ENTITY_NAME, "archiev").getEntities(),
        TITLE_MATCH,
        DESCRIPTION_MATCH);
    assertTrue(explain("archiev", DESCRIPTION_MATCH).isMatch());

    // A typo joined to a short suffix uses the shared Java/template fuzzy derivation, which splits
    // identifiers before applying edit distance. A long digit run still does not match.
    assertEquals(
        search.apply(DATASET_ENTITY_NAME, "orderz_20240101").getNumEntities().intValue(), 0);
    assertEquals(
        search.apply(DATASET_ENTITY_NAME, "orderz_aa_bb_cc").getNumEntities().intValue(), 0);
    assertFalse(explain("orderz_20240101", ORDERS).isMatch());
    assertUrns(search.apply(DATASET_ENTITY_NAME, "orderz_aa").getEntities(), ORDERS);
    assertTrue(explain("orderz_aa", ORDERS).isMatch());

    // A letter and digit run matches whole or by all its parts on the light query. "Cargo
    // Overview" holds only "cargo", so it neither hides "Cargo2017 Report" nor keeps the full query
    // from finding "cargo2018" in a description. A 6-digit run held in a title is found, not
    // stopped
    assertUrns(search.apply(DASHBOARD_ENTITY_NAME, "cargo2017").getEntities(), CARGO_REPORT);
    SearchEntityArray splitRun = search.apply(DASHBOARD_ENTITY_NAME, "cargo2018").getEntities();
    assertTrue(
        splitRun.stream().anyMatch(entity -> entity.getEntity().equals(FREIGHT_BOARD)),
        splitRun.toString());
    assertUrns(search.apply(DASHBOARD_ENTITY_NAME, "manifest20240101").getEntities(), MANIFEST);
    // A word that also holds "_" matches a name holding one of its parts, as in DataHub Cloud: the
    // analyzers emit those parts at one position, so "Fleet Overview" hides the description holder
    assertUrns(search.apply(DASHBOARD_ENTITY_NAME, "fleet_v2").getEntities(), FLEET_OVERVIEW);
    // A search with includeExplain explains the light query that served it
    SearchEntityArray explained =
        searchService
            .search(
                fulltext.withSearchFlags(flags -> flags.setIncludeExplain(true)),
                List.of(DASHBOARD_ENTITY_NAME),
                "archive",
                null,
                null,
                0,
                10)
            .getEntities();
    assertUrns(explained, TITLE_MATCH);
    assertTrue(explained.get(0).getExtraFields().containsKey("_explain"));
  }

  /**
   * Scroll starts on the complete search router. An ordinary word uses fulltext, so a description
   * match is returned and keepAlive explain describes that query.
   */
  @Test
  public void testScrollUsesCompleteFulltext() {
    OperationContext fulltext = scrollOperation().withSearchFlags(flags -> flags.setFulltext(true));
    assertShippedClasspathTemplate(fulltext, "archive", true, SEARCH_TEMPLATE, FULLTEXT_TEMPLATE);
    assertUrns(
        searchService
            .fullTextScroll(
                fulltext,
                List.of(DASHBOARD_ENTITY_NAME),
                "archive",
                null,
                null,
                null,
                null,
                10,
                List.of())
            .getEntities(),
        TITLE_MATCH,
        DESCRIPTION_MATCH);
    assertTrue(
        searchService
            .explain(
                fulltext,
                "archive",
                DESCRIPTION_MATCH.toString(),
                DASHBOARD_ENTITY_NAME,
                null,
                null,
                null,
                "5m",
                10,
                List.of())
            .isMatch());
  }

  private ExplainResponse explain(String query, Urn urn) {
    return searchService.explain(
        opContext.withSearchFlags(flags -> flags.setFulltext(true)),
        query,
        urn.toString(),
        urn.getEntityType(),
        null,
        null,
        null,
        null,
        10,
        List.of());
  }

  /** One word of an Indic or Thai name or description finds it: combining marks stay in words. */
  @Test
  public void testWordsWithCombiningMarksAreSearchable() {
    for (String query : List.of("तालिका", "ग्राहक", "ลูกค้า")) {
      assertEquals(
          searchService
              .search(
                  opContext.withSearchFlags(flags -> flags.setFulltext(true)),
                  List.of(DASHBOARD_ENTITY_NAME),
                  query,
                  null,
                  null,
                  0,
                  10)
              .getEntities()
              .stream()
              .map(SearchEntity::getEntity)
              .collect(Collectors.toList()),
          List.of(NON_LATIN),
          query);
    }
  }

  /**
   * A snake_case identifier is indexed whole as well as by its parts. The light query misses a word
   * held only in a description and continues to fulltext. The shared fuzzy derivation splits the
   * identifier, so a description holding one sufficiently long part can also match.
   */
  @Test
  public void testSnakeCaseIdentifiersAreSearchableWhole() {
    assertEqualsNoOrder(
        searchDashboards("glue_id").toArray(), new Object[] {IDENTIFIERS, IDENTIFIER_PART});
    for (String query : List.of("db_id", "pk_fk", "dt_ts", "s3_id")) {
      assertEquals(searchDashboards(query), List.of(IDENTIFIERS), query);
    }
  }

  /** Another form of a word in an entity's name finds it, as V2's stemmed name subfield does. */
  @Test
  public void testEntityNamesAreStemmed() {
    assertEquals(searchDashboards("markets"), List.of(STEMMED_NAME));
  }

  private List<Urn> searchDashboards(String query) {
    return searchService
        .search(
            opContext.withSearchFlags(flags -> flags.setFulltext(true)),
            List.of(DASHBOARD_ENTITY_NAME),
            query,
            null,
            null,
            0,
            10)
        .getEntities()
        .stream()
        .map(SearchEntity::getEntity)
        .collect(Collectors.toList());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSearchWithEmptyIndex() throws IOException {
    // An index without documents answers every query with no hits, and maps no search tier
    Map<String, Object> emptyProperties = getMappedProperties(EMPTY_ENTITY_TYPE);
    assertFalse(emptyProperties.containsKey("_search"));
    assertTrue(
        emptyProperties.keySet().stream().noneMatch(field -> field.startsWith("tier_")),
        emptyProperties.keySet().toString());

    OperationContext fulltext = opContext.withSearchFlags(flags -> flags.setFulltext(true));
    for (String query : List.of("orders", "\"orders\"")) {
      assertUrns(
          searchService
              .search(
                  fulltext,
                  List.of(EMPTY_ENTITY_TYPE, DATASET_ENTITY_NAME),
                  query,
                  null,
                  null,
                  0,
                  10)
              .getEntities(),
          ORDERS);
      assertEquals(
          searchService
              .search(fulltext, List.of(EMPTY_ENTITY_TYPE), query, null, null, 0, 10)
              .getNumEntities()
              .intValue(),
          0,
          query);
    }
    assertEquals(
        urns(searchService.autoComplete(opContext, EMPTY_ENTITY_TYPE, "ord", null, null, 10)),
        List.of());
  }

  /**
   * Root fields that are not shared labels are normalized keywords for filters, facets and sorts,
   * without V2's analyzed subfields. Full text lives on the root field its label names: {@code
   * text} and {@code stemmed} on each one, and a case-preserving {@code keyword} on indexed exact
   * roots. System metadata is the root {@code _system} object. Search V3 does not map an
   * autocomplete field.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testOnlySharedSearchFieldsAreAnalyzed() throws IOException {
    Map<String, Object> properties = getMappedProperties(DATASET_ENTITY_NAME);
    Map<String, Object> name = (Map<String, Object>) properties.get("name");
    assertEquals(name.get("type"), "keyword");
    Map<String, Object> nameFields = (Map<String, Object>) name.get("fields");
    assertEquals(nameFields.keySet(), Set.of("text", "stemmed", "keyword"));
    assertEquals(((Map<String, Object>) nameFields.get("text")).get("analyzer"), "v3_text");
    // The name keeps its stored casing for case-sensitive exact match
    assertFalse(((Map<String, Object>) nameFields.get("keyword")).containsKey("normalizer"));

    Map<String, Object> description = (Map<String, Object>) properties.get("description");
    assertEquals(description.get("type"), "keyword");
    assertEquals(description.get("index"), false);
    assertEquals(description.get("doc_values"), false);
    Map<String, Object> descriptionFields = (Map<String, Object>) description.get("fields");
    assertEquals(descriptionFields.keySet(), Set.of("text", "stemmed"));
    assertEquals(((Map<String, Object>) descriptionFields.get("text")).get("analyzer"), "v3_text");

    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    Map<String, Object> aspectProperties = (Map<String, Object>) aspects.get("properties");
    Map<String, Object> datasetProperties =
        (Map<String, Object>) aspectProperties.get("datasetProperties");
    Map<String, Object> datasetAspectFields =
        (Map<String, Object>) datasetProperties.get("properties");
    Map<String, Object> aspectDescription =
        (Map<String, Object>) datasetAspectFields.get("description");
    assertEquals(aspectDescription.get("index"), false);
    assertEquals(aspectDescription.get("doc_values"), false);
    assertFalse(aspectDescription.containsKey("fields"));

    assertNull(properties.get("autocomplete"));

    properties.forEach(
        (root, mapping) -> {
          Object fields = ((Map<String, Object>) mapping).get("fields");
          if (fields instanceof Map<?, ?> subfields) {
            assertTrue(
                subfields.keySet().stream()
                    .map(String::valueOf)
                    .noneMatch(
                        subfield ->
                            subfield.equals("delimited")
                                || subfield.startsWith("wordGrams")
                                || subfield.equals("ngram")),
                root + " " + subfields.keySet());
          }
        });

    assertNull(properties.get("_search"));
    assertNotNull(properties.get("name"));
    assertNotNull(properties.get("description"));
  }

  @Test
  public void testCamelCaseEntityType() {
    OperationContext fulltext = opContext.withSearchFlags(flags -> flags.setFulltext(true));
    List<String> entityTypes = List.of(DATA_JOB_ENTITY_NAME, DATASET_ENTITY_NAME);
    SearchResult result =
        searchService.search(fulltext, entityTypes, "*", null, null, 0, 10, List.of("_entityType"));
    assertEquals(
        result.getMetadata().getAggregations().stream()
            .filter(agg -> agg.getName().equals("_entityType"))
            .findFirst()
            .get()
            .getAggregations(),
        Map.of(DATA_JOB_ENTITY_NAME, 1L, DATASET_ENTITY_NAME, 2L));
    // The UI sends the entity type enum name
    assertUrns(
        searchService
            .search(
                fulltext,
                entityTypes,
                "*",
                QueryUtils.newFilter("_entityType", "DATA_JOB"),
                null,
                0,
                10)
            .getEntities(),
        NIGHTLY_JOB);
  }

  @Test
  public void testScroll() {
    OperationContext scrolling =
        scrollOperation().withSearchFlags(flags -> flags.setFulltext(true));
    assertShippedClasspathTemplate(scrolling, "*", true, SEARCH_TEMPLATE, LIST_TEMPLATE);
    assertShippedClasspathTemplate(scrolling, "orders", true, SEARCH_TEMPLATE, FULLTEXT_TEMPLATE);
    assertUrns(
        searchService
            .fullTextScroll(scrolling, ENTITY_TYPES, "*", null, null, null, null, 10, List.of())
            .getEntities(),
        ORDERS,
        CUSTOMERS,
        ORDERS_CHART);
    assertUrns(
        searchService
            .fullTextScroll(
                scrolling, ENTITY_TYPES, "orders", null, null, null, null, 10, List.of())
            .getEntities(),
        ORDERS,
        ORDERS_CHART);
  }

  @Test
  public void testIncludeExplainAndSearchType() throws IOException {
    OperationContext explained =
        pagedSearch()
            .withSearchFlags(
                flags ->
                    flags
                        .setFulltext(true)
                        .setIncludeExplain(true)
                        .setSearchType("DFS_QUERY_THEN_FETCH"));
    SearchEntityArray searched =
        searchService
            .search(explained, List.of(DATASET_ENTITY_NAME), "orders", null, null, 0, 10)
            .getEntities();
    assertUrns(searched, ORDERS);
    assertExplained(searched.get(0));
  }

  @Test
  public void testScrollIncludeExplainAndSearchType() throws IOException {
    OperationContext explained =
        scrollOperation()
            .withSearchFlags(
                flags ->
                    flags
                        .setFulltext(true)
                        .setIncludeExplain(true)
                        .setSearchType("DFS_QUERY_THEN_FETCH"));
    SearchEntityArray scrolled =
        searchService
            .fullTextScroll(
                explained, ENTITY_TYPES, "orders", null, null, null, null, 10, List.of())
            .getEntities();
    assertUrns(scrolled, ORDERS, ORDERS_CHART);
    for (SearchEntity entity : scrolled) {
      assertExplained(entity);
    }
  }

  @Test
  public void testAggregateUsesCompleteMembership() {
    OperationContext aggregate =
        aggregateOperation().withSearchFlags(flags -> flags.setFulltext(true));
    OperationContext paged = pagedSearch().withSearchFlags(flags -> flags.setFulltext(true));
    List<String> dashboards = List.of(DASHBOARD_ENTITY_NAME);
    assertShippedClasspathTemplate(aggregate, "*", false, SEARCH_TEMPLATE, LIST_TEMPLATE);
    assertShippedClasspathTemplate(aggregate, "archive", false, SEARCH_TEMPLATE, FULLTEXT_TEMPLATE);

    SearchResult light =
        searchService.search(
            paged, dashboards, "archive", null, null, 0, 10, List.of("_entityType"));
    SearchResult complete =
        searchService.search(
            aggregate, dashboards, "archive", null, null, 0, 0, List.of("_entityType"));
    assertEquals(light.getNumEntities().intValue(), 1);
    assertEquals(complete.getNumEntities().intValue(), 2);
    assertEquals(facetTotal(light, "_entityType"), 1L);
    assertEquals(facetTotal(complete, "_entityType"), 2L);

    SearchResult listed =
        searchService.search(aggregate, dashboards, "*", null, null, 0, 0, List.of("_entityType"));
    assertTrue(listed.getNumEntities() > complete.getNumEntities());
    assertEquals(facetTotal(listed, "_entityType"), listed.getNumEntities().longValue());
  }

  private static void assertExplained(SearchEntity entity) throws IOException {
    JsonNode explanation = new ObjectMapper().readTree(entity.getExtraFields().get("_explain"));
    assertTrue(explanation.get("value").floatValue() > 0);
    assertFalse(explanation.get("description").asText().isEmpty());
  }

  @Test
  public void testAutoComplete() {
    assertEquals(
        urns(searchService.autoComplete(opContext, DATASET_ENTITY_NAME, "ord", null, null, 10)),
        List.of());
    assertEquals(
        searchService
            .autoComplete(opContext, DATASET_ENTITY_NAME, "ord", null, null, 10)
            .getSuggestions(),
        List.of());
    assertEquals(
        urns(searchService.autoComplete(opContext, CHART_ENTITY_NAME, "reg", "title", null, 10)),
        List.of());
  }

  @Test
  public void testBrowseV2() {
    OperationContext browsing = browseOperation().withSearchFlags(flags -> flags.setFulltext(true));
    assertShippedClasspathTemplate(browsing, "*", false, SEARCH_TEMPLATE, LIST_TEMPLATE);
    assertShippedClasspathTemplate(browsing, "orders", false, SEARCH_TEMPLATE, FULLTEXT_TEMPLATE);
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

  /** Browse path v1 is not indexed on Search V3, so legacy browse returns nothing. */
  @Test
  public void testBrowse() {
    BrowseResult root = searchService.browse(opContext, DATASET_ENTITY_NAME, "", null, 0, 10);
    assertEquals(root.getMetadata().getTotalNumEntities().longValue(), 0L);
    assertEquals(groups(root), Map.of());
    assertEquals(root.getNumEntities().intValue(), 0);

    assertEquals(
        groups(searchService.browse(opContext, DATASET_ENTITY_NAME, "/prod", null, 0, 10)),
        Map.of());

    BrowseResult sales =
        searchService.browse(opContext, DATASET_ENTITY_NAME, "/prod/sales", null, 0, 10);
    assertEquals(groups(sales), Map.of());
    assertEquals(
        sales.getEntities().stream().map(BrowseResultEntity::getUrn).collect(Collectors.toList()),
        List.of());
  }

  @Test
  public void testGetBrowsePaths() {
    assertEquals(searchService.getBrowsePaths(opContext, DATASET_ENTITY_NAME, ORDERS), List.of());
    assertEquals(
        searchService.getBrowsePaths(opContext, DATASET_ENTITY_NAME, CUSTOMERS), List.of());
    assertEquals(
        searchService.getBrowsePaths(opContext, DATA_JOB_ENTITY_NAME, NIGHTLY_JOB), List.of());
  }

  /** Map.of takes at most 10 entries; the fixture seeds more. */
  @SuppressWarnings("unchecked")
  private static Map<Urn, List<MCLItem>> seedEvents(Object... urnsAndEvents) {
    Map<Urn, List<MCLItem>> seeded = new java.util.LinkedHashMap<>();
    for (int i = 0; i < urnsAndEvents.length; i += 2) {
      seeded.put((Urn) urnsAndEvents[i], (List<MCLItem>) urnsAndEvents[i + 1]);
    }
    return seeded;
  }

  private List<MCLItem> keyDeletion(Urn urn) {
    return List.of(
        ((TestMCL) events(urn).get(0)).toBuilder().changeType(ChangeType.DELETE).build());
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

  private static BrowsePaths legacyBrowsePaths(String... paths) {
    return new BrowsePaths().setPaths(new StringArray(Arrays.asList(paths)));
  }

  private static List<Urn> urns(AutoCompleteResult result) {
    return result.getEntities().stream()
        .map(AutoCompleteEntity::getUrn)
        .collect(Collectors.toList());
  }

  private static Map<String, Long> groups(BrowseResultV2 result) {
    return result.getGroups().stream()
        .collect(Collectors.toMap(BrowseResultGroupV2::getName, BrowseResultGroupV2::getCount));
  }

  private static Map<String, Long> groups(BrowseResult result) {
    return result.getGroups().stream()
        .collect(Collectors.toMap(BrowseResultGroup::getName, BrowseResultGroup::getCount));
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getMappedProperties(String entityType) throws IOException {
    String index =
        opContext
            .getSearchContext()
            .getIndexConvention()
            .getEntityIndexNameV3(
                opContext,
                V3IndexKeys.resolve(opContext.getEntityRegistry().getEntitySpec(entityType)));
    return (Map<String, Object>)
        getSearchClient()
            .getIndexMapping(
                OperationFingerprint.EMPTY,
                new GetMappingsRequest().indices(index),
                RequestOptions.DEFAULT)
            .mappings()
            .get(index)
            .sourceAsMap()
            .get("properties");
  }

  private boolean indexExists(String index) throws IOException {
    return getSearchClient()
        .indexExists(opContext, new GetIndexRequest(index), RequestOptions.DEFAULT);
  }

  private static void assertUrns(SearchEntityArray entities, Urn... expected) {
    assertEqualsNoOrder(entities.stream().map(SearchEntity::getEntity).toArray(), expected);
  }

  private OperationContext pagedSearch() {
    return opContext.withSearchSurface(SearchTemplateSurfaces.SEARCH_ENTITIES);
  }

  private OperationContext scrollOperation() {
    return opContext.withSearchSurface(SearchTemplateSurfaces.SCROLL_ENTITIES);
  }

  private OperationContext aggregateOperation() {
    return opContext.withSearchSurface(SearchTemplateSurfaces.AGGREGATE_ENTITIES);
  }

  private OperationContext browseOperation() {
    return opContext.withSearchSurface(null);
  }

  private static long facetTotal(SearchResult result, String name) {
    return result.getMetadata().getAggregations().stream()
        .filter(aggregation -> name.equals(aggregation.getName()))
        .flatMap(aggregation -> aggregation.getAggregations().values().stream())
        .mapToLong(Long::longValue)
        .sum();
  }

  /**
   * The route walk starts on a shipped classpath template. {@code expectedChain} is the seed and
   * each route it follows. Light's ladder continues to fulltext only when that first query does not
   * stop, which the walk decides per request. Scroll binding uses the scroll request path.
   */
  private static void assertShippedClasspathTemplate(
      OperationContext context, String query, String... expectedChain) {
    assertShippedClasspathTemplate(context, query, false, expectedChain);
  }

  private static void assertShippedClasspathTemplate(
      OperationContext context, String query, boolean scroll, String... expectedChain) {
    SearchTemplateCoordinator coordinator = SearchTemplates.current();
    assertNotNull(coordinator);
    OperationContext bound = coordinator.bind(context, query, scroll);
    ResolvedSearchTemplate resolved = bound.getSearchContext().getResolvedSearchTemplate();
    assertNotNull(resolved, query);
    assertEquals(resolved.getRouteChain(), List.of(expectedChain), query);
    MergedSearchTemplate template = coordinator.loader().load(context, resolved.getUrn());
    assertTrue(template.isClasspathBridge(), template.getUrn());
    if ("urn:li:searchTemplate:light".equals(resolved.getUrn())) {
      assertNotNull(template.getLadder());
      assertEquals(template.getLadder().getNext().toString(), "urn:li:searchTemplate:fulltext");
    }
  }
}

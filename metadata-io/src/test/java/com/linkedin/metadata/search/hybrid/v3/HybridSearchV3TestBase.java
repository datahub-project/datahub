package com.linkedin.metadata.search.hybrid.v3;

import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_STRUCT_PROPS_DISABLED;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION;
import static io.datahubproject.test.search.SearchTestUtils.syncAfterWrite;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.EmbeddingChunk;
import com.linkedin.common.EmbeddingChunkArray;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.EmbeddingModelDataMap;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.FloatArray;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.resolvers.knowledge.DocumentSearchFilterUtils;
import com.linkedin.datahub.graphql.resolvers.search.DefaultEntityFiltersUtil;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.knowledge.DocumentContents;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.knowledge.DocumentState;
import com.linkedin.knowledge.DocumentStatus;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SearchTemplateConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateSource;
import com.linkedin.metadata.config.search.SearchV3Configuration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SemanticSearchService;
import com.linkedin.metadata.search.VectorRetrievalExplanation;
import com.linkedin.metadata.search.cache.EntityDocCountCache;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.SearchWriteAccess;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.DocumentV3EmbeddingMappingContributor;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntityMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntitySettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.UpdateIndicesV3Strategy;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateBootstrap;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCapabilities;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateComponentRegistry;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCoordinator;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFixtures;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateGates;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateKeywordRerank;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateLoader;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateNoAnchorException;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateSurfaces;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateVectorRetrieval;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.embedding.EmbeddingProfileRegistry;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.embedding.EmbeddingTaskType;
import com.linkedin.metadata.search.hybrid.HybridCandidateMerger;
import com.linkedin.metadata.search.hybrid.HybridLexicalScoreNormalizer;
import com.linkedin.metadata.search.hybrid.HybridQueryEmbeddingService;
import com.linkedin.metadata.search.hybrid.HybridScoreCombiner;
import com.linkedin.metadata.search.hybrid.HybridScoreMapBuilder;
import com.linkedin.metadata.search.hybrid.HybridSearchResultReranker;
import com.linkedin.metadata.search.hybrid.HybridVectorScoreNormalizer;
import com.linkedin.metadata.search.semantic.SemanticEntitySearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.service.LifecycleStageTypeService;
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
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.test.metadata.aspect.batch.TestMCL;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.mockito.AdditionalAnswers;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.client.RequestOptions;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Hybrid search on Search V3, run against each engine's test container. Documents carry vectors on
 * the V3 document index; a dataset matches the same words and has none. The kNN query scores only
 * the documents among the reranked keyword rows.
 */
public abstract class HybridSearchV3TestBase extends AbstractTestNGSpringContextTests {

  private static final String MODEL_KEY = "test_model";
  private static final float[] QUERY_VECTOR = {1f, 0f, 0f, 0f};
  // The stronger keyword match has the vector farther from the query
  private static final Urn FAR_DOCUMENT = UrnUtils.getUrn("urn:li:document:revenue-far");
  private static final Urn NEAR_DOCUMENT = UrnUtils.getUrn("urn:li:document:revenue-near");
  private static final Urn DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,revenue,PROD)");
  private static final List<String> ENTITY_TYPES = List.of("dataset", "document");
  // Over 1,000 chunks match "forecast": eleven documents of 100 chunks each sit nearer the query
  // than every chunk of the keyword match, and one chunk of the semantic match is nearer still
  private static final Urn FORECAST_KEYWORD_MATCH =
      UrnUtils.getUrn("urn:li:document:forecast-keyword");
  private static final Urn FORECAST_SEMANTIC_MATCH =
      UrnUtils.getUrn("urn:li:document:forecast-semantic");
  private static final int CROWD_DOCUMENTS = 11;
  private static final int CROWD_CHUNKS = 100;
  // Unpublished, and as near the query as any document
  private static final Urn UNPUBLISHED_DOCUMENT =
      UrnUtils.getUrn("urn:li:document:quarterly-draft");
  private static final String SEMANTIC_TEMPLATE = "urn:li:searchTemplate:semantic";
  // Also as near the query, owned by OWNER, and in review: a lifecycle stage other than
  // published or draft, which a test hides from search
  private static final Urn IN_REVIEW_DOCUMENT = UrnUtils.getUrn("urn:li:document:in-review-notes");
  private static final Urn IN_REVIEW = UrnUtils.getUrn("urn:li:lifecycleStageType:IN_REVIEW");
  private static final Urn OWNER = UrnUtils.getUrn("urn:li:corpuser:owner");

  private final List<String> createdIndices = new ArrayList<>();
  // Each document's semanticContent as written, which a similarity anchor's vectors are read from
  private final Map<Urn, SemanticContent> semanticContents = new HashMap<>();
  private OperationContext opContext;
  private ElasticSearchConfiguration config;
  private SemanticSearchConfiguration semanticSearch;
  private EntityIndexConfiguration entityIndex;
  private MappingsBuilder mappingsBuilder;
  private ESSearchDAO keywordSearch;
  private ESSearchDAO hybridSearch;

  @Nonnull
  protected abstract SearchClientShim<?> getSearchClient();

  @Nonnull
  protected abstract ESBulkProcessor getBulkProcessor();

  @BeforeClass
  public void setUp() throws Exception {
    if (!SemanticEntitySearchService.supportsV3SemanticFilters(getSearchClient())) {
      throw new SkipException("Hybrid read needs a Search V3 client that supports semantic reads");
    }
    ModelEmbeddingConfig model = new ModelEmbeddingConfig();
    model.setVectorDimension(QUERY_VECTOR.length);
    semanticSearch =
        new SemanticSearchConfiguration(
            true, Set.of("document"), Map.of(MODEL_KEY, model), null, null, null);
    entityIndex =
        V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION.toBuilder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(false).build())
            .v3(
                V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION.getV3().toBuilder()
                    .keywordReadEnabled(true)
                    .hybridReadEnabled(true)
                    .build())
            .semanticSearch(semanticSearch)
            .build();
    config = TEST_ES_SEARCH_CONFIG.toBuilder().entityIndex(entityIndex).build();
    IndexConvention indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
            new ConfiguredIndexPrefixResolver("hybridv3"),
            entityIndex);
    EntityRegistry entityRegistry = TestOperationContexts.defaultEntityRegistry();
    // Built as MappingsBuilderFactory and SettingsBuilderFactory build them for the V3 cluster
    mappingsBuilder =
        new MultiEntityMappingsBuilder(
            entityIndex,
            null,
            ESUtils.KEYWORD_MAXLENGTH,
            List.of(new DocumentV3EmbeddingMappingContributor(semanticSearch, getSearchClient())));
    MultiEntitySettingsBuilder settingsBuilder =
        new MultiEntitySettingsBuilder(
            entityIndex, indexConvention, getSearchClient(), semanticSearch);
    opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
                SearchContext.builder()
                    .indexConvention(indexConvention)
                    .searchClusterAccess(SearchClusterAccess.fixed(getSearchClient()))
                    .searchableFieldTypes(
                        ESUtils.buildSearchableFieldTypes(entityRegistry, mappingsBuilder))
                    .searchableFieldPaths(ESUtils.buildSearchableFieldPaths(entityRegistry))
                    .build())
            .withSearchFlags(flags -> flags.setFulltext(true));

    ESIndexBuilder indexBuilder =
        new ESIndexBuilder(
            getSearchClient(),
            config,
            TEST_ES_STRUCT_PROPS_DISABLED,
            Map.of(),
            new GitVersion("0.0.0-test", "123456", Optional.empty()),
            getSearchClient());
    Map<String, Map<String, Object>> mappings =
        mappingsBuilder.getIndexMappings(opContext, List.of()).stream()
            .collect(
                Collectors.toMap(
                    MappingsBuilder.IndexMapping::getIndexName,
                    MappingsBuilder.IndexMapping::getMappings));
    for (String entityType : ENTITY_TYPES) {
      String indexName = indexConvention.getEntityIndexNameV3(opContext, entityType);
      indexBuilder.buildIndex(
          opContext,
          indexBuilder.buildReindexState(
              opContext,
              indexName,
              mappings.get(indexName),
              settingsBuilder.getSettings(config.getIndex(), indexName)));
      createdIndices.add(indexName);
    }

    keywordSearch =
        new ESSearchDAO(
            false, config, null, QueryFilterRewriteChain.EMPTY, TEST_SEARCH_SERVICE_CONFIG);
    EmbeddingProvider embeddingProvider = mock(EmbeddingProvider.class);
    when(embeddingProvider.embed(
            anyString(), any(), any(EmbeddingTaskType.class), any(Duration.class)))
        .thenReturn(QUERY_VECTOR);
    hybridSearch =
        new ESSearchDAO(
            false,
            config,
            null,
            QueryFilterRewriteChain.EMPTY,
            false,
            TEST_SEARCH_SERVICE_CONFIG,
            new Sha256UrnEntityDocumentIdHasher(),
            new HybridSearchResultReranker(
                new HybridQueryEmbeddingService(
                    embeddingProvider, null, MODEL_KEY, QUERY_VECTOR.length),
                new V3HybridKnnRequestBuilder(semanticSearch),
                new HybridScoreMapBuilder(
                    new HybridVectorScoreNormalizer(
                        getSearchClient().getEngineType(), model.getSpaceType())),
                new HybridCandidateMerger(
                    new HybridLexicalScoreNormalizer(), new HybridScoreCombiner())),
            null);

    ElasticSearchService searchService =
        new ElasticSearchService(
            indexBuilder,
            TEST_SEARCH_SERVICE_CONFIG,
            config,
            mappingsBuilder,
            settingsBuilder,
            keywordSearch,
            new ESBrowseDAO(
                config, null, QueryFilterRewriteChain.EMPTY, TEST_SEARCH_SERVICE_CONFIG),
            new ESWriteDAO(
                config,
                getSearchClient(),
                getBulkProcessor(),
                SearchWriteAccess.fixed(getBulkProcessor())));
    // No document title equals the query: an exact title match scores in a keyword tier of its own,
    // which the vector score does not lift a document over. The keyword match whose rank an
    // assertion relies on leads through a title that holds the query word; that lead is small
    // next to the vectors, which decide
    Map<Urn, List<MCLItem>> batch = new LinkedHashMap<>();
    batch.put(
        FAR_DOCUMENT,
        document(
            FAR_DOCUMENT,
            DocumentState.PUBLISHED,
            "Revenue by region",
            "Revenue by region and revenue by quarter",
            new float[] {0f, 1f, 0f, 0f}));
    batch.put(
        NEAR_DOCUMENT,
        document(
            NEAR_DOCUMENT,
            DocumentState.PUBLISHED,
            "Notes on quarterly revenue",
            "How we report quarterly revenue",
            new float[] {1f, 0f, 0f, 0f}));
    batch.put(DATASET, events(DATASET, new DatasetProperties().setName("revenue")));
    float[] far = {0f, 0f, 1f, 0f};
    batch.put(
        FORECAST_KEYWORD_MATCH,
        document(
            FORECAST_KEYWORD_MATCH,
            DocumentState.PUBLISHED,
            "Forecast summary",
            "Forecast forecast forecast",
            far,
            far,
            far));
    batch.put(
        FORECAST_SEMANTIC_MATCH,
        document(
            FORECAST_SEMANTIC_MATCH,
            DocumentState.PUBLISHED,
            "Yearly forecast planning",
            "How we plan the yearly forecast",
            new float[] {1f, 0f, 0f, 0f}));
    float[][] crowdChunks = new float[CROWD_CHUNKS][];
    Arrays.fill(crowdChunks, new float[] {0.8f, 0.6f, 0f, 0f});
    for (int i = 0; i < CROWD_DOCUMENTS; i++) {
      Urn crowd = UrnUtils.getUrn("urn:li:document:forecast-crowd-" + i);
      batch.put(
          crowd,
          document(
              crowd,
              DocumentState.PUBLISHED,
              "Monthly forecast outlook " + i,
              "Mentions the forecast once",
              crowdChunks));
    }
    // Its words match no keyword query here
    batch.put(
        UNPUBLISHED_DOCUMENT,
        document(
            UNPUBLISHED_DOCUMENT,
            DocumentState.UNPUBLISHED,
            "Draft of the quarterly results",
            "Quarterly results not yet shared",
            new float[] {1f, 0f, 0f, 0f}));
    List<MCLItem> inReview =
        new ArrayList<>(
            document(
                IN_REVIEW_DOCUMENT,
                DocumentState.UNPUBLISHED,
                "Team notes in review",
                "Notes the team is still reviewing",
                new float[] {1f, 0f, 0f, 0f}));
    // Its key aspect is already among the document's events
    events(
            IN_REVIEW_DOCUMENT,
            new Status().setLifecycleStage(IN_REVIEW),
            new Ownership()
                .setOwners(
                    new OwnerArray(
                        new Owner().setOwner(OWNER).setType(OwnershipType.TECHNICAL_OWNER))))
        .stream()
        .skip(1)
        .forEach(inReview::add);
    batch.put(IN_REVIEW_DOCUMENT, inReview);
    // With the semantic configuration the writer lifts document vectors to the root embeddings
    UpdateIndicesV3Strategy.builder()
        .v3Config(entityIndex.getV3())
        .elasticSearchService(searchService)
        .searchDocumentTransformer(
            new SearchDocumentTransformer(1000, 1000, 1000, false, ESUtils.KEYWORD_MAXLENGTH))
        .timeseriesAspectService(mock(TimeseriesAspectService.class))
        .semanticSearchConfiguration(semanticSearch)
        .build()
        .processBatch(opContext, batch, false);
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
  public void testDocumentsFollowTheirVectorsInTheirOwnPositions() {
    SearchResult keyword =
        keywordSearch.search(opContext, ENTITY_TYPES, "revenue", null, null, 0, 10, List.of());
    SearchResult hybrid =
        hybridSearch.search(opContext, ENTITY_TYPES, "revenue", null, null, 0, 10, List.of());

    // Keyword search ranks the stronger text match first; the vectors reverse the documents
    assertEquals(documents(keyword), List.of(FAR_DOCUMENT, NEAR_DOCUMENT));
    assertEquals(documents(hybrid), List.of(NEAR_DOCUMENT, FAR_DOCUMENT));
    // The dataset keeps its keyword position, and totals stay those of keyword search
    assertEquals(
        urns(hybrid).indexOf(DATASET), urns(keyword).indexOf(DATASET), urns(hybrid).toString());
    assertEquals(hybrid.getNumEntities(), keyword.getNumEntities());
  }

  @Test
  public void testFilteredSearchIsReranked() {
    // The type facet as the UI sends it
    Filter documentsOnly = filter("_entityType", "DOCUMENT");

    SearchResult hybrid =
        hybridSearch.search(
            opContext, ENTITY_TYPES, "revenue", documentsOnly, null, 0, 10, List.of());

    assertEquals(urns(hybrid), List.of(NEAR_DOCUMENT, FAR_DOCUMENT));
  }

  /**
   * Keyword-surface parity with hybrid read on. When no row in the window can be blended, the
   * shipped fulltext template returns exactly what the same template without its keywordRerank
   * sections returns, which the default hybrid reranker serves: the ordered URNs, the scores, the
   * total, and the facets.
   */
  @Test
  public void testShippedFulltextKeepsTheKeywordResultWithNoEligibleRows() {
    String fulltext = "urn:li:searchTemplate:fulltext";
    String keywordOnlyUrn = "urn:li:searchTemplate:fulltextKeywordOnly";
    Map<String, SearchTemplateProperties> catalog = new HashMap<>();
    for (String urn : SearchTemplateBootstrap.urns()) {
      catalog.put(urn, SearchTemplateBootstrap.get(urn));
    }
    SearchTemplateProperties keywordOnly = SearchTemplateBootstrap.get(fulltext);
    keywordOnly.removeRetrieval();
    keywordOnly.removeKnn();
    keywordOnly.setApiVersion(SearchTemplateCapabilities.API_VERSION);
    catalog.put(keywordOnlyUrn, keywordOnly);
    SearchTemplateConfiguration templates = new SearchTemplateConfiguration();
    templates.setEnabled(true);
    SearchTemplateFixtures.setShippedLimits(templates);
    OperationContext searching =
        opContext.withSearchTemplateRuntime(
            new SearchTemplateCoordinator(
                config.toBuilder()
                    .search(
                        config.getSearch().toBuilder()
                            .v3(new SearchV3Configuration().setTemplates(templates))
                            .build())
                    .build(),
                null,
                new SearchTemplateLoader(0, SearchTemplateSource.CLASSPATH, catalog),
                ForkJoinPool.commonPool(),
                SearchTemplateComponentRegistry.empty(),
                SearchTemplateGates.of(
                    EmbeddingProfileRegistry.fromSemanticSearch(semanticSearch, MODEL_KEY),
                    semanticSearch,
                    true)));
    // The window holds the dataset and one document, and one document row cannot be blended
    Filter oneDocument =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("urn")
                                    .setCondition(Condition.EQUAL)
                                    .setValues(
                                        new StringArray(
                                            List.of(
                                                DATASET.toString(), NEAR_DOCUMENT.toString())))))));
    List<String> facets = List.of("_entityType", "platform");
    List<SearchResult> served = new ArrayList<>();
    for (String template : List.of(fulltext, keywordOnlyUrn)) {
      served.add(
          hybridSearch.search(
              searching.withSearchFlags(
                  flags -> flags.setSearchTemplate(UrnUtils.getUrn(template))),
              ENTITY_TYPES,
              "revenue",
              oneDocument,
              null,
              0,
              10,
              facets));
    }

    assertEquals(Set.copyOf(urns(served.get(0))), Set.of(DATASET, NEAR_DOCUMENT));
    assertEquals(urns(served.get(0)), urns(served.get(1)));
    assertEquals(scores(served.get(0)), scores(served.get(1)));
    assertEquals(served.get(0).getNumEntities(), served.get(1).getNumEntities());
    assertEquals(
        served.get(0).getMetadata().getAggregations(),
        served.get(1).getMetadata().getAggregations());
    // The shipped rung is a keywordRerank rung that kept keyword order for this reason
    SearchResult explained =
        hybridSearch.search(
            searching.withSearchFlags(
                flags ->
                    flags.setSearchTemplate(UrnUtils.getUrn(fulltext)).setIncludeExplain(true)),
            ENTITY_TYPES,
            "revenue",
            oneDocument,
            null,
            0,
            10,
            facets);
    assertEquals(
        explained.getMetadata().getKeywordRerank().getReason(),
        SearchTemplateKeywordRerank.NO_ELIGIBLE_ROWS);
  }

  /**
   * Semantic search through the semantic search surface, end to end on a real index. With its
   * rollout on, the shipped semantic template's vector rung serves the request through the search
   * cache, and explain names the template. With it off, the request stays on the legacy semantic
   * search, which runs its own kNN search on the same index. On OpenSearch before 3.5 it stays
   * there with the rollout on too: before 3.5 the k-NN plugin applies a nested kNN pre-filter in
   * the chunk scope when it names an underscore-prefixed field such as {@code _entityType}, which
   * the template's filter always does. Without a caller filter that pre-filter holds only the
   * system clauses, and the search must still find documents.
   */
  @Test
  public void testTheSemanticSurfaceServesTheSemanticTemplateOnlyWithItsSwitchOn()
      throws IOException {
    SemanticSearchService semantic = semanticSearchService();
    SearchTemplateConfiguration templates = semanticSurfaceTemplates();
    OperationContext searching = templateContext(templates);
    List<String> documents = List.of("document");
    // Approximate kNN with a two-row window can miss the exact matches among the crowd's 1,100
    // near chunks, so the filter, which both searches apply inside the kNN request, holds three
    // documents: the two nearest the query and one far from it
    Filter candidates =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("urn")
                                    .setCondition(Condition.EQUAL)
                                    .setValues(
                                        new StringArray(
                                            List.of(
                                                NEAR_DOCUMENT.toString(),
                                                FORECAST_SEMANTIC_MATCH.toString(),
                                                FAR_DOCUMENT.toString())))))));

    // Off by default: the request stays on the legacy semantic search, with no template explain
    assertFalse(semantic.isServedByTemplate(searching));
    SearchResult legacy =
        semantic.semanticSearchAcrossEntities(
            searching, documents, "quarterly revenue", candidates, null, 0, 2, List.of());
    assertEquals(Set.copyOf(urns(legacy)), Set.of(NEAR_DOCUMENT, FORECAST_SEMANTIC_MATCH));
    assertFalse(legacy.getMetadata().hasRouteChain());
    assertFalse(legacy.getMetadata().hasVectorRetrieval());

    // On: the shipped semantic template's vector rung serves the two documents nearest the query,
    // except on OpenSearch before 3.5, where the request stays on the legacy semantic search
    templates.getRollout().setSemanticSearch("active");
    boolean templateServes = engineCanServeTheSemanticTemplate();
    assertEquals(semantic.isServedByTemplate(searching), templateServes);
    SearchResult served = semanticSearch(semantic, searching, documents, candidates);
    assertEquals(Set.copyOf(urns(served)), Set.of(NEAR_DOCUMENT, FORECAST_SEMANTIC_MATCH));
    assertEquals(served.getMetadata().hasRouteChain(), templateServes);
    assertEquals(served.getMetadata().hasVectorRetrieval(), templateServes);
    if (templateServes) {
      assertEquals(served.getMetadata().getRouteChain(), List.of(SEMANTIC_TEMPLATE));
      VectorRetrievalExplanation explanation = served.getMetadata().getVectorRetrieval();
      assertEquals(explanation.getOutcome(), SearchTemplateVectorRetrieval.SERVED);
      assertEquals(explanation.getProfile(), MODEL_KEY);
      assertEquals(explanation.getEntityTypes(), documents);
      assertTrue(explanation.isFilterInVectorSearch());
    }

    // No caller filter: on either path the search finds documents. Approximate kNN without the
    // candidate filter can miss the exact matches, so only the path is checked
    SearchResult unfiltered = semanticSearch(semantic, searching, documents, null);
    assertFalse(unfiltered.getEntities().isEmpty());
    assertEquals(unfiltered.getMetadata().hasRouteChain(), templateServes);
    assertEquals(unfiltered.getMetadata().hasVectorRetrieval(), templateServes);
    if (templateServes) {
      assertEquals(
          unfiltered.getMetadata().getVectorRetrieval().getOutcome(),
          SearchTemplateVectorRetrieval.SERVED);
    }
  }

  /**
   * The published-document default the semantic search resolver adds on the template path, applied
   * inside the nested kNN request on a real index. Its lifecycle stage, state and placement clauses
   * read fields of the parent document, and they leave out the unpublished document although it is
   * as near the query as the published one. The caller's filter names the two documents, as in the
   * case above: without it, approximate kNN can miss the exact matches among the crowd's 1,100
   * identical chunks.
   */
  @Test
  public void testTheSemanticTemplateLeavesOutUnpublishedDocumentsUnderTheDocumentDefault()
      throws IOException {
    if (!engineCanServeTheSemanticTemplate()) {
      throw new SkipException("Semantic search stays off the template on OpenSearch before 3.5");
    }
    SemanticSearchService semantic = semanticSearchService();
    SearchTemplateConfiguration templates = semanticSurfaceTemplates();
    templates.getRollout().setSemanticSearch("active");
    OperationContext searching = templateContext(templates);
    List<String> documents = List.of("document");
    Filter callerFilter = filter("urn", NEAR_DOCUMENT.toString(), UNPUBLISHED_DOCUMENT.toString());
    // What the resolver passes with that filter for a caller who cannot manage documents
    Filter withDefault =
        DefaultEntityFiltersUtil.addDefaultEntityFilters(callerFilter, documents, true, false);

    SearchResult withoutDefault = semanticSearch(semantic, searching, documents, callerFilter);
    SearchResult served = semanticSearch(semantic, searching, documents, withDefault);

    // Without the default both come back, so the unpublished document is indexed with its vector
    assertEquals(Set.copyOf(urns(withoutDefault)), Set.of(NEAR_DOCUMENT, UNPUBLISHED_DOCUMENT));
    assertEquals(served.getMetadata().getRouteChain(), List.of(SEMANTIC_TEMPLATE));
    assertEquals(
        served.getMetadata().getVectorRetrieval().getOutcome(),
        SearchTemplateVectorRetrieval.SERVED);
    assertEquals(urns(served), List.of(NEAR_DOCUMENT));
  }

  /** Semantic search as the resolver runs it: through the template when the service says so. */
  private static SearchResult semanticSearch(
      SemanticSearchService semantic,
      OperationContext searching,
      List<String> entityTypes,
      Filter filter) {
    return semantic.isServedByTemplate(searching)
        ? semantic.semanticSearchAcrossEntitiesByTemplate(
            searching, entityTypes, "quarterly revenue", filter, null, 0, 2, List.of())
        : semantic.semanticSearchAcrossEntities(
            searching, entityTypes, "quarterly revenue", filter, null, 0, 2, List.of());
  }

  /** Every engine but OpenSearch before 3.5, read from the container this leg runs. */
  private boolean engineCanServeTheSemanticTemplate() throws IOException {
    if (!getSearchClient().getEngineType().isOpenSearch()) {
      return true;
    }
    String[] version = getSearchClient().getEngineVersion().split("\\.");
    int major = Integer.parseInt(version[0]);
    return major > 3 || major == 3 && Integer.parseInt(version[1]) >= 5;
  }

  /**
   * Semantic search on this leg's index, every query embedded as {@link #QUERY_VECTOR}. The
   * template path reads through the search cache; the legacy path runs its own kNN search.
   */
  private SemanticSearchService semanticSearchService() {
    EmbeddingProvider fixedQuery = mock(EmbeddingProvider.class);
    when(fixedQuery.embed(anyString(), any(), any(EmbeddingTaskType.class), any(Duration.class)))
        .thenReturn(QUERY_VECTOR);
    when(fixedQuery.embed(anyString(), any(), any(EmbeddingTaskType.class)))
        .thenReturn(QUERY_VECTOR);
    when(fixedQuery.embed(anyString(), any())).thenReturn(QUERY_VECTOR);
    ESSearchDAO vectorSearch =
        new ESSearchDAO(
            false,
            config,
            null,
            QueryFilterRewriteChain.EMPTY,
            false,
            TEST_SEARCH_SERVICE_CONFIG,
            new Sha256UrnEntityDocumentIdHasher(),
            null,
            new HybridQueryEmbeddingService(fixedQuery, null, MODEL_KEY, QUERY_VECTOR.length));
    EntitySearchService entitySearch = mock(EntitySearchService.class);
    when(entitySearch.getSearchServiceConfig()).thenReturn(TEST_SEARCH_SERVICE_CONFIG);
    when(entitySearch.search(any(), any(), any(), any(), any(), anyInt(), any(), any()))
        .thenAnswer(
            invocation ->
                vectorSearch.search(
                    invocation.getArgument(0),
                    invocation.getArgument(1),
                    invocation.getArgument(2),
                    invocation.getArgument(3),
                    invocation.getArgument(4),
                    invocation.getArgument(5),
                    invocation.getArgument(6),
                    invocation.getArgument(7)));
    return new SemanticSearchService(
        mock(EntityDocCountCache.class),
        new CachingEntitySearchService(new ConcurrentMapCacheManager(), entitySearch, 100, true),
        new SemanticEntitySearchService(
            getSearchClient(), fixedQuery, mappingsBuilder, MODEL_KEY, entityIndex),
        TEST_SEARCH_SERVICE_CONFIG.toBuilder().semanticSearchEnabled(true).build());
  }

  /**
   * Templates on, with the semantic search surface selecting the shipped semantic template. The
   * surface's rollout stays off until a caller sets it.
   */
  private static SearchTemplateConfiguration semanticSurfaceTemplates() {
    SearchTemplateConfiguration templates = new SearchTemplateConfiguration();
    templates.setEnabled(true);
    SearchTemplateFixtures.setShippedLimits(templates);
    templates
        .getDefaults()
        .put("fulltext", new HashMap<>(Map.of("default", "urn:li:searchTemplate:search")));
    templates
        .getSurfaces()
        .put(
            SearchTemplateSurfaces.SEMANTIC_SEARCH,
            Map.of("fulltext", Map.<String, Object>of("default", SEMANTIC_TEMPLATE)));
    return templates;
  }

  /** This leg's context running the shipped templates under {@code templates}, explain on. */
  private OperationContext templateContext(SearchTemplateConfiguration templates) {
    Map<String, SearchTemplateProperties> catalog = new HashMap<>();
    for (String urn : SearchTemplateBootstrap.urns()) {
      catalog.put(urn, SearchTemplateBootstrap.get(urn));
    }
    return opContext
        .withSearchTemplateRuntime(
            new SearchTemplateCoordinator(
                config.toBuilder()
                    .search(
                        config.getSearch().toBuilder()
                            .v3(new SearchV3Configuration().setTemplates(templates))
                            .build())
                    .build(),
                null,
                new SearchTemplateLoader(0, SearchTemplateSource.CLASSPATH, catalog),
                ForkJoinPool.commonPool(),
                SearchTemplateComponentRegistry.empty(),
                SearchTemplateGates.of(
                    EmbeddingProfileRegistry.fromSemanticSearch(semanticSearch, MODEL_KEY),
                    semanticSearch,
                    true)))
        .withSearchFlags(flags -> flags.setIncludeExplain(true));
  }

  /**
   * Similarity on a real index, where the anchor check renders the document defaults on this
   * engine's own fields. For a viewer who cannot manage documents, a published anchor serves its
   * neighbors, and an unpublished one fails as noAnchor. A viewer who can manage documents has no
   * unpublished clause in the defaults, so the same unpublished anchor serves, which shows that
   * clause decided.
   */
  @Test
  public void testTheDocumentDefaultsDecideWhichDocumentsCanAnchorSimilarity() throws IOException {
    if (!engineCanServeTheSemanticTemplate()) {
      // Before 3.5, OpenSearch applies the kNN entity type filter at chunk scope, where it
      // matches nothing
      throw new SkipException("Similarity finds no neighbors on OpenSearch before 3.5");
    }
    Filter viewer = documentDefaults(false);
    Filter manager = documentDefaults(true);

    SearchResult published = similar(NEAR_DOCUMENT, viewer);
    assertEquals(
        published.getMetadata().getVectorRetrieval().getOutcome(),
        SearchTemplateVectorRetrieval.SERVED);
    assertTrue(urns(published).contains(FORECAST_SEMANTIC_MATCH), urns(published).toString());
    assertFalse(urns(published).contains(NEAR_DOCUMENT), urns(published).toString());
    assertFalse(urns(published).contains(UNPUBLISHED_DOCUMENT), urns(published).toString());

    expectThrows(
        SearchTemplateNoAnchorException.class, () -> similar(UNPUBLISHED_DOCUMENT, viewer));

    SearchResult managed = similar(UNPUBLISHED_DOCUMENT, manager);
    assertTrue(
        urns(managed).containsAll(List.of(NEAR_DOCUMENT, FORECAST_SEMANTIC_MATCH)),
        urns(managed).toString());
  }

  /**
   * Document search's visibility rule names the lifecycle stage, so it takes the place of the
   * hidden-stage clause for the anchor as it does for the results: with in review hidden from
   * search, the owner's document in review still anchors document search. The document defaults
   * admit no document in a stage other than published, so, hidden or not, they still refuse it.
   */
  @Test
  public void testAnOwnersDocumentInAHiddenStageAnchorsDocumentSearch() {
    LifecycleStageTypeService stages = mock(LifecycleStageTypeService.class);
    when(stages.getHiddenStageUrns(anySet())).thenReturn(Set.of(IN_REVIEW.toString()));
    LifecycleStageTypeService previous = ESUtils.getLifecycleStageTypeService();
    ESUtils.setLifecycleStageTypeService(stages);
    try {
      SearchResult owned =
          similar(
              IN_REVIEW_DOCUMENT,
              DocumentSearchFilterUtils.buildCombinedFilter(
                  List.of(), List.of(OWNER.toString()), true, false));
      assertTrue(urns(owned).contains(FORECAST_SEMANTIC_MATCH), urns(owned).toString());
      assertFalse(urns(owned).contains(UNPUBLISHED_DOCUMENT), urns(owned).toString());

      // Another viewer's rule does not admit the owner's document in review
      expectThrows(
          SearchTemplateNoAnchorException.class,
          () ->
              similar(
                  IN_REVIEW_DOCUMENT,
                  DocumentSearchFilterUtils.buildCombinedFilter(
                      List.of(), List.of("urn:li:corpuser:not-the-owner"), true, false)));

      for (boolean canManageDocuments : List.of(false, true)) {
        expectThrows(
            SearchTemplateNoAnchorException.class,
            () -> similar(IN_REVIEW_DOCUMENT, documentDefaults(canManageDocuments)));
      }
    } finally {
      ESUtils.setLifecycleStageTypeService(previous);
    }
  }

  /** The document defaults as the GraphQL resolvers add them for documents. */
  private static Filter documentDefaults(boolean canManageDocuments) {
    return DefaultEntityFiltersUtil.addDefaultEntityFilters(
        null, List.of("document"), true, canManageDocuments);
  }

  /**
   * The documents similar to {@code anchor} through the shipped similarity template, with {@code
   * visibility} as the request filter and as the anchor filter, as a GraphQL resolver sends it when
   * the caller sets no filter of their own.
   */
  private SearchResult similar(Urn anchor, Filter visibility) {
    Map<String, SearchTemplateProperties> catalog = new HashMap<>();
    for (String urn : SearchTemplateBootstrap.urns()) {
      catalog.put(urn, SearchTemplateBootstrap.get(urn));
    }
    SearchTemplateConfiguration templates = new SearchTemplateConfiguration();
    templates.setEnabled(true);
    SearchTemplateFixtures.setShippedLimits(templates);
    // The DataHub index's store reads an anchor's vectors from its stored semanticContent
    AspectRetriever stored =
        mock(AspectRetriever.class, AdditionalAnswers.delegatesTo(opContext.getAspectRetriever()));
    semanticContents.forEach(
        (urn, content) ->
            doReturn(new Aspect(content.data()))
                .when(stored)
                .getLatestAspectObject(
                    any(),
                    eq(urn),
                    eq(SearchTemplateVectorRetrieval.SEMANTIC_CONTENT_ASPECT_NAME)));
    OperationContext searching =
        opContext.toBuilder()
            .retrieverContext(
                RetrieverContext.builder()
                    .graphRetriever(opContext.getRetrieverContext().getGraphRetriever())
                    .aspectRetriever(stored)
                    .cachingAspectRetriever(
                        opContext.getRetrieverContext().getCachingAspectRetriever())
                    .searchRetriever(opContext.getRetrieverContext().getSearchRetriever())
                    .build())
            .build(opContext.getSessionActorContext(), false)
            .withSearchTemplateRuntime(
                new SearchTemplateCoordinator(
                    config.toBuilder()
                        .search(
                            config.getSearch().toBuilder()
                                .v3(new SearchV3Configuration().setTemplates(templates))
                                .build())
                        .build(),
                    null,
                    new SearchTemplateLoader(0, SearchTemplateSource.CLASSPATH, catalog),
                    ForkJoinPool.commonPool(),
                    SearchTemplateComponentRegistry.empty(),
                    SearchTemplateGates.of(
                        EmbeddingProfileRegistry.fromSemanticSearch(semanticSearch, MODEL_KEY),
                        semanticSearch,
                        true)))
            .withSearchFlags(
                flags ->
                    flags
                        .setSearchTemplate(UrnUtils.getUrn("urn:li:searchTemplate:similarity"))
                        .setIncludeExplain(true))
            .withAnchorFilter(visibility);
    // Similarity embeds nothing, so it needs no query embedding service
    return new ESSearchDAO(
            false,
            config,
            null,
            QueryFilterRewriteChain.EMPTY,
            false,
            TEST_SEARCH_SERVICE_CONFIG,
            new Sha256UrnEntityDocumentIdHasher(),
            null,
            null)
        .search(
            searching, List.of("document"), anchor.toString(), visibility, null, 0, 10, List.of());
  }

  private static List<Double> scores(SearchResult result) {
    return result.getEntities().stream().map(SearchEntity::getScore).collect(Collectors.toList());
  }

  private List<MCLItem> document(
      Urn urn, DocumentState state, String title, String text, float[]... chunkVectors) {
    AuditStamp created = new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(0L);
    EmbeddingChunkArray chunks = new EmbeddingChunkArray();
    for (int i = 0; i < chunkVectors.length; i++) {
      chunks.add(
          new EmbeddingChunk()
              .setPosition(i)
              .setVector(new FloatArray(toList(chunkVectors[i])))
              .setText(text));
    }
    EmbeddingModelData embedding =
        new EmbeddingModelData()
            .setModelVersion("test/" + MODEL_KEY)
            .setGeneratedAt(0L)
            .setTotalChunks(chunkVectors.length)
            .setChunks(chunks);
    SemanticContent semanticContent =
        new SemanticContent()
            .setEmbeddings(new EmbeddingModelDataMap(Map.of(MODEL_KEY, embedding)));
    semanticContents.put(urn, semanticContent);
    return events(
        urn,
        new DocumentInfo()
            .setTitle(title)
            .setStatus(new DocumentStatus().setState(state))
            .setContents(new DocumentContents().setText(text))
            .setCreated(created)
            .setLastModified(created),
        semanticContent);
  }

  @Test
  public void testSemanticMatchPassesKeywordMatchAmongManyChunks() {
    SearchResult keyword =
        keywordSearch.search(opContext, ENTITY_TYPES, "forecast", null, null, 0, 20, List.of());
    SearchResult hybrid =
        hybridSearch.search(opContext, ENTITY_TYPES, "forecast", null, null, 0, 20, List.of());

    // Nested kNN counts documents, not chunks, so the keyword match gets its own vector score
    // although 1,100 chunks of other documents are nearer the query, and the semantic match passes
    assertEquals(documents(keyword).get(0), FORECAST_KEYWORD_MATCH);
    assertEquals(documents(keyword).size(), CROWD_DOCUMENTS + 2);
    List<Urn> reranked = documents(hybrid);
    assertTrue(
        reranked.indexOf(FORECAST_SEMANTIC_MATCH) < reranked.indexOf(FORECAST_KEYWORD_MATCH),
        reranked.toString());
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

  private static List<Float> toList(float[] vector) {
    List<Float> values = new ArrayList<>(vector.length);
    for (float value : vector) {
      values.add(value);
    }
    return values;
  }

  private static Filter filter(String field, String... values) {
    Criterion criterion =
        new Criterion()
            .setField(field)
            .setCondition(Condition.EQUAL)
            .setValues(new StringArray(List.of(values)));
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion().setAnd(new CriterionArray(criterion))));
  }

  private static List<Urn> urns(SearchResult result) {
    return result.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList());
  }

  private static List<Urn> documents(SearchResult result) {
    return urns(result).stream()
        .filter(urn -> "document".equals(urn.getEntityType()))
        .collect(Collectors.toList());
  }
}

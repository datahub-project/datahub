package com.linkedin.metadata.search.hybrid;

import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_STRUCT_PROPS_DISABLED;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION;
import static io.datahubproject.test.search.SearchTestUtils.syncAfterWrite;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.EmbeddingChunk;
import com.linkedin.common.EmbeddingChunkArray;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.EmbeddingModelDataMap;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.FloatArray;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.knowledge.DocumentContents;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.knowledge.DocumentState;
import com.linkedin.knowledge.DocumentStatus;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.SearchWriteAccess;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.DocumentV3EmbeddingMappingContributor;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntityMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntitySettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.embedding.EmbeddingTaskType;
import com.linkedin.metadata.search.semantic.SemanticEntitySearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.service.UpdateIndicesV3Strategy;
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
import com.linkedin.test.metadata.aspect.batch.TestMCL;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.client.RequestOptions;
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

  private final List<String> createdIndices = new ArrayList<>();
  private OperationContext opContext;
  private ESSearchDAO keywordSearch;
  private ESSearchDAO hybridSearch;

  @Nonnull
  protected abstract SearchClientShim<?> getSearchClient();

  @Nonnull
  protected abstract ESBulkProcessor getBulkProcessor();

  @BeforeClass
  public void setUp() throws Exception {
    if (!SemanticEntitySearchService.supportsV3SemanticFilters(getSearchClient())) {
      throw new SkipException(
          "Hybrid read needs V3 semantic reads, which DataHub refuses on OpenSearch before 3.5");
    }
    ModelEmbeddingConfig model = new ModelEmbeddingConfig();
    model.setVectorDimension(QUERY_VECTOR.length);
    SemanticSearchConfiguration semanticSearch =
        new SemanticSearchConfiguration(true, Set.of("document"), Map.of(MODEL_KEY, model), null);
    EntityIndexConfiguration entityIndex =
        V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION.toBuilder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(false).build())
            .v3(
                V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION.getV3().toBuilder()
                    .keywordReadEnabled(true)
                    .semanticReadEnabled(true)
                    .hybridReadEnabled(true)
                    .build())
            .semanticSearch(semanticSearch)
            .build();
    ElasticSearchConfiguration config =
        TEST_ES_SEARCH_CONFIG.toBuilder().entityIndex(entityIndex).build();
    IndexConvention indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
            new ConfiguredIndexPrefixResolver("hybridv3"),
            entityIndex);
    EntityRegistry entityRegistry = TestOperationContexts.defaultEntityRegistry();
    // Built as MappingsBuilderFactory and SettingsBuilderFactory build them for the V3 cluster
    MappingsBuilder mappingsBuilder =
        new MultiEntityMappingsBuilder(
            entityIndex,
            getSearchClient(),
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
            new GitVersion("0.0.0-test", "123456", Optional.empty()));
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
                    new HybridLexicalScoreNormalizer(), new HybridScoreCombiner())));

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
    // which the vector score does not lift a document over. Each keyword match leads through a
    // title that holds the query word; that lead is small next to the vectors, which decide
    Map<Urn, List<MCLItem>> batch = new LinkedHashMap<>();
    batch.put(
        FAR_DOCUMENT,
        document(
            FAR_DOCUMENT,
            "Revenue by region",
            "Revenue by region and revenue by quarter",
            new float[] {0f, 1f, 0f, 0f}));
    batch.put(
        NEAR_DOCUMENT,
        document(
            NEAR_DOCUMENT,
            "Notes on quarterly revenue",
            "How we report quarterly revenue",
            new float[] {1f, 0f, 0f, 0f}));
    batch.put(DATASET, events(DATASET, new DatasetProperties().setName("revenue")));
    float[] far = {0f, 0f, 1f, 0f};
    batch.put(
        FORECAST_KEYWORD_MATCH,
        document(
            FORECAST_KEYWORD_MATCH,
            "Forecast summary",
            "Forecast forecast forecast",
            far,
            far,
            far));
    batch.put(
        FORECAST_SEMANTIC_MATCH,
        document(
            FORECAST_SEMANTIC_MATCH,
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
              crowd, "Monthly forecast outlook " + i, "Mentions the forecast once", crowdChunks));
    }
    // With the semantic configuration the writer lifts document vectors to the root embeddings
    new UpdateIndicesV3Strategy(
            entityIndex.getV3(),
            searchService,
            new SearchDocumentTransformer(1000, 1000, 1000, false, ESUtils.KEYWORD_MAXLENGTH),
            mock(TimeseriesAspectService.class),
            null,
            new Sha256UrnEntityDocumentIdHasher(),
            List.of(),
            false,
            semanticSearch)
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

  private List<MCLItem> document(Urn urn, String title, String text, float[]... chunkVectors) {
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
    return events(
        urn,
        new DocumentInfo()
            .setTitle(title)
            .setStatus(new DocumentStatus().setState(DocumentState.PUBLISHED))
            .setContents(new DocumentContents().setText(text))
            .setCreated(created)
            .setLastModified(created),
        new SemanticContent()
            .setEmbeddings(new EmbeddingModelDataMap(Map.of(MODEL_KEY, embedding))));
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

  private static Filter filter(String field, String value) {
    Criterion criterion =
        new Criterion()
            .setField(field)
            .setCondition(Condition.EQUAL)
            .setValues(new StringArray(List.of(value)));
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

package com.linkedin.metadata.search.quality;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_STRUCT_PROPS_DISABLED;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION;
import static io.datahubproject.test.search.SearchTestUtils.createDelegatingMappingsBuilder;
import static io.datahubproject.test.search.SearchTestUtils.createDelegatingSettingsBuilder;
import static io.datahubproject.test.search.SearchTestUtils.syncAfterWrite;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateSource;
import com.linkedin.metadata.config.search.SearchV3Configuration;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.SearchWriteAccess;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.UpdateIndicesV3Strategy;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCoordinator;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFixtures;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateLoader;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateSurfaces;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplates;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.elasticsearch.ConfiguredIndexPrefixResolver;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import com.linkedin.test.metadata.aspect.batch.TestMCL;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.QrelScorer;
import io.datahubproject.test.search.RankedSearchHit;
import io.datahubproject.test.search.RankedSearchResult;
import io.datahubproject.test.search.SearchQualityPack;
import io.datahubproject.test.search.SearchQualityReport;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.mockito.Mockito;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.client.RequestOptions;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Loads the complete search-quality datapack once and scores judgments against Search V3. The pack
 * is indexed through {@link UpdateIndicesV3Strategy}; judgments only choose which queries run.
 */
public abstract class SearchQualityV3TestBase extends AbstractTestNGSpringContextTests {
  private static final long PACK_TIME = 1700000000000L;
  private static final Urn ACTOR = UrnUtils.getUrn("urn:li:corpuser:datahub");

  private final List<String> createdIndices = new ArrayList<>();
  private final Map<String, Card> cards = new HashMap<>();
  private OperationContext opContext;
  private ElasticSearchService searchService;
  private int shardCount;

  @Nonnull
  protected abstract SearchClientShim<?> getSearchClient();

  @Nonnull
  protected abstract ESBulkProcessor getBulkProcessor();

  @Nonnull
  protected abstract String engineName();

  @BeforeClass
  public void loadPack() throws Exception {
    List<JsonNode> shards = SearchQualityPack.readShards();
    shardCount = shards.size();
    assertEquals(shardCount, 6);
    EntityIndexConfiguration entityIndex =
        V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION.toBuilder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(false).build())
            .v3(
                V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION.getV3().toBuilder()
                    .keywordReadEnabled(true)
                    .build())
            .build();
    var config =
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
            new ConfiguredIndexPrefixResolver("searchquality"),
            entityIndex);
    EntityRegistry entityRegistry = TestOperationContexts.defaultEntityRegistry();
    MappingsBuilder mappingsBuilder = createDelegatingMappingsBuilder(entityIndex);
    Map<Urn, List<RecordTemplate>> definitionAspects = new LinkedHashMap<>();
    List<Pair<Urn, StructuredPropertyDefinition>> structuredProperties = new ArrayList<>();
    Map<Urn, List<MCLItem>> events = new LinkedHashMap<>();
    Set<String> entityTypes = new HashSet<>();
    for (JsonNode shard : shards) {
      for (JsonNode proposal : shard) {
        Urn urn = UrnUtils.getUrn(proposal.path("entityUrn").asText());
        entityTypes.add(urn.getEntityType());
        RecordTemplate aspect = aspect(entityRegistry, urn, proposal);
        rememberCard(proposal, aspect);
        events
            .computeIfAbsent(urn, ignored -> new ArrayList<>())
            .add(event(entityRegistry, urn, aspect));
        if (aspect instanceof StructuredPropertyDefinition definition) {
          definitionAspects.computeIfAbsent(urn, ignored -> new ArrayList<>()).add(definition);
          definitionAspects.get(urn).add(new Status().setRemoved(false));
          structuredProperties.add(Pair.of(urn, definition));
        }
      }
    }
    for (Urn urn : events.keySet()) {
      EntitySpec entitySpec = entityRegistry.getEntitySpec(urn.getEntityType());
      events
          .get(urn)
          .add(
              0,
              event(
                  entityRegistry,
                  urn,
                  EntityKeyUtils.convertUrnToEntityKey(urn, entitySpec.getKeyAspectSpec())));
    }
    MockAspectRetriever aspectRetriever = new MockAspectRetriever(definitionAspects);
    aspectRetriever.setEntityRegistry(entityRegistry);
    List<Urn> propertyUrns = structuredProperties.stream().map(Pair::getKey).toList();
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .aspectRetriever(aspectRetriever)
            .cachingAspectRetriever(definitionsWithActiveUsers(aspectRetriever))
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(structuredPropertySearchRetriever(propertyUrns))
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
                    .build())
            .withSearchSurface(SearchTemplateSurfaces.SEARCH_ENTITIES);
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
    Map<String, Map<String, Object>> mappings =
        mappingsBuilder.getIndexMappings(opContext, structuredProperties).stream()
            .collect(
                Collectors.toMap(
                    MappingsBuilder.IndexMapping::getIndexName,
                    MappingsBuilder.IndexMapping::getMappings));
    for (String entityType : entityTypes) {
      String indexName =
          indexConvention.getEntityIndexNameV3(
              opContext, V3IndexKeys.resolve(entityRegistry.getEntitySpec(entityType)));
      indexBuilder.buildIndex(
          opContext,
          indexBuilder.buildReindexState(
              opContext,
              indexName,
              mappings.get(indexName),
              settingsBuilder.getSettings(config.getIndex(), indexName)));
      createdIndices.add(indexName);
    }
    UpdateIndicesV3Strategy.builder()
        .v3Config(entityIndex.getV3())
        .elasticSearchService(searchService)
        .searchDocumentTransformer(
            new SearchDocumentTransformer(1000, 1000, 1000, false, ESUtils.KEYWORD_MAXLENGTH))
        .timeseriesAspectService(Mockito.mock(TimeseriesAspectService.class))
        .build()
        .processBatch(opContext, events, true);
    syncAfterWrite(getBulkProcessor());
  }

  @AfterClass(alwaysRun = true)
  public void deleteIndices() throws Exception {
    SearchTemplates.install(null);
    for (String index : createdIndices) {
      getSearchClient()
          .deleteIndex(
              OperationFingerprint.EMPTY, new DeleteIndexRequest(index), RequestOptions.DEFAULT);
    }
  }

  @Test
  public void testEveryShardWasIndexed() {
    assertEquals(shardCount, 6);
    assertTrue(cards.containsKey("urn:li:dataPlatform:cranfield"));
    assertTrue(
        cards.containsKey("urn:li:dataset:(urn:li:dataPlatform:cranfield,cranfield.1,PROD)"));
    assertTrue(
        cards.containsKey(
            "urn:li:dataset:(urn:li:dataPlatform:dbt,long_tail_companions.analytics.dogs_in_movies,PROD)"));
    assertTrue(
        cards.containsKey("urn:li:dataset:(urn:li:dataPlatform:catalogprobe,orders_daily,PROD)"));
    assertTrue(cards.containsKey("urn:li:document:semantic-access-request"));
  }

  @Test
  public void testRepresentativeEntityFromEachShardIsSearchable() throws Exception {
    assertFalse(search("dataPlatform", "cranfield", null).getHits().isEmpty(), "definitions");
    assertFalse(
        search(DATASET_ENTITY_NAME, "cranfield.1", null).getHits().isEmpty(), "cranfield name");
    assertTrue(
        search(DATASET_ENTITY_NAME, "spanwise distribution of the lift", null).getHits().stream()
            .anyMatch(
                hit ->
                    hit.getUrn()
                        .equals("urn:li:dataset:(urn:li:dataPlatform:cranfield,cranfield.1,PROD)")),
        "cranfield description");
    assertFalse(search(DATASET_ENTITY_NAME, "pet_profiles", null).getHits().isEmpty(), "long tail");
    assertFalse(
        search(
                DATASET_ENTITY_NAME,
                "*",
                new Filter()
                    .setOr(
                        new ConjunctiveCriterionArray(
                            new ConjunctiveCriterion()
                                .setAnd(
                                    new CriterionArray(
                                        buildCriterion("rowCount", Condition.EQUAL, "68"))))))
            .getHits()
            .isEmpty(),
        "timeseries");
    assertFalse(
        search(DATASET_ENTITY_NAME, "Catalogprobe Orders Daily", null).getHits().isEmpty(),
        "catalog probe");
    assertFalse(
        search("document", "Getting Started with DataHub", null).getHits().isEmpty(),
        "semantic probe");
  }

  @Test
  public void testSearchEntitiesAndLineageRoutes() {
    OperationContext keyword =
        opContext
            .withSearchSurface(SearchTemplateSurfaces.SEARCH_ENTITIES)
            .withSearchFlags(flags -> flags.setFulltext(true));
    ResolvedSearchTemplate entities =
        SearchTemplates.current()
            .bind(keyword, "experimental investigation of the aerodynamics", false)
            .getSearchContext()
            .getResolvedSearchTemplate();
    assertNotNull(entities);
    assertEquals(entities.getRouteChain().get(0), "urn:li:searchTemplate:searchLight");
    OperationContext lineage =
        opContext
            .withSearchSurface(SearchTemplateSurfaces.SEARCH_ENTITIES_LINEAGE)
            .withSearchFlags(flags -> flags.setFulltext(true));
    ResolvedSearchTemplate lineageTemplate =
        SearchTemplates.current()
            .bind(lineage, "pet profiles", false)
            .getSearchContext()
            .getResolvedSearchTemplate();
    assertEquals(lineageTemplate.getUrn(), "urn:li:searchTemplate:lineage");
  }

  @Test
  public void testBenchmarkNdcgAndProbes() throws Exception {
    JsonNode file = SearchQualityPack.judgments();
    double floor = file.path("floors").path(engineName()).path("benchmarkNdcgAt10").asDouble(0.0);
    List<SearchQualityReport.QueryScore> scores = new ArrayList<>();
    List<String> probeFailures = new ArrayList<>();
    List<SearchQualityReport.ProbeScore> probes = new ArrayList<>();
    for (JsonNode judgment : file.path("judgments")) {
      String group = judgment.path("group").asText();
      if ("semantic-probe".equals(group)) {
        continue;
      }
      if ("benchmark".equals(group)) {
        scores.add(SearchQualityReport.query(judgment, run(judgment)));
      } else if (judgment.path("surfaces").toString().contains("keyword")
          || judgment.path("surfaces").toString().contains("lineage")) {
        SearchQualityReport.ProbeScore probe = SearchQualityReport.probe(judgment, run(judgment));
        if (!probe.passed()) {
          probeFailures.add(probe.message());
        }
        if ("ranking".equals(group) || "catalog-probe".equals(group)) {
          probes.add(probe);
        }
      }
    }
    double mean =
        QrelScorer.mean(scores.stream().map(SearchQualityReport.QueryScore::ndcgAt10).toList());
    Path report = writeReport("benchmarkNdcgAt10", floor, scores, probes);
    assertEquals(scores.size(), 225);
    assertTrue(mean > 0.0, withReport("benchmark nDCG@" + engineName() + "=" + mean, report));
    assertTrue(mean + 1e-9 >= floor, withReport("nDCG " + mean + " fell below " + floor, report));
    assertTrue(
        probeFailures.isEmpty(),
        withReport(
            "nDCG@" + engineName() + "=" + mean + " " + String.join("\n", probeFailures), report));
  }

  /**
   * The same Cranfield queries scored by the Java Search V3 builder. Skipping template execution is
   * the fallback used when templates are off: the request never renders a template, and the
   * light-then-full cascade in {@code ESSearchDAO} decides the query.
   */
  @Test
  public void testJavaBuilderBenchmarkNdcg() throws Exception {
    OperationContext javaContext =
        opContext
            .withSearchSurface(SearchTemplateSurfaces.SEARCH_ENTITIES)
            .withSkipTemplateExecution(true)
            .withSearchFlags(flags -> flags.setFulltext(true));
    assertFalse(SearchTemplates.current().executes(javaContext));
    JsonNode file = SearchQualityPack.judgments();
    double floor =
        file.path("floors").path(engineName()).path("javaBuilderBenchmarkNdcgAt10").asDouble(0.0);
    List<SearchQualityReport.QueryScore> scores = new ArrayList<>();
    for (JsonNode judgment : file.path("judgments")) {
      if (!"benchmark".equals(judgment.path("group").asText())) {
        continue;
      }
      scores.add(SearchQualityReport.query(judgment, run(judgment, true)));
    }
    double mean =
        QrelScorer.mean(scores.stream().map(SearchQualityReport.QueryScore::ndcgAt10).toList());
    Path report = writeReport("javaBuilderBenchmarkNdcgAt10", floor, scores, List.of());
    assertEquals(scores.size(), 225);
    assertTrue(mean > 0.0, withReport("java builder nDCG@" + engineName() + "=" + mean, report));
    assertTrue(
        mean + 1e-9 >= floor,
        withReport("java builder nDCG " + mean + " fell below " + floor, report));
  }

  /**
   * Mean nDCG@10 of the long-tail catalog queries on the template path. Grades prefer the asset
   * whose name or description is the query, then the same asset on another platform.
   */
  @Test
  public void testLongtailNdcg() throws Exception {
    scoreGroup("longtail", false, "longtailNdcgAt10");
  }

  /**
   * The same catalog queries scored by the Java Search V3 builder, with template execution skipped.
   */
  @Test
  public void testJavaBuilderLongtailNdcg() throws Exception {
    OperationContext javaContext =
        opContext
            .withSearchSurface(SearchTemplateSurfaces.SEARCH_ENTITIES)
            .withSkipTemplateExecution(true)
            .withSearchFlags(flags -> flags.setFulltext(true));
    assertFalse(SearchTemplates.current().executes(javaContext));
    scoreGroup("longtail", true, "javaBuilderLongtailNdcgAt10");
  }

  private void scoreGroup(String group, boolean javaBuilder, String floorName) throws Exception {
    JsonNode file = SearchQualityPack.judgments();
    double floor = file.path("floors").path(engineName()).path(floorName).asDouble(0.0);
    List<SearchQualityReport.QueryScore> scores = new ArrayList<>();
    for (JsonNode judgment : file.path("judgments")) {
      if (!group.equals(judgment.path("group").asText())) {
        continue;
      }
      scores.add(SearchQualityReport.query(judgment, run(judgment, javaBuilder)));
    }
    double mean =
        QrelScorer.mean(scores.stream().map(SearchQualityReport.QueryScore::ndcgAt10).toList());
    Path report = writeReport(floorName, floor, scores, List.of());
    assertFalse(scores.isEmpty(), group + " has no queries");
    assertTrue(
        mean + 1e-9 >= floor, withReport(group + " nDCG " + mean + " fell below " + floor, report));
  }

  private Path writeReport(
      String metric,
      double floor,
      List<SearchQualityReport.QueryScore> queries,
      List<SearchQualityReport.ProbeScore> probes)
      throws IOException {
    String engine =
        getSearchClient().getEngineType() == SearchClientShim.SearchEngineType.OPENSEARCH_3
            ? "opensearch3"
            : engineName();
    return SearchQualityReport.write(engine, metric, floor, queries, probes);
  }

  private static String withReport(String message, Path report) {
    if (report == null) {
      return message;
    }
    return message + " report=" + report;
  }

  private RankedSearchResult run(JsonNode judgment) throws Exception {
    return run(judgment, false);
  }

  private RankedSearchResult run(JsonNode judgment, boolean javaBuilder) throws Exception {
    boolean lineage = judgment.path("surfaces").toString().contains("lineage");
    Filter filter = null;
    if (judgment.hasNonNull("filter")) {
      JsonNode criterion = judgment.get("filter");
      filter =
          new Filter()
              .setOr(
                  new ConjunctiveCriterionArray(
                      new ConjunctiveCriterion()
                          .setAnd(
                              new CriterionArray(
                                  buildCriterion(
                                      criterion.path("field").asText(),
                                      Condition.valueOf(criterion.path("condition").asText()),
                                      criterion.path("value").asText())))));
    }
    if (judgment.has("candidateUrns")) {
      List<String> urns = new ArrayList<>();
      judgment.get("candidateUrns").forEach(node -> urns.add(node.asText()));
      filter =
          new Filter()
              .setOr(
                  new ConjunctiveCriterionArray(
                      new ConjunctiveCriterion()
                          .setAnd(
                              new CriterionArray(buildCriterion("urn", Condition.EQUAL, urns)))));
    }
    List<String> entityTypes = new ArrayList<>();
    judgment.path("entityTypes").forEach(node -> entityTypes.add(node.asText()));
    String surface =
        lineage
            ? SearchTemplateSurfaces.SEARCH_ENTITIES_LINEAGE
            : SearchTemplateSurfaces.SEARCH_ENTITIES;
    return search(
        entityTypes,
        judgment.path("query").asText(),
        filter,
        surface,
        judgment.path("id").asText(),
        javaBuilder);
  }

  private RankedSearchResult search(String entityType, String query, Filter filter)
      throws Exception {
    return search(
        List.of(entityType),
        query,
        filter,
        SearchTemplateSurfaces.SEARCH_ENTITIES,
        entityType + ":" + query,
        false);
  }

  private RankedSearchResult search(
      List<String> entityTypes,
      String query,
      Filter filter,
      String surface,
      String queryId,
      boolean javaBuilder)
      throws Exception {
    OperationContext context =
        opContext
            .withSearchSurface(surface)
            .withSkipTemplateExecution(javaBuilder)
            .withSearchFlags(flags -> flags.setFulltext(true).setSkipHighlighting(false));
    SearchEntityArray entities =
        searchService.search(context, entityTypes, query, filter, null, 0, 10).getEntities();
    List<RankedSearchHit> hits = new ArrayList<>();
    for (SearchEntity entity : entities) {
      String urn = entity.getEntity().toString();
      Card card = cards.getOrDefault(urn, new Card("", ""));
      List<String> matched = new ArrayList<>();
      if (entity.getMatchedFields() != null) {
        entity
            .getMatchedFields()
            .forEach(
                field -> {
                  String rendered = field.getName() + " " + field.getValue();
                  matched.add(rendered);
                });
      }
      hits.add(
          new RankedSearchHit(
              urn,
              entity.getEntity().getEntityType(),
              card.name,
              card.description,
              entity.getScore() == null ? 0.0 : entity.getScore(),
              matched));
    }
    return new RankedSearchResult(queryId, surface, query, hits);
  }

  private void rememberCard(JsonNode proposal, RecordTemplate aspect) {
    String urn = proposal.path("entityUrn").asText();
    JsonNode json = proposal.path("aspect").path("json");
    String aspectName = proposal.path("aspectName").asText();
    Card card = cards.computeIfAbsent(urn, ignored -> new Card("", ""));
    if (json.hasNonNull("name")) {
      card.name = json.get("name").asText();
    }
    if (json.hasNonNull("title")) {
      card.name = json.get("title").asText();
    }
    if (json.hasNonNull("description")) {
      card.description = json.get("description").asText();
    }
    if (json.has("contents") && json.get("contents").hasNonNull("text")) {
      card.description = json.get("contents").get("text").asText();
    }
    if (aspect == null || aspectName.isEmpty()) {
      return;
    }
  }

  private static RecordTemplate aspect(EntityRegistry entityRegistry, Urn urn, JsonNode proposal) {
    String aspectName = proposal.path("aspectName").asText();
    AspectSpec aspectSpec =
        entityRegistry.getEntitySpec(urn.getEntityType()).getAspectSpec(aspectName);
    if (aspectSpec == null) {
      throw new IllegalStateException("No aspect " + aspectName + " on " + urn);
    }
    return GenericRecordUtils.deserializeAspect(
        ByteString.copyString(
            proposal.path("aspect").path("json").toString(), StandardCharsets.UTF_8),
        GenericRecordUtils.JSON,
        aspectSpec);
  }

  private static MCLItem event(EntityRegistry entityRegistry, Urn urn, RecordTemplate aspect) {
    EntitySpec entitySpec = entityRegistry.getEntitySpec(urn.getEntityType());
    AspectSpec aspectSpec =
        entitySpec.getAspectSpec(
            com.linkedin.metadata.utils.PegasusUtils.getAspectNameFromSchema(aspect.schema()));
    SystemMetadata systemMetadata =
        new SystemMetadata()
            .setLastObserved(PACK_TIME)
            .setRunId("search-quality")
            .setLastRunId("search-quality");
    return TestMCL.builder()
        .urn(urn)
        .changeType(ChangeType.UPSERT)
        .metadataChangeLog(new MetadataChangeLog().setSystemMetadata(systemMetadata))
        .entitySpec(entitySpec)
        .aspectSpec(aspectSpec)
        .recordTemplate(aspect)
        .auditStamp(new AuditStamp().setActor(ACTOR).setTime(PACK_TIME))
        .build();
  }

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
            SearchTemplateSurfaces.SEARCH_ENTITIES_LINEAGE,
            Map.of("fulltext", slot("urn:li:searchTemplate:lineage"))));
    return templates;
  }

  private static Map<String, Object> slot(String urn) {
    Map<String, Object> slot = new HashMap<>();
    slot.put("default", urn);
    return slot;
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

  private static final class Card {
    private String name;
    private String description;

    private Card(String name, String description) {
      this.name = name;
      this.description = description;
    }
  }
}

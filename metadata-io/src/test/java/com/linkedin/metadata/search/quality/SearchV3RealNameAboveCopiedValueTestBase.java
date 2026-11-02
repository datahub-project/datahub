package com.linkedin.metadata.search.quality;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_STRUCT_PROPS_DISABLED;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION;
import static io.datahubproject.test.search.SearchTestUtils.createDelegatingMappingsBuilder;
import static io.datahubproject.test.search.SearchTestUtils.createDelegatingSettingsBuilder;
import static io.datahubproject.test.search.SearchTestUtils.syncAfterWrite;
import static org.testng.Assert.assertEquals;

import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.DataHubSearchConfig;
import com.linkedin.dataset.DatasetProperties;
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
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateBootstrap;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateComponentRegistry;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCoordinator;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFixtures;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateLoader;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateSurfaces;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
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
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import com.linkedin.test.metadata.aspect.batch.TestMCL;
import com.linkedin.test.metadata.aspect.batch.TestSystemAspect;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.client.RequestOptions;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * A catalog where many assets share a display title, a structured property whose values are copied
 * into the name. A user searching for a real name must find that asset first, above every asset
 * whose copied title merely equals it, and a partial real-name match must rank above a partial
 * title match.
 *
 * <p>Each scenario asserts the exact expected order, by tranche, against the shipped templates and
 * against a strict class-order template stored as test data. The tranches are: EXACT_NAME,
 * EXACT_COPIED, PARTIAL_NAME, PARTIAL_COPIED, then OTHER. Exact means the query as typed equals the
 * whole value, ignoring case: kennel_booking is exact for kennel_booking, never for
 * kennel_booking_archive or booking. A value that differs from the query only in separators may
 * rank either way and is left out of the comparison. Partial means every query word starts a word
 * of the value, or the value starts with the query, with no stemming. Queries are typed without
 * quotes, as a user types them; one scenario checks that quoting gives the same order. A dataset
 * takes its best tranche, graded on its real name first, so a dataset whose own title equals or
 * holds the query is graded on its real name.
 */
public abstract class SearchV3RealNameAboveCopiedValueTestBase
    extends AbstractTestNGSpringContextTests {
  private static final long TIME = 1700000000000L;
  private static final Urn ACTOR = UrnUtils.getUrn("urn:li:corpuser:datahub");
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static final String DISPLAY_TITLE = "io.example.displayTitle";
  static final Urn DISPLAY_TITLE_URN =
      UrnUtils.getUrn("urn:li:structuredProperty:" + DISPLAY_TITLE);
  protected static final String CLASS_ORDER = "urn:li:searchTemplate:rankingClassOrder";

  /** More copies than the 100-row window a three- or four-word query used to get. */
  static final int EQUAL_TITLE_COPIES = 150;

  private final List<String> createdIndices = new ArrayList<>();
  private final Map<String, Dataset> corpus = new LinkedHashMap<>();
  private OperationContext opContext;
  private ElasticSearchService searchService;

  @Nonnull
  protected abstract SearchClientShim<?> getSearchClient();

  @Nonnull
  protected abstract ESBulkProcessor getBulkProcessor();

  record Dataset(String urn, String name, @Nullable String title) {}

  private void add(String name, @Nullable String title) {
    String urn = "urn:li:dataset:(urn:li:dataPlatform:hive,petshop." + name + ",PROD)";
    corpus.put(urn, new Dataset(urn, name, title));
  }

  private void buildCorpus() {
    add("kennel_booking", null);
    for (int i = 0; i < EQUAL_TITLE_COPIES; i++) {
      add(String.format("kb_listing_%03d", i), "Kennel Booking");
    }
    for (int i = 0; i < 30; i++) {
      add(String.format("kennel_booking_snapshot_%02d", i), null);
    }
    // Near misses for kennel_booking: they hold the words but are not the whole value.
    add("kennel_booking_archive", null);
    add("kennel_booking_2024", null);
    add("booking_kennel", null);
    add("kennel", null);
    add("booking", null);
    add("kba_ref", "Kennel Booking Archive");
    add("groomed_coat_notes", null);
    add("gcn_ref", "Groomed Coat Notes");
    add("groom_coat_notes_stage", null);
    add("salon_audit_groomed", null);
    add("groomed_journal", "Groomed Journal Entries");
    for (int i = 0; i < 20; i++) {
      add(String.format("gje_ref_%02d", i), "Groomed Journal Entries " + i);
    }
    // Indexed after the datasets that only carry the title, so index order cannot put it first.
    for (int i = 0; i < 20; i++) {
      add(String.format("cr_ref_%02d", i), "Cattery Roster");
    }
    add("Cattery Roster", "Cattery Roster");
  }

  // ── Scenarios ───────────────────────────────────────────────────────────────

  /**
   * Typed as a user types it, without quotes: only the dataset named exactly kennel_booking is an
   * exact real-name match. Names and titles that merely hold the words (kennel_booking_archive,
   * booking_kennel, "Kennel Booking Archive", the 30 snapshots) rank below it, and the tranche
   * order holds past the old 100-row window.
   */
  protected void assertUnquotedQueryExactOnlyForWholeName(@Nullable String template)
      throws Exception {
    assertTrancheOrder(template, "kennel_booking");
  }

  /** The same query in quotes must give the same tranche order as the unquoted one. */
  protected void assertQuotedQueryExactOnlyForWholeName(@Nullable String template)
      throws Exception {
    assertTrancheOrder(template, "\"kennel_booking\"");
  }

  /**
   * Only the title equal to the query is an exact copied match: groomed_journal, titled "Groomed
   * Journal Entries", ranks above the 20 titles "Groomed Journal Entries N".
   */
  protected void assertExactCopiedOnlyForWholeTitle(@Nullable String template) throws Exception {
    assertTrancheOrder(template, "groomed journal entries");
  }

  /**
   * The one copied title equal to the query ranks above groom_coat_notes_stage, which shares the
   * other words and only the stem of "groomed".
   */
  protected void assertStemOnlyNameBelowExactCopiedTitle(@Nullable String template)
      throws Exception {
    assertTrancheOrder(template, "groomed coat notes");
  }

  /** A real name equal to its own copied title is an exact real-name match. */
  protected void assertRealNameEqualToOwnTitleIsExactRealName(@Nullable String template)
      throws Exception {
    assertTrancheOrder(template, "cattery roster");
  }

  /** A real name holding the word ranks with the partial real names, even when its title does. */
  protected void assertPartialRealNameWithOwnTitleRanksAsRealName(@Nullable String template)
      throws Exception {
    assertTrancheOrder(template, "groomed");
  }

  /**
   * Compares the served results, by tranche, with the expected tranche sequence. A tranche holds
   * ties in any order; a position in the wrong tranche fails, and the message lists the actual
   * order beside the expected one.
   */
  private void assertTrancheOrder(@Nullable String template, String query) throws Exception {
    List<Dataset> graded = corpus.values().stream().filter(d -> rank(query, d) != OTHER).toList();
    List<Dataset> actual = search(template, query, graded.size());
    List<Dataset> expected =
        graded.stream()
            .filter(d -> rank(query, d) != SEPARATOR_ONLY)
            .sorted((a, b) -> Integer.compare(rank(query, a), rank(query, b)))
            .toList();
    // A name or title that differs from the query only in separators may rank either way.
    List<String> actualTranches =
        actual.stream()
            .filter(d -> rank(query, d) != SEPARATOR_ONLY)
            .limit(expected.size())
            .map(d -> className(query, d))
            .toList();
    List<String> expectedTranches = expected.stream().map(d -> className(query, d)).toList();
    assertEquals(
        actualTranches,
        expectedTranches,
        String.format(
            "%s, query %s: results out of tranche order%n  expected: %s%n  actual:   %s",
            template == null ? "shipped templates" : template,
            query,
            describe(query, expected),
            describe(query, actual)));
  }

  /** The first five results by name, then the whole list as runs of one class. */
  private String describe(String query, List<Dataset> datasets) {
    List<String> head = new ArrayList<>();
    for (int i = 0; i < Math.min(5, datasets.size()); i++) {
      Dataset d = datasets.get(i);
      head.add("#" + (i + 1) + " " + d.name() + " [" + className(query, d) + "]");
    }
    List<String> runs = new ArrayList<>();
    String run = null;
    int count = 0;
    for (Dataset d : datasets) {
      String c = className(query, d);
      if (c.equals(run)) {
        count++;
        continue;
      }
      if (run != null) {
        runs.add(run + " x" + count);
      }
      run = c;
      count = 1;
    }
    if (run != null) {
      runs.add(run + " x" + count);
    }
    return String.join(", ", head) + " | " + String.join(" > ", runs);
  }

  // ── Grading from the corpus definition ─────────────────────────────────────

  private static final String[] CLASSES = {
    "EXACT_NAME", "EXACT_COPIED", "PARTIAL_NAME", "PARTIAL_COPIED", "OTHER", "SEPARATOR_ONLY"
  };
  private static final int OTHER = 4;
  private static final int SEPARATOR_ONLY = 5;

  /** Words for a partial match: lowercased, accents folded, split on any separator. */
  static List<String> words(String text) {
    String folded =
        java.text.Normalizer.normalize(text, java.text.Normalizer.Form.NFD)
            .replaceAll("\\p{M}", "")
            .toLowerCase();
    return java.util.Arrays.stream(folded.split("[^a-z0-9]+")).filter(w -> !w.isEmpty()).toList();
  }

  /** The query as typed, without surrounding quotes. */
  static String typed(String query) {
    String q = query.trim();
    return q.length() > 1 && q.startsWith("\"") && q.endsWith("\"")
        ? q.substring(1, q.length() - 1)
        : q;
  }

  /** Exact: the whole value equals the query, ignoring case. */
  static boolean exact(String query, @Nullable String value) {
    return value != null && typed(query).equalsIgnoreCase(value.trim());
  }

  /** The same words as the query, differing only in separators: not part of the rule. */
  static boolean separatorOnly(String query, @Nullable String value) {
    return value != null && !exact(query, value) && words(typed(query)).equals(words(value));
  }

  /** Partial: every query word starts a word of the value, or the value starts with the query. */
  static boolean starts(String query, @Nullable String value) {
    if (value == null) {
      return false;
    }
    List<String> q = words(typed(query));
    List<String> v = words(value);
    return !q.isEmpty()
        && !v.isEmpty()
        && (q.stream().allMatch(w -> v.stream().anyMatch(x -> x.startsWith(w)))
            || String.join(" ", v).startsWith(String.join(" ", q)));
  }

  /** The best tranche the dataset reaches; its real name is graded before its copied title. */
  static int rank(String query, Dataset d) {
    if (exact(query, d.name())) {
      return 0;
    }
    if (separatorOnly(query, d.name())) {
      return SEPARATOR_ONLY;
    }
    if (exact(query, d.title())) {
      return 1;
    }
    if (separatorOnly(query, d.title())) {
      return SEPARATOR_ONLY;
    }
    if (starts(query, d.name())) {
      return 2;
    }
    if (starts(query, d.title())) {
      return 3;
    }
    return OTHER;
  }

  private String className(String query, Dataset d) {
    return d == null ? "UNKNOWN" : CLASSES[rank(query, d)];
  }

  // ── Search ─────────────────────────────────────────────────────────────────

  private List<Dataset> search(@Nullable String template, String query, int depth)
      throws Exception {
    OperationContext context =
        opContext
            .withSearchSurface(SearchTemplateSurfaces.SEARCH_ENTITIES)
            .withSearchFlags(
                flags -> {
                  SearchFlags f = flags.setFulltext(true).setSkipCache(true);
                  if (template != null) {
                    f.setSearchTemplate(UrnUtils.getUrn(template));
                  }
                  return f;
                });
    SearchEntityArray entities =
        searchService
            .search(
                context,
                List.of(DATASET_ENTITY_NAME),
                query,
                null,
                (List<SortCriterion>) null,
                0,
                depth)
            .getEntities();
    List<Dataset> out = new ArrayList<>();
    for (SearchEntity entity : entities) {
      String urn = entity.getEntity().toString();
      out.add(corpus.getOrDefault(urn, new Dataset(urn, urn, null)));
    }
    return out;
  }

  // ── Templates ──────────────────────────────────────────────────────────────

  /** Same body as the e2e seeder's class-order template, with the title named by its URN. */
  static String classOrderBody() throws Exception {
    String title = "{{urn:li:structuredProperty:" + DISPLAY_TITLE + "}}.text";
    Map<String, Object> ds = Map.of("term", Map.of("_entityType", "dataset"));
    Map<String, Object> exactName =
        Map.of("term", Map.of("name.lowercase", Map.of("value", "{{normalizedKeyword}}")));
    Map<String, Object> phraseName =
        Map.of("match_phrase", Map.of("name.text", Map.of("query", "{{alphanumericSplit}}")));
    Map<String, Object> titlePhrase =
        Map.of("match_phrase", Map.of(title, Map.of("query", "{{alphanumericSplit}}")));
    List<Object> tiers =
        List.of(
            tier(9000, "EXACT_NAME", List.of(ds, exactName), List.of(titlePhrase)),
            tier(8000, "EXACT_TITLE", List.of(ds, exactName, titlePhrase), null),
            tier(5500, "PHRASE_NAME", List.of(ds, phraseName), List.of(titlePhrase)),
            tier(5000, "PARTIAL_NAME", List.of(ds, partial("name.text")), List.of(partial(title))),
            tier(4000, "PARTIAL_TITLE", List.of(ds, partial(title)), null));
    Map<String, Object> body =
        Map.of(
            "bool",
            Map.of(
                "minimum_should_match",
                1,
                "should",
                List.of(
                    Map.of("dis_max", Map.of("tie_breaker", 0.0, "queries", tiers)),
                    Map.of(
                        "match",
                        Map.of(
                            "name.text",
                            Map.of("query", "{{alphanumericSplit}}", "boost", 0.001))))));
    return MAPPER.writeValueAsString(body);
  }

  private static Map<String, Object> partial(String field) {
    return Map.of(
        "bool",
        Map.of(
            "should",
            List.of(
                Map.of("match", Map.of(field, Map.of("query", "{{alphanumericSplit}}"))),
                Map.of(
                    "match_bool_prefix",
                    Map.of(field, Map.of("query", "{{alphanumericSplit}}"))))));
  }

  private static Map<String, Object> tier(
      int boost, String name, List<Object> filter, @Nullable List<Object> mustNot) {
    Map<String, Object> bool = new LinkedHashMap<>();
    bool.put("filter", filter);
    if (mustNot != null) {
      bool.put("must_not", mustNot);
    }
    return Map.of(
        "constant_score",
        Map.of("boost", boost, "_name", "class:" + name, "filter", Map.of("bool", bool)));
  }

  private static SearchTemplateProperties classOrderTemplate() throws Exception {
    return new SearchTemplateProperties()
        .setName("Class order")
        .setDescription("Strict match-class order in the engine score.")
        .setApiVersion(1)
        .setEditable(true)
        .setQuery(classOrderBody())
        .setFunctionScore(
            "{\"functions\":[{\"weight\":1.0}],\"score_mode\":\"multiply\",\"boost_mode\":\"multiply\"}")
        .setRoutes(new com.linkedin.search.template.SearchTemplateRouteArray());
  }

  // ── Index setup ────────────────────────────────────────────────────────────

  @BeforeClass
  public void indexCorpus() throws Exception {
    buildCorpus();
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
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(
            config,
            null,
            new SearchTemplateLoader(0, SearchTemplateSource.STORAGE),
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    IndexConvention indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
            new ConfiguredIndexPrefixResolver("rankingrequirements"),
            entityIndex);
    EntityRegistry entityRegistry = TestOperationContexts.defaultEntityRegistry();
    MappingsBuilder mappingsBuilder = createDelegatingMappingsBuilder(entityIndex);

    StructuredPropertyDefinition displayTitle =
        new StructuredPropertyDefinition()
            .setQualifiedName(DISPLAY_TITLE)
            .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"))
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
            .setSearchConfiguration(
                new DataHubSearchConfig()
                    .setQueryByDefault(true)
                    .setCopyTo(new StringArray(List.of("name"))));
    Map<Urn, List<RecordTemplate>> definitionAspects = new LinkedHashMap<>();
    for (String templateUrn : SearchTemplateBootstrap.urns()) {
      SearchTemplateProperties template = SearchTemplateBootstrap.get(templateUrn);
      if (template != null) {
        definitionAspects.put(UrnUtils.getUrn(templateUrn), List.of(template));
      }
    }
    definitionAspects.put(DISPLAY_TITLE_URN, List.of(displayTitle, new Status().setRemoved(false)));
    definitionAspects.put(UrnUtils.getUrn(CLASS_ORDER), List.of(classOrderTemplate()));
    List<Pair<Urn, StructuredPropertyDefinition>> structuredProperties =
        List.of(Pair.of(DISPLAY_TITLE_URN, displayTitle));

    Map<Urn, List<MCLItem>> events = new LinkedHashMap<>();
    for (Dataset d : corpus.values()) {
      Urn urn = UrnUtils.getUrn(d.urn());
      EntitySpec spec = entityRegistry.getEntitySpec(DATASET_ENTITY_NAME);
      List<MCLItem> items = new ArrayList<>();
      items.add(
          event(
              entityRegistry,
              urn,
              EntityKeyUtils.convertUrnToEntityKey(urn, spec.getKeyAspectSpec())));
      items.add(event(entityRegistry, urn, new DatasetProperties().setName(d.name())));
      if (d.title() != null) {
        items.add(
            event(
                entityRegistry,
                urn,
                new StructuredProperties()
                    .setProperties(
                        new StructuredPropertyValueAssignmentArray(
                            new StructuredPropertyValueAssignment()
                                .setPropertyUrn(DISPLAY_TITLE_URN)
                                .setValues(
                                    new PrimitivePropertyValueArray(
                                        PrimitivePropertyValue.create(d.title())))))));
      }
      events.put(urn, items);
    }

    MockAspectRetriever aspectRetriever =
        new StoredTemplateRetriever(
            definitionAspects, Map.of(UrnUtils.getUrn(CLASS_ORDER), classOrderTemplate()));
    aspectRetriever.setEntityRegistry(entityRegistry);
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .aspectRetriever(aspectRetriever)
            .cachingAspectRetriever(definitionsWithActiveUsers(aspectRetriever))
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(propertySearchRetriever(List.of(DISPLAY_TITLE_URN)))
            .build();
    opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            () -> retrieverContext,
            SearchContext.builder()
                .indexConvention(indexConvention)
                .searchClusterAccess(SearchClusterAccess.fixed(getSearchClient()))
                .searchTemplateRuntime(coordinator)
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
    Map<String, Map<String, Object>> mappings =
        mappingsBuilder.getIndexMappings(opContext, structuredProperties).stream()
            .collect(
                Collectors.toMap(
                    MappingsBuilder.IndexMapping::getIndexName,
                    MappingsBuilder.IndexMapping::getMappings));
    String indexName =
        indexConvention.getEntityIndexNameV3(
            opContext, V3IndexKeys.resolve(entityRegistry.getEntitySpec(DATASET_ENTITY_NAME)));
    indexBuilder.buildIndex(
        opContext,
        indexBuilder.buildReindexState(
            opContext,
            indexName,
            mappings.get(indexName),
            settingsBuilder.getSettings(config.getIndex(), indexName)));
    createdIndices.add(indexName);
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
    for (String index : createdIndices) {
      getSearchClient()
          .deleteIndex(
              OperationFingerprint.EMPTY, new DeleteIndexRequest(index), RequestOptions.DEFAULT);
    }
  }

  private static MCLItem event(EntityRegistry entityRegistry, Urn urn, RecordTemplate aspect) {
    EntitySpec entitySpec = entityRegistry.getEntitySpec(urn.getEntityType());
    AspectSpec aspectSpec =
        entitySpec.getAspectSpec(PegasusUtils.getAspectNameFromSchema(aspect.schema()));
    return TestMCL.builder()
        .urn(urn)
        .changeType(ChangeType.UPSERT)
        .metadataChangeLog(
            new MetadataChangeLog()
                .setSystemMetadata(
                    new SystemMetadata().setLastObserved(TIME).setRunId("ranking-requirements")))
        .entitySpec(entitySpec)
        .aspectSpec(aspectSpec)
        .recordTemplate(aspect)
        .auditStamp(new AuditStamp().setActor(ACTOR).setTime(TIME))
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
            Map.of("fulltext", slot("urn:li:searchTemplate:searchLight"))));
    return templates;
  }

  private static Map<String, Object> slot(String urn) {
    Map<String, Object> slot = new HashMap<>();
    slot.put("default", urn);
    return slot;
  }

  /**
   * The template loader reads a stored template as a system aspect with its record, which the plain
   * mock does not carry; this serves the class-order template the way storage would.
   */
  private static final class StoredTemplateRetriever extends MockAspectRetriever {
    private final Map<Urn, SearchTemplateProperties> templates;

    private StoredTemplateRetriever(
        Map<Urn, List<RecordTemplate>> data, Map<Urn, SearchTemplateProperties> templates) {
      super(data);
      this.templates = templates;
    }

    @Nonnull
    @Override
    public Map<Urn, Map<String, SystemAspect>> getLatestSystemAspects(
        @Nonnull OperationFingerprint context, Map<Urn, Set<String>> urnAspectNames) {
      Map<Urn, Map<String, SystemAspect>> found =
          new HashMap<>(super.getLatestSystemAspects(context, urnAspectNames));
      for (Urn urn : urnAspectNames.keySet()) {
        SearchTemplateProperties template = templates.get(urn);
        if (template != null) {
          AuditStamp stamp = new AuditStamp().setActor(ACTOR).setTime(TIME);
          found.put(
              urn,
              Map.of(
                  SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME,
                  TestSystemAspect.builder()
                      .urn(urn)
                      .recordTemplate(template)
                      .systemMetadata(new SystemMetadata().setAspectModified(stamp))
                      .auditStamp(stamp)
                      .build()));
        }
      }
      return found;
    }
  }

  /** The stored definitions, plus the empty active-users lookup that search reads. */
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
        Set<Urn> missing = new HashSet<>(urns);
        missing.removeAll(found.keySet());
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

  private static SearchRetriever propertySearchRetriever(List<Urn> propertyUrns) {
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
}

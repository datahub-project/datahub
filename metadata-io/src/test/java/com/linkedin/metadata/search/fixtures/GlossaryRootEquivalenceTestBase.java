package com.linkedin.metadata.search.fixtures;

import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.createDelegatingMappingsBuilder;
import static io.datahubproject.test.search.SearchTestUtils.syncAfterWrite;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.VersionTag;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GetRootGlossaryNodesResult;
import com.linkedin.datahub.graphql.generated.GetRootGlossaryTermsResult;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.datahub.graphql.resolvers.glossary.GetRootGlossaryNodesResolver;
import com.linkedin.datahub.graphql.resolvers.glossary.GetRootGlossaryTermsResolver;
import com.linkedin.datahub.graphql.resolvers.search.ScrollAcrossEntitiesResolver;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.config.EntityServiceConfiguration;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.config.cache.EntityDocCountCacheConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.cache.EntityDocCountCache;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.SearchWriteAccess;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.OpenSearchSearchClientShim;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.ranker.SimpleRanker;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.service.LifecycleStageTypeService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.metadata.utils.elasticsearch.ConfiguredIndexPrefixResolver;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.SearchTestUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Checks, against a real search index, that the two ways the UI lists the root of the business
 * glossary return the same entities.
 *
 * <ul>
 *   <li><b>Legacy:</b> the {@code getRootGlossaryTerms} and {@code getRootGlossaryNodes} queries
 *       ({@link GetRootGlossaryTermsResolver}, {@link GetRootGlossaryNodesResolver}), one {@code
 *       filter} call per entity type on {@code hasParentNode = false}. The glossary pickers and the
 *       home page hierarchy module still use them.
 *   <li><b>Filter-based search:</b> the glossary sidebar's {@code scrollAcrossEntities} request
 *       (built by {@code getGlossaryScrollInput.ts} in the web app), one scroll over both types
 *       with the same {@code hasParentNode = false} filter, sorted by type and name.
 * </ul>
 *
 * <p>Both sides run the production GraphQL resolvers over a {@link JavaEntityClient} backed by the
 * container's index. Documents are built with {@link SearchDocumentTransformer} from real aspects,
 * the way the index writer builds them, so field presence (e.g. no {@code hasParentNode} without an
 * info aspect) matches production.
 *
 * <p>Out of scope: search authorization. Both sides run with a system context that has search
 * access controls off, so this compares which documents count as roots, not who may see them.
 */
public abstract class GlossaryRootEquivalenceTestBase extends AbstractTestNGSpringContextTests {

  private static final String INDEX_PREFIX = "glossary_root_eq";

  /** Lifecycle stage configured as hidden from search, like the draft stage of versioned terms. */
  private static final Urn HIDDEN_STAGE = UrnUtils.getUrn("urn:li:lifecycleStageType:DRAFT");

  private static final Urn VISIBLE_STAGE = UrnUtils.getUrn("urn:li:lifecycleStageType:PUBLISHED");

  /**
   * Sidebar page sizes to scroll with, all smaller than the number of roots. A page size of 1 makes
   * every entity (including the unnamed ones) the search-after cursor of a page.
   */
  private static final int[] SCROLL_PAGE_SIZES = {1, 7};

  /** The web app requests up to 1000 roots per type from the legacy queries. */
  private static final int LEGACY_COUNT = 1000;

  private static final int GENERATED_ROOT_TERMS = 12;

  private static final AuditStamp AUDIT_STAMP =
      new AuditStamp()
          .setTime(1_700_000_000_000L)
          .setActor(UrnUtils.getUrn("urn:li:corpuser:glossary_root_eq"));

  @Nonnull
  protected abstract SearchClientShim<?> getSearchClient();

  @Nonnull
  protected abstract ESBulkProcessor getBulkProcessor();

  @Nonnull
  protected abstract ESIndexBuilder getIndexBuilder();

  @Nonnull
  protected abstract CustomSearchConfiguration getCustomSearchConfiguration();

  private OperationContext opContext;
  private ElasticSearchService entitySearchService;
  private EntityClient entityClient;
  private QueryContext queryContext;
  private LifecycleStageTypeService previousLifecycleStageTypeService;

  private final SearchDocumentTransformer transformer =
      new SearchDocumentTransformer(1000, 1000, 1000, false, ESUtils.KEYWORD_MAXLENGTH);

  /** Expected roots, kept as the data is seeded. */
  private final Set<String> expectedRootTerms = new TreeSet<>();

  private final Set<String> expectedRootNodes = new TreeSet<>();

  /** Roots in the hidden lifecycle stage: excluded by default, shown with the opt-in flag. */
  private final Set<String> hiddenStageRoots = new TreeSet<>();

  @BeforeClass
  public void setup() throws Exception {
    // Saved first so tearDown restores it even if the rest of setup fails.
    previousLifecycleStageTypeService = ESUtils.getLifecycleStageTypeService();
    IndexConvention indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
            new ConfiguredIndexPrefixResolver(INDEX_PREFIX),
            SearchTestUtils.DEFAULT_ENTITY_INDEX_CONFIGURATION);

    OperationContext systemContext = TestOperationContexts.systemContextNoSearchAuthorization();
    SearchContext searchContext =
        SearchContext.builder()
            .indexConvention(indexConvention)
            .searchableFieldTypes(
                ESUtils.buildSearchableFieldTypes(
                    systemContext.getEntityRegistry(),
                    createDelegatingMappingsBuilder(
                        SearchTestUtils.DEFAULT_ENTITY_INDEX_CONFIGURATION)))
            .searchableFieldPaths(
                ESUtils.buildSearchableFieldPaths(systemContext.getEntityRegistry()))
            .searchClusterAccess(SearchClusterAccess.fixed(getSearchClient()))
            .build();
    opContext =
        systemContext.toBuilder()
            .searchContext(searchContext)
            .build(systemContext.getSessionAuthentication(), true);

    entitySearchService = buildEntitySearchService();
    entitySearchService.reindexAll(opContext, Collections.emptySet());
    entitySearchService.clear(opContext);
    entityClient = buildEntityClient(buildSearchService(entitySearchService));

    queryContext = mock(QueryContext.class);
    when(queryContext.getOperationContext()).thenReturn(opContext);

    // Hidden-stage exclusion is resolved through this process-wide hook at query time.
    LifecycleStageTypeService lifecycleStageTypeService = mock(LifecycleStageTypeService.class);
    when(lifecycleStageTypeService.getHiddenStageUrns(anySet()))
        .thenReturn(Set.of(HIDDEN_STAGE.toString()));
    ESUtils.setLifecycleStageTypeService(lifecycleStageTypeService);

    seed();
    syncAfterWrite(getBulkProcessor());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    ESUtils.setLifecycleStageTypeService(previousLifecycleStageTypeService);
  }

  private void seed() throws Exception {
    // Root groups, a nested chain under the first, and a soft-deleted root group.
    Urn rootNodeA = node("root_a", "Finance", null);
    expectedRootNodes.add(rootNodeA.toString());
    Urn rootNodeB = node("root_b", "Engineering", null);
    expectedRootNodes.add(rootNodeB.toString());
    Urn childNode = node("child", "Accounting", rootNodeA);
    Urn grandchildNode = node("grandchild", "Payables", childNode);
    Urn deletedRootNode = node("deleted_root", "Retired", null, status(true, null));
    // A group whose info has no name: the sidebar sorts on the name, so it must not be dropped.
    expectedRootNodes.add(node("root_unnamed", null, null).toString());
    // A group whose parent was never created is still a child as far as the index knows.
    node("orphan", "Orphaned group", new GlossaryNodeUrn("does_not_exist"));
    // A group referenced elsewhere but never defined: key aspect only, no glossaryNodeInfo.
    index(UrnUtils.getUrn("urn:li:glossaryNode:undefined_node"));
    // Status aspects that are present but neutral must not change anything.
    expectedRootNodes.add(
        node("root_status_false", "Marketing", null, status(false, VISIBLE_STAGE)).toString());
    Urn hiddenRootNode = node("hidden_root", "Draft group", null, status(false, HIDDEN_STAGE));
    hiddenStageRoots.add(hiddenRootNode.toString());

    // Root terms, children at several depths, and soft-deleted terms at root and below.
    expectedRootTerms.add(term("root_revenue", "Revenue", null).toString());
    expectedRootTerms.add(term("root_unnamed", null, null).toString());
    term("child_ledger", "Ledger", rootNodeA);
    term("child_platform", "Platform", rootNodeB);
    term("nested_invoice", "Invoice", grandchildNode);
    term("deleted_root", "Old metric", null, status(true, null));
    term("deleted_child", "Old ledger", childNode, status(true, null));
    // A live term under a soft-deleted group is not a root: hasParentNode stays true.
    term("under_deleted_group", "Stranded", deletedRootNode);
    // A term referenced by assets but never defined (no glossaryTermInfo), see #20538.
    index(UrnUtils.getUrn("urn:li:glossaryTerm:undefined_term"));
    expectedRootTerms.add(
        term("root_status_false", "Churn", null, status(false, VISIBLE_STAGE)).toString());
    Urn hiddenRootTerm = term("hidden_root", "Draft term", null, status(false, HIDDEN_STAGE));
    hiddenStageRoots.add(hiddenRootTerm.toString());
    // Hidden children stay children either way.
    term("hidden_child", "Draft child", rootNodeB, status(false, HIDDEN_STAGE));

    // Versioned root term: only the latest version is listed.
    Urn versionSet = UrnUtils.getUrn("urn:li:versionSet:(glossary_root_eq_set,glossaryTerm)");
    term("versioned_v1", "Margin", null, version(versionSet, "1", false));
    expectedRootTerms.add(
        term("versioned_v2", "Margin", null, version(versionSet, "2", true)).toString());

    // Enough roots that the sidebar scroll needs several pages.
    for (int i = 0; i < GENERATED_ROOT_TERMS; i++) {
      expectedRootTerms.add(term(String.format("bulk_%02d", i), "Bulk " + i, null).toString());
    }
  }

  @Test
  public void legacyRootQueriesMatchExpectedRoots() throws Exception {
    assertEquals(legacyRootTerms(), expectedRootTerms);
    assertEquals(legacyRootNodes(), expectedRootNodes);
  }

  @Test
  public void sidebarScrollMatchesLegacyRootQueries() throws Exception {
    Set<String> legacy = new TreeSet<>(legacyRootTerms());
    legacy.addAll(legacyRootNodes());

    for (int pageSize : SCROLL_PAGE_SIZES) {
      List<String> sidebar = sidebarRoots(false, pageSize);

      assertEquals(
          sidebar.size(),
          new LinkedHashSet<>(sidebar).size(),
          "sidebar scroll returned duplicates across pages of " + pageSize + ": " + sidebar);
      assertEquals(new TreeSet<>(sidebar), legacy, "page size " + pageSize);
    }
  }

  @Test
  public void sidebarScrollListsGroupsBeforeTerms() throws Exception {
    List<String> sidebar = sidebarRoots(false, SCROLL_PAGE_SIZES[1]);
    int lastNode = -1;
    int firstTerm = sidebar.size();
    for (int i = 0; i < sidebar.size(); i++) {
      if (sidebar.get(i).startsWith("urn:li:glossaryNode:")) {
        lastNode = i;
      } else if (firstTerm == sidebar.size()) {
        firstTerm = i;
      }
    }
    assertTrue(lastNode >= 0 && firstTerm < sidebar.size(), "expected groups and terms");
    assertTrue(lastNode < firstTerm, "groups must precede terms: " + sidebar);
  }

  /**
   * Editors' sidebar opts in to hidden lifecycle stages; the legacy queries have no such option.
   * The difference is expected to be exactly the hidden-stage roots and nothing else.
   */
  @Test
  public void editorSidebarAddsOnlyHiddenStageRoots() throws Exception {
    Set<String> legacy = new TreeSet<>(legacyRootTerms());
    legacy.addAll(legacyRootNodes());

    Set<String> editorSidebar = new TreeSet<>(sidebarRoots(true, SCROLL_PAGE_SIZES[1]));

    Set<String> expected = new TreeSet<>(legacy);
    expected.addAll(hiddenStageRoots);
    assertEquals(editorSidebar, expected);
  }

  // --- the two implementations under test, called through their GraphQL resolvers ---

  private Set<String> legacyRootTerms() throws Exception {
    GetRootGlossaryTermsResult result =
        new GetRootGlossaryTermsResolver(entityClient).get(legacyEnvironment()).get();
    assertEquals(result.getTotal(), result.getTerms().size());
    return result.getTerms().stream().map(GlossaryTerm::getUrn).collect(Collectors.toSet());
  }

  private Set<String> legacyRootNodes() throws Exception {
    GetRootGlossaryNodesResult result =
        new GetRootGlossaryNodesResolver(entityClient).get(legacyEnvironment()).get();
    assertEquals(result.getTotal(), result.getNodes().size());
    return result.getNodes().stream().map(GlossaryNode::getUrn).collect(Collectors.toSet());
  }

  /** Pages through the sidebar's root scroll the way the web app does, in result order. */
  private List<String> sidebarRoots(boolean includeHiddenLifecycleStages, int pageSize)
      throws Exception {
    ScrollAcrossEntitiesResolver resolver = new ScrollAcrossEntitiesResolver(entityClient, null);
    List<String> urns = new ArrayList<>();
    String scrollId = null;
    int pages = 0;
    do {
      ScrollResults page =
          resolver.get(sidebarEnvironment(scrollId, includeHiddenLifecycleStages, pageSize)).get();
      page.getSearchResults().forEach(r -> urns.add(r.getEntity().getUrn()));
      scrollId = page.getNextScrollId();
      assertTrue(++pages <= 100, "sidebar scroll did not terminate");
    } while (scrollId != null);
    assertTrue(pages > 1, "fixture should need more than one sidebar page");
    return urns;
  }

  private DataFetchingEnvironment legacyEnvironment() {
    DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    when(env.getContext()).thenReturn(queryContext);
    when(env.getArgument("input")).thenReturn(Map.of("start", 0, "count", LEGACY_COUNT));
    return env;
  }

  /** Mirrors getGlossaryScrollInput({ parentNode: null }) in the web app. */
  private DataFetchingEnvironment sidebarEnvironment(
      @Nullable String scrollId, boolean includeHiddenLifecycleStages, int pageSize) {
    Map<String, Object> searchFlags = new HashMap<>();
    searchFlags.put("skipCache", true);
    if (includeHiddenLifecycleStages) {
      searchFlags.put("includeHiddenLifecycleStages", true);
    }
    Map<String, Object> input = new HashMap<>();
    input.put("scrollId", scrollId);
    input.put("query", "*");
    input.put("types", List.of("GLOSSARY_NODE", "GLOSSARY_TERM"));
    input.put(
        "orFilters",
        List.of(
            Map.of("and", List.of(Map.of("field", "hasParentNode", "values", List.of("false"))))));
    input.put("count", pageSize);
    input.put(
        "sortInput",
        Map.of(
            "sortCriteria",
            List.of(
                Map.of("field", "_index", "sortOrder", "ASCENDING"),
                Map.of("field", "_entityName", "sortOrder", "ASCENDING"))));
    input.put("searchFlags", searchFlags);

    DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    when(env.getContext()).thenReturn(queryContext);
    when(env.getArgument("input")).thenReturn(input);
    return env;
  }

  // --- seeding ---

  private Urn node(
      String id, @Nullable String name, @Nullable Urn parent, RecordTemplate... extraAspects)
      throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:glossaryNode:" + id);
    GlossaryNodeInfo info = new GlossaryNodeInfo().setDefinition("Definition of " + id);
    if (name != null) {
      info.setName(name);
    }
    if (parent != null) {
      info.setParentNode(GlossaryNodeUrn.createFromUrn(parent));
    }
    index(urn, prepend(info, extraAspects));
    return urn;
  }

  private Urn term(
      String id, @Nullable String name, @Nullable Urn parent, RecordTemplate... extraAspects)
      throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:glossaryTerm:" + id);
    GlossaryTermInfo info =
        new GlossaryTermInfo().setDefinition("Definition of " + id).setTermSource("INTERNAL");
    if (name != null) {
      info.setName(name);
    }
    if (parent != null) {
      info.setParentNode(GlossaryNodeUrn.createFromUrn(parent));
    }
    index(urn, prepend(info, extraAspects));
    return urn;
  }

  private static Status status(boolean removed, @Nullable Urn lifecycleStage) {
    Status status = new Status().setRemoved(removed);
    if (lifecycleStage != null) {
      status.setLifecycleStage(lifecycleStage);
    }
    return status;
  }

  private static VersionProperties version(Urn versionSet, String label, boolean isLatest) {
    return new VersionProperties()
        .setVersionSet(versionSet)
        .setVersion(new VersionTag().setVersionTag(label))
        .setSortId(label)
        .setIsLatest(isLatest);
  }

  private static RecordTemplate[] prepend(RecordTemplate first, RecordTemplate[] rest) {
    RecordTemplate[] all = new RecordTemplate[rest.length + 1];
    all[0] = first;
    System.arraycopy(rest, 0, all, 1, rest.length);
    return all;
  }

  /** Indexes the key aspect plus the given aspects as one merged document, like the writer. */
  private void index(Urn urn, RecordTemplate... aspects) throws Exception {
    EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(urn.getEntityType());
    AspectSpec keySpec = entitySpec.getKeyAspectSpec();
    ObjectNode document =
        transformer
            .transformAspect(
                opContext,
                urn,
                EntityKeyUtils.convertUrnToEntityKey(urn, keySpec),
                keySpec,
                false,
                AUDIT_STAMP)
            .orElseThrow();
    for (RecordTemplate aspect : aspects) {
      AspectSpec aspectSpec =
          entitySpec.getAspectSpec(PegasusUtils.getAspectNameFromSchema(aspect.schema()));
      assertNotNull(aspectSpec, "aspect not registered for " + urn.getEntityType());
      transformer
          .transformAspect(opContext, urn, aspect, aspectSpec, false, AUDIT_STAMP)
          .ifPresent(document::setAll);
    }
    entitySearchService.upsertDocument(
        opContext,
        urn.getEntityType(),
        document.toString(),
        opContext.getSearchContext().getIndexConvention().getEntityDocumentId(urn));
  }

  // --- wiring, following SampleDataFixtureConfiguration ---

  private ElasticSearchService buildEntitySearchService() {
    IndexConvention mockIndexConvention = mock(IndexConvention.class);
    when(mockIndexConvention.isV2EntityIndexType(anyString())).thenReturn(true);
    ESSearchDAO searchDAO =
        new ESSearchDAO(
            false,
            TEST_OS_SEARCH_CONFIG,
            getCustomSearchConfiguration(),
            QueryFilterRewriteChain.EMPTY,
            false,
            TEST_SEARCH_SERVICE_CONFIG);
    ESBrowseDAO browseDAO =
        new ESBrowseDAO(
            TEST_OS_SEARCH_CONFIG,
            getCustomSearchConfiguration(),
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
    ESWriteDAO writeDAO =
        new ESWriteDAO(
            TEST_OS_SEARCH_CONFIG,
            getSearchClient(),
            getBulkProcessor(),
            SearchWriteAccess.fixed(getBulkProcessor()));
    return new ElasticSearchService(
        getIndexBuilder(),
        TEST_SEARCH_SERVICE_CONFIG,
        TEST_ES_SEARCH_CONFIG,
        new V2MappingsBuilder(
            TEST_ES_SEARCH_CONFIG.getEntityIndex(),
            OpenSearchSearchClientShim.PARTIAL_NGRAM_CONFIG,
            ESUtils.KEYWORD_MAXLENGTH),
        new V2LegacySettingsBuilder(TEST_ES_SEARCH_CONFIG.getIndex(), mockIndexConvention, false),
        searchDAO,
        browseDAO,
        writeDAO);
  }

  private SearchService buildSearchService(ElasticSearchService entitySearchService) {
    EntityDocCountCacheConfiguration docCountConfig = new EntityDocCountCacheConfiguration();
    docCountConfig.setTtlSeconds(600L);
    return new SearchService(
        new EntityDocCountCache(opContext.getEntityRegistry(), entitySearchService, docCountConfig),
        new CachingEntitySearchService(
            new ConcurrentMapCacheManager(), entitySearchService, 100, false),
        new SimpleRanker(),
        TEST_SEARCH_SERVICE_CONFIG);
  }

  /**
   * Every URN "exists" in the primary store, so result validation keeps every hit and the
   * comparison is purely about what the index returns.
   */
  private EntityClient buildEntityClient(SearchService searchService) {
    AspectDao aspectDao = mock(AspectDao.class);
    when(aspectDao.batchGet(any(OperationContext.class), anySet(), anyBoolean()))
        .thenAnswer(
            args -> {
              Set<EntityAspectIdentifier> ids = args.getArgument(1);
              return ids.stream()
                  .collect(
                      Collectors.toMap(
                          id -> id,
                          id -> {
                            EntityAspect aspect = mock(EntityAspect.class);
                            when(aspect.getUrn()).thenReturn(id.getUrn());
                            when(aspect.getAspect()).thenReturn(id.getAspect());
                            when(aspect.getVersion()).thenReturn(id.getVersion());
                            return aspect;
                          }));
            });
    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    return new JavaEntityClient(
        new EntityServiceImpl(
            aspectDao,
            null,
            preProcessHooks,
            new EntityServiceConfiguration().setAlwaysEmitChangeLog(true).setEnableBrowseV2(true),
            mock(MetricUtils.class)),
        null,
        entitySearchService,
        new CachingEntitySearchService(
            new ConcurrentMapCacheManager(), entitySearchService, 1, false),
        searchService,
        null,
        null,
        null,
        null,
        EntityClientConfig.builder().batchGetV2Size(1).build(),
        null);
  }
}

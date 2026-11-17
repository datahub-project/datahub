package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_FULLTEXT_URN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.FloatMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.validation.SearchTemplateValidator;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.search.template.SearchTemplateProfiles;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRetrieval;
import com.linkedin.search.template.SearchTemplateRetrievalAnchor;
import com.linkedin.search.template.SearchTemplateRoute;
import com.linkedin.search.template.SearchTemplateRouteMatch;
import com.linkedin.search.template.SearchTemplateScope;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import org.testng.annotations.Test;

/**
 * The semantic template contract: stored fields, write-time rules, and the keywordRerank reader.
 */
public class SearchTemplateSemanticContractTest extends SearchTemplateFixture {
  private static final String RERANK = "urn:li:searchTemplate:rerank";
  private static final String BLEND =
      "{ \"vectorWeight\": 0.4, \"others\": \"keepKeywordPlaces\" }";
  private static final String LEGACY = "{ \"field\": \"embeddings.vector\", \"k\": 10 }";

  @Test
  public void testKeywordRerankBindsWithoutDocumentRerankAndKeepsKeywordOrder() {
    store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", "{ \"match_all\": {} }", null));
    SearchTemplateProperties rerank = parentOf("Rerank", SEARCH_TEMPLATE_FULLTEXT_URN);
    rerank.setApiVersion(2);
    rerank.setRetrieval(retrieval(r -> r.setCombine("keywordRerank")));
    rerank.setKnn(BLEND);
    store(RERANK, rerank);
    templates.getDefaults().put("fulltext", Map.of("default", RERANK));
    for (boolean semanticRead : List.of(false, true)) {
      SearchTemplateCoordinator coordinator = coordinator(semanticRead);
      for (String entity : List.of("dataset", "document")) {
        OperationContext bound =
            coordinator.bind(opContext, "customer orders", false, List.of(entity));
        ResolvedSearchTemplate resolved = bound.getSearchContext().getResolvedSearchTemplate();
        assertEquals(resolved.getUrn(), RERANK);
        coordinator.armServed(bound, RERANK);
        assertFalse(resolved.isKnnApplied(), semanticRead + " " + entity);
        assertTrue(resolved.isTemplateRerank(), semanticRead + " " + entity);
      }
    }

    // The earlier shape keeps today's check: without document rerank the search fails.
    SearchTemplateProperties legacy = parentOf("Legacy", SEARCH_TEMPLATE_FULLTEXT_URN);
    legacy.setKnn(LEGACY);
    store("urn:li:searchTemplate:legacy", legacy);
    templates.getDefaults().put("fulltext", Map.of("default", "urn:li:searchTemplate:legacy"));
    assertThrows(
        SearchTemplateException.class,
        () -> coordinator(false).bind(opContext, "customer orders", false, List.of("dataset")));
  }

  @Test
  public void testKnnShapeFollowsTheEffectiveCombine() {
    SearchTemplateProperties blend = rung(r -> r.setCombine("keywordRerank"));
    blend.setKnn(BLEND);
    SearchTemplateChecks.check(blend, 1024);

    SearchTemplateProperties keyword = rung(r -> r.setMode("keyword"));
    keyword.setKnn(BLEND);
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(keyword, 1024));

    SearchTemplateProperties malformedLegacy = body("Legacy", "{ \"match_all\": {} }", null);
    malformedLegacy.setKnn("{ \"k\": 10 }");
    store("urn:li:searchTemplate:malformedLegacy", malformedLegacy);
    assertThrows(
        SearchTemplateException.class,
        () ->
            SearchTemplateChecks.checkMerged(
                loader.load(opContext, "urn:li:searchTemplate:malformedLegacy"), 1024));

    // A child that sets only the blend inherits the combine. One aspect is checked as JSON, and
    // the merged template applies the parser.
    SearchTemplateProperties parent = rung(r -> r.setCombine("keywordRerank"));
    store("urn:li:searchTemplate:rerankParent", parent);
    SearchTemplateProperties child = parentOf("Child", "urn:li:searchTemplate:rerankParent");
    child.setKnn(BLEND);
    SearchTemplateChecks.check(child, 1024);
    store("urn:li:searchTemplate:rerankChild", child);
    SearchTemplateChecks.checkMerged(
        loader.load(opContext, "urn:li:searchTemplate:rerankChild"), 1024);

    for (String bad :
        List.of(
            "{ \"vectorWeight\": 1.5 }",
            "{ \"blendFloor\": -0.1 }",
            "{ \"others\": \"drop\" }",
            "{ \"lexical\": { \"inputMin\": 5, \"inputMax\": 5 } }",
            "{ \"lexical\": { \"steepness\": 0 } }")) {
      SearchTemplateProperties invalid = rung(r -> r.setCombine("keywordRerank"));
      invalid.setKnn(bad);
      assertRejects(bad, () -> SearchTemplateChecks.check(invalid, 1024));
    }
  }

  @Test
  public void testRetrievalRulesRejectOnlyWhatNoBuildCanRun() {
    // Valid to store: later combines, several profiles, vectorBody, and similarity.
    SearchTemplateChecks.check(rung(r -> r.setCombine("vectorThenKeyword")), 1024);
    SearchTemplateChecks.check(
        rung(r -> r.setCombine("vector").setProfileScore("weightedSum")), 1024);
    SearchTemplateChecks.check(rung(r -> r.setCombine("vector").setVectorBody("{}")), 1024);
    SearchTemplateChecks.check(similarity(), 1024);
    SearchTemplateProperties profiles = rung(r -> r.setCombine("vector"));
    profiles.setProfiles(
        new SearchTemplateProfiles().setReplace(new StringArray(List.of("a", "b"))));
    SearchTemplateChecks.check(profiles, 1024);

    List<Consumer<SearchTemplateRetrieval>> invalid =
        List.of(
            r -> r.setCombine("hybrid"),
            r -> r.setMode("vector").setCombine("keywordRerank"),
            r -> r.setCombine("keywordRerank").setMinSimilarity(0.5f),
            r -> r.setMode("keyword").setMinSimilarity(0.5f),
            r -> r.setCombine("vector").setMinSimilarity(1.5f),
            r -> r.setCombine("vector").setProfileScore("weightedSum").setMinSimilarity(0.5f),
            r -> r.setCombine("keywordRerank").setQuerySource("anchorEntity"),
            r -> r.setMode("keyword").setQuerySource("text"),
            r -> r.setMode("keyword").setVectorBody("{}"),
            r -> r.setCombine("vector").setProfileScore("best").setVectorBody("{}"),
            r -> r.setCombine("vector").setAnchor(new SearchTemplateRetrievalAnchor()));
    for (Consumer<SearchTemplateRetrieval> rule : invalid) {
      SearchTemplateProperties properties = rung(rule);
      assertRejects(
          properties.getRetrieval().toString(), () -> SearchTemplateChecks.check(properties, 1024));
    }
  }

  @Test
  public void testMergedSectionsRules() {
    SearchTemplateProperties similarityKnn = similarity();
    similarityKnn.setKnn(BLEND);
    assertMergedRejects("similarityKnn", similarityKnn);

    SearchTemplateProperties similarityNext = similarity();
    similarityNext.setLadder(stopLadder(1, SEARCH_TEMPLATE_FULLTEXT_URN));
    assertMergedRejects("similarityNext", similarityNext);

    SearchTemplateProperties keywordProfiles = body("Keyword", "{ \"match_all\": {} }", null);
    keywordProfiles.setApiVersion(2);
    keywordProfiles.setProfiles(
        new SearchTemplateProfiles().setReplace(new StringArray(List.of("a"))));
    assertMergedRejects("keywordProfiles", keywordProfiles);

    // A native mixed query cites the placeholders, so it is valid to store and fails at bind.
    SearchTemplateProperties nativeQuery =
        body("Native", "{ \"bool\": { \"should\": [ {{&embeddingClausesJson}} ] } }", null);
    nativeQuery.setApiVersion(2);
    nativeQuery.setProfiles(new SearchTemplateProfiles().setReplace(new StringArray(List.of("a"))));
    store("urn:li:searchTemplate:nativeQuery", nativeQuery);
    MergedSearchTemplate nativeMerged = loader.load(opContext, "urn:li:searchTemplate:nativeQuery");
    SearchTemplateChecks.checkMerged(nativeMerged, 1024);
    assertEquals(rejection(nativeMerged), "nativeQuery");

    SearchTemplateProperties scorePlaceholder =
        body("Native", "{ \"match_all\": {} }", "{ \"functions\": [ {{&embeddingClausesJson}} ] }");
    assertRejects("scorePlaceholder", () -> SearchTemplateChecks.check(scorePlaceholder, 1024));

    SearchTemplateProperties placeholderCombine =
        body("Native", "{ \"bool\": { \"should\": [ {{&embeddingClausesJson}} ] } }", null);
    placeholderCombine.setApiVersion(2);
    placeholderCombine.setRetrieval(retrieval(r -> r.setCombine("keywordRerank")));
    assertMergedRejects("placeholderCombine", placeholderCombine);
  }

  @Test
  public void testDeferredSectionsFailClosedAtBind() {
    assertEquals(rejectionOf("vector", rung(r -> r.setCombine("vector"))), "retrievalCombine");
    assertEquals(
        rejectionOf("vectorBody", rung(r -> r.setCombine("keywordRerank").setVectorBody("{}"))),
        "vectorBody");
    SearchTemplateProperties scoped = body("Scoped", "{ \"match_all\": {} }", null);
    scoped.setApiVersion(2);
    scoped.setScope(new SearchTemplateScope().setEntityTypes(new StringArray(List.of("document"))));
    assertEquals(rejectionOf("scoped", scoped), "scope");
    assertNull(rejectionOf("keywordRerank", rung(r -> r.setCombine("keywordRerank"))));
    assertEquals(
        rejectionOf(
            "rerankScore", rung(r -> r.setCombine("keywordRerank").setProfileScore("best"))),
        "profileScore");
    SearchTemplateProperties nativeSibling =
        body(
            "Native",
            "{ \"bool\": { \"should\": [ { \"match_all\": {} }, {{&embeddingClausesJson}} ] } }",
            null);
    nativeSibling.setApiVersion(2);
    SearchTemplateChecks.check(nativeSibling, 1024);

    SearchTemplateProperties router = body("Router", "{ \"match_all\": {} }", null);
    router.setApiVersion(2);
    SearchTemplateRoute conditional = route("^urn:.*", SEARCH_TEMPLATE_FULLTEXT_URN);
    conditional.setMatch(new SearchTemplateRouteMatch().setSemanticEligible(true));
    router.setRoutes(routes(conditional, route("^orders$", SEARCH_TEMPLATE_FULLTEXT_URN)));
    store("urn:li:searchTemplate:router", router);
    MergedSearchTemplate routes = loader.load(opContext, "urn:li:searchTemplate:router");
    assertEquals(
        SearchTemplateRoutes.decide(routes, "orders").target(), SEARCH_TEMPLATE_FULLTEXT_URN);
    assertThrows(
        SearchTemplateException.class, () -> SearchTemplateRoutes.decide(routes, "urn:li:x"));
  }

  @Test
  public void testProfilesAndScopeMergeDownTheChain() {
    SearchTemplateProperties root = body("Root", "{ \"match_all\": {} }", null);
    root.setApiVersion(2);
    root.setRetrieval(retrieval(r -> r.setCombine("vector")));
    root.setProfiles(
        new SearchTemplateProfiles()
            .setReplace(new StringArray(List.of("a")))
            .setOverride(new FloatMap(Map.of("a", 2f))));
    root.setScope(new SearchTemplateScope().setEntityTypes(new StringArray(List.of("document"))));
    store("urn:li:searchTemplate:root", root);
    SearchTemplateProperties child = parentOf("Child", "urn:li:searchTemplate:root");
    child.setProfiles(new SearchTemplateProfiles().setAdd(new StringArray(List.of("b"))));
    child.setScope(new SearchTemplateScope());
    store("urn:li:searchTemplate:child", child);

    MergedSearchTemplate merged = loader.load(opContext, "urn:li:searchTemplate:child");
    assertEquals(merged.getProfiles().getReplace(), List.of("a", "b"));
    assertEquals(merged.getProfiles().getOverride().get("a"), 2f);
    assertNull(merged.getScope());
    assertEquals(
        loader.load(opContext, "urn:li:searchTemplate:root").getScope().getEntityTypes(),
        List.of("document"));

    SearchTemplateProperties keep = parentOf("Keep", "urn:li:searchTemplate:child");
    keep.setProfiles(new SearchTemplateProfiles().setAdd(new StringArray()));
    store("urn:li:searchTemplate:keep", keep);
    assertEquals(
        loader.load(opContext, "urn:li:searchTemplate:keep").getProfiles().getReplace(),
        List.of("a", "b"));

    SearchTemplateProperties router = body("Router", null, null);
    router.setDefaultRoute(defaultTo(SEARCH_TEMPLATE_FULLTEXT_URN));
    router.setScope(new SearchTemplateScope().setEntityTypes(new StringArray(List.of("x"))));
    assertRejects("routerScope", () -> SearchTemplateChecks.check(router, 1024));

    SearchTemplateProperties future = parentOf("Future", "urn:li:searchTemplate:child");
    SearchTemplateScope futureScope = new SearchTemplateScope();
    futureScope.data().put("futureCondition", true);
    future.setScope(futureScope);
    store("urn:li:searchTemplate:future", future);
    assertThrows(
        SearchTemplateException.class,
        () -> loader.load(opContext, "urn:li:searchTemplate:future"));

    SearchTemplateProperties reset = parentOf("Reset", "urn:li:searchTemplate:child");
    reset.setProfiles(new SearchTemplateProfiles().setUseDefault(true));
    store("urn:li:searchTemplate:reset", reset);
    assertNull(loader.load(opContext, "urn:li:searchTemplate:reset").getProfiles());

    for (SearchTemplateProfiles bad :
        List.of(
            new SearchTemplateProfiles().setReplace(new StringArray()),
            new SearchTemplateProfiles().setUseDefault(true).setAdd(new StringArray(List.of("a"))),
            new SearchTemplateProfiles()
                .setAdd(new StringArray(List.of("a")))
                .setRemove(new StringArray(List.of("a"))),
            new SearchTemplateProfiles().setOverride(new FloatMap(Map.of("a", 0f))))) {
      SearchTemplateProperties properties = rung(r -> r.setCombine("vector"));
      properties.setProfiles(bad);
      assertRejects(bad.toString(), () -> SearchTemplateChecks.check(properties, 1024));
    }
  }

  @Test
  public void testSemanticFieldsRequireTheSemanticApiVersion() {
    SearchTemplateProperties earlier = rung(r -> r.setCombine("keywordRerank"));
    earlier.setApiVersion(1);
    assertRejects("earlierContract", () -> SearchTemplateChecks.checkMerged(merged(earlier), 1024));
    store("urn:li:searchTemplate:earlierContract", earlier);
    assertThrows(
        SearchTemplateException.class,
        () -> loader.load(opContext, "urn:li:searchTemplate:earlierContract"));
  }

  @Test
  public void testStampedChildKeepsTheParentContract() {
    store("urn:li:searchTemplate:semanticParent", rung(r -> r.setCombine("keywordRerank")));
    SearchTemplateProperties child = parentOf("Child", "urn:li:searchTemplate:semanticParent");
    child.setApiVersion(1);
    store("urn:li:searchTemplate:stampedChild", child);
    assertEquals(loader.load(opContext, "urn:li:searchTemplate:stampedChild").getApiVersion(), 2);

    for (String tag :
        List.of(
            "{{^embeddingClausesJson}}{}{{/embeddingClausesJson}}",
            "{{#embeddingField}}{{/embeddingField}}",
            "{{{embeddingClausesJson}}}")) {
      assertTrue(SearchTemplateCapabilities.citesEmbeddingPlaceholders(tag), tag);
    }

    SearchTemplateProperties nativeEarlier =
        body("Native", "{ \"bool\": { \"should\": [ {{&embeddingClausesJson}} ] } }", null);
    store("urn:li:searchTemplate:nativeEarlier", nativeEarlier);
    assertThrows(
        SearchTemplateException.class,
        () -> loader.load(opContext, "urn:li:searchTemplate:nativeEarlier"));

    SearchTemplateProperties patched = rung(r -> r.setCombine("vector"));
    patched.setProfiles(
        new SearchTemplateProfiles().setUseDefault(true).setAdd(new StringArray(List.of("a"))));
    store("urn:li:searchTemplate:patched", patched);
    assertThrows(
        SearchTemplateException.class,
        () -> loader.load(opContext, "urn:li:searchTemplate:patched"));
  }

  private static MergedSearchTemplate merged(SearchTemplateProperties properties) {
    return MergedSearchTemplate.builder()
        .urn("urn:li:searchTemplate:mergedSemantic")
        .dialect("ELASTICSEARCH")
        .apiVersion(properties.getApiVersion())
        .query(properties.getQuery())
        .retrieval(properties.getRetrieval())
        .queryFragments(Map.of())
        .parameters(Map.of())
        .chain(List.of("urn:li:searchTemplate:mergedSemantic"))
        .build();
  }

  @Test
  public void testParentWriteRevalidatesInheritingChildren() {
    SearchTemplateProperties parent = rung(r -> r.setCombine("keywordRerank"));
    store("urn:li:searchTemplate:writeParent", parent);
    SearchTemplateProperties child = parentOf("Child", "urn:li:searchTemplate:writeParent");
    child.setKnn(BLEND);
    store("urn:li:searchTemplate:writeChild", child);
    SearchTemplateValidator validator = templateValidator();

    assertTrue(
        validate(
                validator,
                holeContext(),
                item(
                    UrnUtils.getUrn("urn:li:searchTemplate:writeParent"),
                    SYSTEM,
                    ChangeType.UPSERT,
                    rung(r -> r.setCombine("keywordRerank"))))
            .isEmpty());
    assertFalse(
        validate(
                validator,
                holeContext(),
                item(
                    UrnUtils.getUrn("urn:li:searchTemplate:writeParent"),
                    SYSTEM,
                    ChangeType.UPSERT,
                    body("Keyword", "{ \"match_all\": {} }", null)))
            .isEmpty());
  }

  @Test
  public void testNewSectionsSurviveTheStoredJson() {
    SearchTemplateProperties properties = similarity();
    properties.setProfiles(new SearchTemplateProfiles().setAdd(new StringArray(List.of("a"))));
    properties.setScope(new SearchTemplateScope().setEntityTypes(new StringArray(List.of("x"))));
    SearchTemplateRoute route = route("^urn:.*", SEARCH_TEMPLATE_FULLTEXT_URN);
    route.setMatch(
        new SearchTemplateRouteMatch().setSurfaces(new StringArray(List.of("semanticSearch"))));
    properties.setRoutes(routes(route));
    SearchTemplateProperties read =
        RecordUtils.toRecordTemplate(
            SearchTemplateProperties.class, RecordUtils.toJsonString(properties));
    assertEquals(read.getRetrieval().getCombine(), "vector");
    assertEquals(read.getRetrieval().getQuerySource(), "anchorEntity");
    assertEquals(read.getRetrieval().getMinSimilarity(), 0.8f);
    assertEquals(read.getRetrieval().getAnchor().getUrnParameter(), "raw");
    assertTrue(read.getRetrieval().getAnchor().isExcludeSelf());
    assertEquals(read.getProfiles().getAdd(), List.of("a"));
    assertEquals(read.getScope().getEntityTypes(), List.of("x"));
    assertEquals(read.getRoutes().get(0).getMatch().getSurfaces(), List.of("semanticSearch"));
  }

  private SearchTemplateCoordinator coordinator(boolean semanticRead) {
    ElasticSearchConfiguration elastic = v3Config();
    EntityIndexVersionConfiguration indexV3 = elastic.getEntityIndex().getV3();
    indexV3.setSemanticReadEnabled(semanticRead);
    return new SearchTemplateCoordinator(
        elastic, null, loader, ForkJoinPool.commonPool(), SearchTemplateComponentRegistry.empty());
  }

  private void assertMergedRejects(String name, SearchTemplateProperties properties) {
    String urn = "urn:li:searchTemplate:" + name;
    store(urn, properties);
    assertRejects(name, () -> SearchTemplateChecks.checkMerged(loader.load(opContext, urn), 1024));
  }

  private static void assertRejects(String name, Runnable check) {
    try {
      check.run();
    } catch (SearchTemplateException e) {
      return;
    }
    fail("Expected the search template to be rejected: " + name);
  }

  private String rejectionOf(String name, SearchTemplateProperties properties) {
    String urn = "urn:li:searchTemplate:" + name;
    store(urn, properties);
    return rejection(loader.load(opContext, urn));
  }

  private static String rejection(MergedSearchTemplate template) {
    return SearchTemplateCapabilities.rejectionCode(
        template, false, false, SearchTemplateComponentRegistry.empty());
  }

  private static SearchTemplateProperties similarity() {
    SearchTemplateProperties properties =
        rung(
            r ->
                r.setCombine("vector")
                    .setQuerySource("anchorEntity")
                    .setCandidateCap(500)
                    .setMinSimilarity(0.8f)
                    .setAnchor(
                        new SearchTemplateRetrievalAnchor()
                            .setUrnParameter("raw")
                            .setExcludeSelf(true)));
    return properties;
  }

  private static SearchTemplateProperties rung(Consumer<SearchTemplateRetrieval> setup) {
    SearchTemplateProperties properties = body("Rung", "{ \"match_all\": {} }", null);
    properties.setApiVersion(2);
    properties.setRetrieval(retrieval(setup));
    return properties;
  }

  private static SearchTemplateRetrieval retrieval(Consumer<SearchTemplateRetrieval> setup) {
    SearchTemplateRetrieval retrieval = new SearchTemplateRetrieval();
    setup.accept(retrieval);
    return retrieval;
  }
}

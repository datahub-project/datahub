package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_FULLTEXT_URN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.FloatMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.search.template.SearchTemplateProfiles;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRanking;
import com.linkedin.search.template.SearchTemplateRetrieval;
import com.linkedin.search.template.SearchTemplateRoute;
import com.linkedin.search.template.SearchTemplateRouteMatch;
import com.linkedin.search.template.SearchTemplateScope;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.testng.annotations.Test;

/** Write and load rules of the semantic template contract that the bind tests do not reach. */
public class SearchTemplateSemanticRulesTest extends SearchTemplateFixture {

  @Test
  public void testProfilesRejectSelectionsNoBuildCanRun() {
    for (SearchTemplateProfiles bad :
        List.of(
            new SearchTemplateProfiles().setUseDefault(false),
            new SearchTemplateProfiles().setUseDefault(true).setOverride(weights("a", 1f)),
            new SearchTemplateProfiles().setReplace(ids("a", "a")),
            new SearchTemplateProfiles().setAdd(ids(" ")),
            new SearchTemplateProfiles().setOverride(weights(" ", 1f)),
            new SearchTemplateProfiles().setOverride(weights("a", Float.NaN)))) {
      assertThrows(SearchTemplateException.class, () -> SearchTemplateProfilesPolicy.check(bad));
    }
  }

  @Test
  public void testPatchesWithoutReplaceMergeOntoTheEntityDefault() {
    SearchTemplateProfiles parent =
        new SearchTemplateProfiles().setAdd(ids("a")).setRemove(ids("d"));
    SearchTemplateProfiles merged =
        SearchTemplateProfilesPolicy.merge(
            parent, new SearchTemplateProfiles().setAdd(ids("b")).setRemove(ids("a")));
    assertNull(merged.getReplace());
    assertEquals(merged.getAdd(), List.of("b"));
    assertEquals(merged.getRemove(), List.of("d", "a"));
    // A child with no operation keeps the parent's selection.
    assertSame(SearchTemplateProfilesPolicy.merge(parent, new SearchTemplateProfiles()), parent);
  }

  @Test
  public void testRerankBlendRejectsMalformedSections() {
    for (String bad :
        List.of(
            "[]",
            "{ \"k\": 0 }",
            "{ \"lexical\": 1 }",
            "{ \"model\": 5 }",
            "{ \"vectorWeight\": \"high\" }",
            "{ \"vectorWeight\": 2 }",
            "{ \"blendFloor\": 2 }",
            "{ \"others\": \"dropThem\" }",
            "{ \"lexical\": { \"inputMin\": 5, \"inputMax\": 1 } }",
            "{ \"lexical\": { \"steepness\": 0 } }")) {
      assertThrows(SearchTemplateException.class, () -> SearchTemplateJson.parseRerankKnn(bad));
    }
    SearchTemplateRerankKnn defaults = SearchTemplateJson.parseRerankKnn("{}");
    assertEquals(defaults.others(), SearchTemplateRerankKnn.KEEP_KEYWORD_PLACES);
    assertEquals(defaults.vectorWeight(), SearchTemplateRerankKnn.DEFAULT_VECTOR_WEIGHT);
    assertNull(defaults.blendFloor());
    assertThrows(
        SearchTemplateException.class, () -> SearchTemplateJson.requireObject("[]", "knn"));
  }

  @Test
  public void testScopeAndRouteMatchNamesAreUniqueAndNonBlank() {
    SearchTemplateProperties scoped = rung(r -> r.setCombine("vector"));
    scoped.setScope(new SearchTemplateScope().setEntityTypes(ids("document", "document")));
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(scoped, 1024));

    assertRouteMatch(new SearchTemplateRouteMatch().setEntityTypes(ids("dataset")), true);
    assertRouteMatch(
        new SearchTemplateRouteMatch().setEntityTypes(ids("dataset", "dataset")), false);
    assertRouteMatch(new SearchTemplateRouteMatch().setSurfaces(ids(" ")), false);
  }

  @Test
  public void testReservedFragmentNameAndRankingNeedKeywordRetrieval() {
    SearchTemplateProperties fragments = body("Native", "{ \"match_all\": {} }", null);
    fragments.setQueryFragments(
        new StringMap(Map.of(SearchTemplateHoles.EMBEDDING_CLAUSES_JSON, "{ \"match_all\": {} }")));
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(fragments, 1024));

    SearchTemplateProperties ranked = body("Ranked", "{ \"match_all\": {} }", null);
    ranked.setCapabilities(new StringArray(List.of("rankingSignals")));
    ranked.setRanking(new SearchTemplateRanking().setTemplate(SEARCH_TEMPLATE_FULLTEXT_URN));
    SearchTemplateChecks.check(ranked, 1024);
    ranked.setApiVersion(2);
    ranked.setRetrieval(retrieval(r -> r.setCombine("vector")));
    assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(ranked, 1024));
  }

  @Test
  public void testEveryDeferredShapeFailsTheSelectedTemplate() {
    SearchTemplateProperties scoped = rung(r -> r.setCombine("keywordRerank"));
    scoped.setScope(new SearchTemplateScope().setEntityTypes(ids("document")));
    SearchTemplateProperties keywordProfiles = body("Keyword", "{ \"match_all\": {} }", null);
    keywordProfiles.setApiVersion(2);
    keywordProfiles.setProfiles(new SearchTemplateProfiles().setReplace(ids("a")));
    SearchTemplateProperties keywordScore = body("Keyword", "{ \"match_all\": {} }", null);
    keywordScore.setApiVersion(2);
    keywordScore.setRetrieval(retrieval(r -> r.setProfileScore("best")));
    SearchTemplateProperties placeholders =
        body("Native", "{ \"bool\": { \"should\": [ {{&embeddingClausesJson}} ] } }", null);
    placeholders.setApiVersion(2);
    Map<String, SearchTemplateProperties> shapes =
        Map.of(
            "vectorThenKeyword", rung(r -> r.setCombine("vectorThenKeyword")),
            "vectorBody", rung(r -> r.setCombine("keywordRerank").setVectorBody("{}")),
            "scoped", scoped,
            "keywordProfiles", keywordProfiles,
            "keywordScore", keywordScore,
            "placeholders", placeholders);
    shapes.forEach(
        (name, properties) -> {
          store("urn:li:searchTemplate:" + name, properties);
          MergedSearchTemplate merged = loader.load(opContext, "urn:li:searchTemplate:" + name);
          assertThrows(
              SearchTemplateException.class,
              () ->
                  SearchTemplateCapabilities.requireSupported(
                      merged,
                      true,
                      false,
                      SearchTemplateComponentRegistry.empty(),
                      SearchTemplateGates.none()));
        });
  }

  @Test
  public void testFragmentAndRouteMatchCitationsNeedTheSemanticVersion() {
    SearchTemplateProperties fragment = body("Native", "{ \"match_all\": {} }", null);
    fragment.setQueryFragments(new StringMap(Map.of("filters", "{{embeddingField}}")));
    store("urn:li:searchTemplate:fragmentCite", fragment);
    assertThrows(
        SearchTemplateException.class,
        () -> loader.load(opContext, "urn:li:searchTemplate:fragmentCite"));

    SearchTemplateProperties router = body("Router", "{ \"match_all\": {} }", null);
    SearchTemplateRoute route = route("^urn:.*", SEARCH_TEMPLATE_FULLTEXT_URN);
    route.setMatch(new SearchTemplateRouteMatch().setSemanticEligible(true));
    router.setRoutes(routes(route));
    store("urn:li:searchTemplate:matchV1", router);
    assertThrows(
        SearchTemplateException.class,
        () -> loader.load(opContext, "urn:li:searchTemplate:matchV1"));
  }

  @Test
  public void testAProfilesResetNeedsTheSemanticVersion() {
    SearchTemplateProperties reset = body("Reset", "{ \"match_all\": {} }", null);
    reset.setProfiles(new SearchTemplateProfiles().setUseDefault(true));
    store("urn:li:searchTemplate:resetV1", reset);
    assertThrows(
        SearchTemplateException.class,
        () -> loader.load(opContext, "urn:li:searchTemplate:resetV1"));
  }

  @Test
  public void testMergedChecksCoverInheritedProfilesScopeAndKnnShapes() {
    SearchTemplateProperties scoped = rung(r -> r.setCombine("vector"));
    scoped.setProfiles(new SearchTemplateProfiles().setReplace(ids("a")));
    scoped.setScope(new SearchTemplateScope().setEntityTypes(ids("document")));
    store("urn:li:searchTemplate:scopedVector", scoped);
    MergedSearchTemplate merged = loader.load(opContext, "urn:li:searchTemplate:scopedVector");
    SearchTemplateChecks.checkInheritedSections(merged);
    assertEquals(merged.getScope().getEntityTypes(), List.of("document"));

    SearchTemplateProperties legacy = body("Legacy", "{ \"match_all\": {} }", null);
    legacy.setKnn("{ \"field\": \"embeddings.vector\", \"k\": 10 }");
    store("urn:li:searchTemplate:legacyKnn", legacy);
    SearchTemplateChecks.checkMerged(
        loader.load(opContext, "urn:li:searchTemplate:legacyKnn"), 1024);

    SearchTemplateProperties notObject = rung(r -> r.setCombine("keywordRerank"));
    notObject.setKnn("[]");
    store("urn:li:searchTemplate:knnArray", notObject);
    assertThrows(
        SearchTemplateException.class,
        () ->
            SearchTemplateChecks.checkMerged(
                loader.load(opContext, "urn:li:searchTemplate:knnArray"), 1024));
  }

  @Test
  public void testDeprecatedKnnNamesStillWrite() {
    SearchTemplateProperties named = rung(r -> r.setCombine("keywordRerank"));
    named.setKnn("{ \"model\": \"text_embedding_3_large\", \"vectorWeight\": 0.4 }");
    assertTrue(
        validate(
                templateValidator(),
                holeContext(),
                item(
                    UrnUtils.getUrn("urn:li:searchTemplate:namedKnn"),
                    SYSTEM,
                    ChangeType.UPSERT,
                    named))
            .isEmpty());
  }

  @Test
  public void testPlaceholderInAnySectionFailsAKeywordRerankWrite() {
    Map<String, Consumer<SearchTemplateProperties>> citations =
        Map.of(
            "functionScore",
            p ->
                p.setFunctionScore(
                    "{ \"functions\": [ { \"filter\": { \"exists\": { \"field\":"
                        + " \"{{embeddingField}}\" } }, \"weight\": 2 } ] }"),
            "knn",
            p -> p.setKnn("{ \"field\": \"{{embeddingField}}\", \"k\": 10 }"),
            "aggregations",
            p ->
                p.setAggregations(
                    "{ \"vectors\": { \"value_count\": { \"field\": \"{{embeddingField}}\" } } }"),
            "sort",
            p -> p.setSort("[ { \"{{embeddingField}}\": \"desc\" } ]"));
    List<String> accepted = new ArrayList<>();
    citations.forEach(
        (section, cite) -> {
          // The same citation on a keyword rung is the native query shape, which still writes.
          SearchTemplateProperties keyword = body("Native", "{ \"match_all\": {} }", null);
          keyword.setApiVersion(2);
          cite.accept(keyword);
          assertTrue(write(section + "Keyword", keyword).isEmpty(), section);

          SearchTemplateProperties rerank = rung(r -> r.setCombine("keywordRerank"));
          cite.accept(rerank);
          if (write(section + "Rerank", rerank).isEmpty()) {
            accepted.add(section);
          }
        });
    assertEquals(accepted, List.of());
  }

  private List<?> write(String name, SearchTemplateProperties properties) {
    return validate(
        templateValidator(),
        holeContext(),
        item(
            UrnUtils.getUrn("urn:li:searchTemplate:" + name),
            SYSTEM,
            ChangeType.UPSERT,
            properties));
  }

  private void assertRouteMatch(SearchTemplateRouteMatch match, boolean valid) {
    SearchTemplateProperties router = body("Router", "{ \"match_all\": {} }", null);
    router.setApiVersion(2);
    SearchTemplateRoute route = route("^urn:.*", SEARCH_TEMPLATE_FULLTEXT_URN);
    route.setMatch(match);
    router.setRoutes(routes(route));
    if (valid) {
      SearchTemplateChecks.check(router, 1024);
    } else {
      assertThrows(SearchTemplateException.class, () -> SearchTemplateChecks.check(router, 1024));
    }
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

  private static StringArray ids(String... ids) {
    return new StringArray(List.of(ids));
  }

  private static FloatMap weights(String id, float weight) {
    return new FloatMap(Map.of(id, weight));
  }
}

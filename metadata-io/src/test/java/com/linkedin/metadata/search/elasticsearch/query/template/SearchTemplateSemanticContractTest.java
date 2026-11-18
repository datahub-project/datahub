package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_FULLTEXT_URN;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
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
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.embedding.EmbeddingProfile;
import com.linkedin.metadata.search.embedding.EmbeddingProfileRegistry;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.search.template.SearchTemplateProfiles;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRetrieval;
import com.linkedin.search.template.SearchTemplateRetrievalAnchor;
import com.linkedin.search.template.SearchTemplateRoute;
import com.linkedin.search.template.SearchTemplateRouteMatch;
import com.linkedin.search.template.SearchTemplateScope;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        assertEquals(resolved.getTemplateRerank(), RERANK, semanticRead + " " + entity);
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

    // A knn that names both model and field with no combine is read as keywordRerank: it binds
    // without document rerank, and the served rung owns its rerank instead of the legacy kNN leg.
    String migratedUrn = "urn:li:searchTemplate:migrated";
    SearchTemplateProperties migrated = parentOf("Migrated", SEARCH_TEMPLATE_FULLTEXT_URN);
    migrated.setKnn(
        "{ \"model\": \"text-embedding-3-small\", \"field\": \"embeddings.vector\", \"k\": 10 }");
    store(migratedUrn, migrated);
    templates.getDefaults().put("fulltext", Map.of("default", migratedUrn));
    for (boolean semanticRead : List.of(false, true)) {
      SearchTemplateCoordinator coordinator = coordinator(semanticRead);
      OperationContext bound =
          coordinator.bind(opContext, "customer orders", false, List.of("dataset", "document"));
      coordinator.armServed(bound, migratedUrn);
      ResolvedSearchTemplate resolved = bound.getSearchContext().getResolvedSearchTemplate();
      assertFalse(resolved.isKnnApplied(), String.valueOf(semanticRead));
      assertEquals(resolved.getTemplateRerank(), migratedUrn, String.valueOf(semanticRead));
    }
  }

  @Test
  public void testUnreadableRerankBlendDoesNotFailTheRender() {
    store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", "{ \"match_all\": {} }", null));
    SearchTemplateProperties rerank = parentOf("Rerank", SEARCH_TEMPLATE_FULLTEXT_URN);
    rerank.setApiVersion(2);
    rerank.setRetrieval(retrieval(r -> r.setCombine("keywordRerank")));
    rerank.setKnn("{ \"vectorWeight\": ");
    store(RERANK, rerank);
    templates.getDefaults().put("fulltext", Map.of("default", RERANK));

    // The rerank reads the blend later and keeps keyword order when it cannot.
    SearchTemplateDryRender preview =
        coordinator(true)
            .preview(
                opContext,
                "customer orders",
                List.of("document"),
                false,
                SearchClientShim.SearchEngineType.OPENSEARCH_3);
    assertEquals(preview.resolvedUrn(), RERANK);
  }

  @Test
  public void testKeywordRerankCacheIdentityFollowsTheRerankInputs() {
    store(SEARCH_TEMPLATE_FULLTEXT_URN, body("Fulltext", "{ \"match_all\": {} }", null));
    SearchTemplateProperties rerank = parentOf("Rerank", SEARCH_TEMPLATE_FULLTEXT_URN);
    rerank.setApiVersion(2);
    rerank.setRetrieval(retrieval(r -> r.setCombine("keywordRerank")));
    store(RERANK, rerank);
    templates.getDefaults().put("fulltext", Map.of("default", RERANK));
    EmbeddingProfile documentDefault =
        new EmbeddingProfile(
            "text_embedding_3_small",
            EmbeddingProfile.TEXT_CHUNKS,
            "text-embedding-3-small",
            "cosine",
            Set.of("document"),
            Set.of(EmbeddingProfileRegistry.ELASTICSEARCH, EmbeddingProfileRegistry.OPENSEARCH));
    SemanticSearchConfiguration semanticSearch = new SemanticSearchConfiguration();
    semanticSearch.setEnabled(true);
    semanticSearch.setEnabledEntities(Set.of("document"));
    semanticSearch.setModels(Map.of(documentDefault.id(), new ModelEmbeddingConfig()));
    EmbeddingProfileRegistry registry =
        new EmbeddingProfileRegistry(
            List.of(documentDefault), Map.of("document", documentDefault.id()));
    // The resolved profile must be available on the engine serving the search.
    SearchClientShim<?> client = mock(SearchClientShim.class);
    when(client.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    OperationContext searching = TestOperationContexts.withFixedSearchClient(opContext, client);
    Set<String> identities = new HashSet<>();
    for (boolean hybridRead : List.of(false, true)) {
      ElasticSearchConfiguration elastic = v3Config();
      elastic.getEntityIndex().setSemanticSearch(semanticSearch);
      SearchTemplateCoordinator coordinator =
          new SearchTemplateCoordinator(
              elastic,
              null,
              loader,
              ForkJoinPool.commonPool(),
              SearchTemplateComponentRegistry.empty(),
              SearchTemplateGates.of(registry, semanticSearch, hybridRead));
      for (List<String> entities : List.of(List.of("dataset"), List.of("document"))) {
        String identity =
            coordinator
                .bind(searching, "customer orders", false, entities)
                .getSearchContext()
                .getResolvedSearchTemplate()
                .cacheKeyComponent();
        assertTrue(
            identity.contains("~kr" + SearchTemplateKeywordRerank.EXECUTION_VERSION), identity);
        identities.add(identity);
      }
    }
    // Hybrid read, document rerank, and the resolved profile each move the key.
    assertEquals(identities.size(), 4);
    assertTrue(identities.stream().anyMatch(id -> id.contains(documentDefault.id())));
  }

  @Test
  public void testVectorCacheIdentityFollowsTheVectorInputs() {
    store("urn:li:searchTemplate:vectorRung", rung(r -> r.setCombine("vector")));
    templates.getDefaults().put("fulltext", Map.of("default", "urn:li:searchTemplate:vectorRung"));
    EmbeddingProfile documentDefault =
        new EmbeddingProfile(
            "text_embedding_3_small",
            EmbeddingProfile.TEXT_CHUNKS,
            "text-embedding-3-small",
            "cosine",
            Set.of("document"),
            Set.of(EmbeddingProfileRegistry.ELASTICSEARCH, EmbeddingProfileRegistry.OPENSEARCH));
    EmbeddingProfileRegistry registry =
        new EmbeddingProfileRegistry(
            List.of(documentDefault), Map.of("document", documentDefault.id()));
    SearchClientShim<?> client = mock(SearchClientShim.class);
    when(client.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    OperationContext searching = TestOperationContexts.withFixedSearchClient(opContext, client);
    Set<String> identities = new HashSet<>();
    for (Float floor : java.util.Arrays.asList(null, 0.7f)) {
      SemanticSearchConfiguration semanticSearch = new SemanticSearchConfiguration();
      semanticSearch.setEnabled(true);
      semanticSearch.setEnabledEntities(Set.of("document"));
      semanticSearch.setModels(Map.of(documentDefault.id(), new ModelEmbeddingConfig()));
      semanticSearch.setMinSimilarity(floor);
      SearchTemplateCoordinator coordinator =
          new SearchTemplateCoordinator(
              v3Config(),
              null,
              loader,
              ForkJoinPool.commonPool(),
              SearchTemplateComponentRegistry.empty(),
              SearchTemplateGates.of(registry, semanticSearch, false));
      for (List<String> entities : List.of(List.of("dataset"), List.of("document"))) {
        String identity =
            coordinator
                .bind(searching, "customer orders", false, entities)
                .getSearchContext()
                .getResolvedSearchTemplate()
                .cacheKeyComponent();
        assertTrue(
            identity.contains("~vr" + SearchTemplateVectorRetrieval.EXECUTION_VERSION), identity);
        identities.add(identity);
      }
    }
    // The resolved profile and the floor inherited from configuration each move the key.
    assertEquals(identities.size(), 4);
    assertTrue(identities.stream().anyMatch(id -> id.contains(documentDefault.id())));
  }

  @Test
  public void testScrollFailsClosedOnAVectorRung() {
    store("urn:li:searchTemplate:vectorRung", rung(r -> r.setCombine("vector")));
    templates.getDefaults().put("fulltext", Map.of("default", "urn:li:searchTemplate:vectorRung"));
    SearchTemplateCoordinator coordinator = coordinator(true);

    // A search binds the rung. A scroll has no keyword query to continue, and it does not fall
    // back to one.
    coordinator.bind(opContext, "customer orders", false, List.of("document"));
    assertThrows(
        SearchTemplateException.class,
        () -> coordinator.bind(opContext, "customer orders", true, List.of("document")));
    SearchTemplateDryRender preview =
        coordinator.preview(
            opContext,
            "customer orders",
            List.of("document"),
            true,
            SearchClientShim.SearchEngineType.OPENSEARCH_3);
    assertTrue(
        preview.capabilities().stream()
            .anyMatch(
                decision ->
                    "sections".equals(decision.name())
                        && !decision.accepted()
                        && "vectorScroll".equals(decision.detail())));
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
    SearchTemplateChecks.check(
        rung(r -> r.setCombine("vector").setCandidateCap(1).setOnFailure("next")), 1024);

    List<Consumer<SearchTemplateRetrieval>> invalid =
        List.of(
            r -> r.setCombine("hybrid"),
            r -> r.setCombine("vector").setCandidateCap(0),
            r -> r.setCombine("vector").setOnFailure("degradeIfNative"),
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
    assertEquals(rejection(nativeMerged), "nativeMixedQuery");

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
    assertNull(rejectionOf("vector", rung(r -> r.setCombine("vector"))));
    // Write rejects a cap below 1; bind rejects one stored before that check
    assertEquals(
        rejectionOf("zeroCap", rung(r -> r.setCombine("vector").setCandidateCap(0))),
        "retrievalCandidateCap");
    assertEquals(
        rejectionOf("similarity", rung(r -> r.setCombine("vector").setQuerySource("anchorEntity"))),
        "similarity");
    assertEquals(
        rejectionOf("vectorBody", rung(r -> r.setCombine("keywordRerank").setVectorBody("{}"))),
        "vectorBody");
    SearchTemplateProperties scoped = body("Scoped", "{ \"match_all\": {} }", null);
    scoped.setApiVersion(2);
    scoped.setScope(new SearchTemplateScope().setEntityTypes(new StringArray(List.of("document"))));
    assertEquals(rejectionOf("scoped", scoped), "entityPartition");
    assertNull(rejectionOf("keywordRerank", rung(r -> r.setCombine("keywordRerank"))));
    // This release skips the rerank and keeps keyword order, so the selection does not block bind.
    SearchTemplateProperties rerankProfiles =
        rung(r -> r.setCombine("keywordRerank").setProfileScore("weightedSum"));
    rerankProfiles.setProfiles(
        new SearchTemplateProfiles().setReplace(new StringArray(List.of("a", "b"))));
    assertNull(rejectionOf("rerankProfiles", rerankProfiles));
    // A citation outside the native query sections never renders, so it fails closed too.
    SearchTemplateProperties sortPlaceholder = body("Native", "{ \"match_all\": {} }", null);
    sortPlaceholder.setApiVersion(2);
    sortPlaceholder.setSort("[ { \"{{embeddingField}}\": \"desc\" } ]");
    assertEquals(rejectionOf("sortPlaceholder", sortPlaceholder), "nativeMixedQuery");
    SearchTemplateProperties sortPlaceholderV1 = body("Native", "{ \"match_all\": {} }", null);
    sortPlaceholderV1.setSort("[ { \"{{embeddingField}}\": \"desc\" } ]");
    assertThrows(
        SearchTemplateException.class, () -> rejectionOf("sortPlaceholderV1", sortPlaceholderV1));
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
    // The reset still counts as a selection, so deprecated knn names cannot override it.
    assertTrue(loader.load(opContext, "urn:li:searchTemplate:reset").isProfilesSet());

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
  public void testRemoveThatEmptiesTheInheritedReplaceFails() {
    SearchTemplateProperties parent = rung(r -> r.setCombine("vector"));
    parent.setProfiles(new SearchTemplateProfiles().setReplace(new StringArray(List.of("a"))));
    store("urn:li:searchTemplate:emptyParent", parent);
    SearchTemplateProperties child = parentOf("Child", "urn:li:searchTemplate:emptyParent");
    child.setProfiles(new SearchTemplateProfiles().setRemove(new StringArray(List.of("a"))));
    store("urn:li:searchTemplate:emptyChild", child);
    assertThrows(
        SearchTemplateException.class,
        () -> loader.load(opContext, "urn:li:searchTemplate:emptyChild"));

    SearchTemplateValidator validator = templateValidator();
    assertFalse(
        validate(
                validator,
                holeContext(),
                item(
                    UrnUtils.getUrn("urn:li:searchTemplate:emptyChild"),
                    SYSTEM,
                    ChangeType.UPSERT,
                    child))
            .isEmpty());

    // A parent write that narrows the list under an inheriting child's remove fails too.
    SearchTemplateProperties wide = rung(r -> r.setCombine("vector"));
    wide.setProfiles(new SearchTemplateProfiles().setReplace(new StringArray(List.of("a", "b"))));
    store("urn:li:searchTemplate:wideParent", wide);
    SearchTemplateProperties narrowed = parentOf("Child", "urn:li:searchTemplate:wideParent");
    narrowed.setProfiles(new SearchTemplateProfiles().setRemove(new StringArray(List.of("a"))));
    store("urn:li:searchTemplate:narrowedChild", narrowed);
    assertEquals(
        loader.load(opContext, "urn:li:searchTemplate:narrowedChild").getProfiles().getReplace(),
        List.of("b"));
    SearchTemplateProperties narrow = rung(r -> r.setCombine("vector"));
    narrow.setProfiles(new SearchTemplateProfiles().setReplace(new StringArray(List.of("a"))));
    assertFalse(
        validate(
                validator,
                holeContext(),
                item(
                    UrnUtils.getUrn("urn:li:searchTemplate:wideParent"),
                    SYSTEM,
                    ChangeType.UPSERT,
                    narrow))
            .isEmpty());
  }

  @Test
  public void testPlaceholderInAnySectionFailsClosed() {
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
    citations.forEach(
        (section, cite) -> {
          SearchTemplateProperties semantic = body("Native", "{ \"match_all\": {} }", null);
          semantic.setApiVersion(2);
          cite.accept(semantic);
          store("urn:li:searchTemplate:" + section + "V2", semantic);
          // Document rerank is on so a legacy knn section is not rejected before the scan.
          assertEquals(
              SearchTemplateCapabilities.rejectionCode(
                  loader.load(opContext, "urn:li:searchTemplate:" + section + "V2"),
                  true,
                  false,
                  SearchTemplateComponentRegistry.empty(),
                  SearchTemplateGates.none()),
              "nativeMixedQuery",
              section);

          SearchTemplateProperties legacy = body("Native", "{ \"match_all\": {} }", null);
          cite.accept(legacy);
          assertMergedRejects(section + "V1", legacy);
        });
  }

  @Test
  public void testDeprecatedModelAndFieldMustNameOneProfileAtWrite() {
    SearchTemplateProperties split = rung(r -> r.setCombine("keywordRerank"));
    split.setKnn("{ \"model\": \"a\", \"field\": \"b\" }");
    assertMergedRejects("splitNames", split);

    SearchTemplateProperties same = rung(r -> r.setCombine("keywordRerank"));
    same.setKnn("{ \"model\": \"a\", \"field\": \"a\" }");
    store("urn:li:searchTemplate:sameNames", same);
    SearchTemplateChecks.checkMerged(
        loader.load(opContext, "urn:li:searchTemplate:sameNames"), 1024);
  }

  @Test
  public void testEveryLadderRungResolvesItsProfiles() {
    EmbeddingProfile datasetL2 =
        new EmbeddingProfile(
            "dataset_l2",
            EmbeddingProfile.TEXT_CHUNKS,
            null,
            "l2",
            Set.of("dataset"),
            Set.of(EmbeddingProfileRegistry.OPENSEARCH));
    SearchTemplateGates vectorBuild = datasetVectorBuild(datasetL2);
    SearchTemplateProperties root = body("Keyword", "{ \"match_all\": {} }", null);
    root.setLadder(stopLadder(1, "urn:li:searchTemplate:floorChild"));
    store("urn:li:searchTemplate:floorRoot", root);
    store(
        "urn:li:searchTemplate:floorChild",
        rung(r -> r.setCombine("vector").setMinSimilarity(0.5f)));
    useRuntime(
        new SearchTemplateCoordinator(
            v3Config(),
            null,
            loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty(),
            vectorBuild));
    ResolvedSearchTemplate resolved =
        new ResolvedSearchTemplate("urn:li:searchTemplate:floorRoot", 1L, null, null);
    resolved.setExecutionCapabilities(
        false, false, List.of("dataset"), EmbeddingProfileRegistry.OPENSEARCH);
    opContext = opContext.withResolvedSearchTemplate(resolved);

    // The keyword root finds nothing, so the walk tries the vector rung, whose L2 profile cannot
    // take a cosine floor.
    assertThrows(
        SearchTemplateException.class,
        () -> walk("urn:li:searchTemplate:floorRoot", "orders", null, 10, urn -> page(0)));
  }

  @Test
  public void testBindAndPreviewCountTheProfilesTheRequestSelects() {
    EmbeddingProfile datasetCosine =
        new EmbeddingProfile(
            "dataset_cosine",
            EmbeddingProfile.TEXT_CHUNKS,
            null,
            "cosine",
            Set.of("dataset"),
            Set.of(EmbeddingProfileRegistry.ELASTICSEARCH, EmbeddingProfileRegistry.OPENSEARCH));
    SearchTemplateProperties two = rung(r -> r.setCombine("vector"));
    two.setProfiles(new SearchTemplateProfiles().setAdd(new StringArray(List.of("connector"))));
    store("urn:li:searchTemplate:twoProfiles", two);
    templates.getDefaults().put("fulltext", Map.of("default", "urn:li:searchTemplate:twoProfiles"));
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(
            v3Config(),
            null,
            loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty(),
            datasetVectorBuild(datasetCosine));

    assertThrows(
        SearchTemplateException.class,
        () -> coordinator.bind(opContext, "customer orders", false, List.of("dataset")));
    // An entity that does not join the vector leg selects nothing, so the request binds.
    coordinator.bind(opContext, "customer orders", false, List.of("chart"));
    SearchTemplateDryRender preview =
        coordinator.preview(
            opContext,
            "customer orders",
            List.of("dataset"),
            false,
            SearchClientShim.SearchEngineType.OPENSEARCH_3);
    assertTrue(
        preview.capabilities().stream()
            .anyMatch(
                decision ->
                    "sections".equals(decision.name())
                        && !decision.accepted()
                        && SearchTemplateGates.SEVERAL_PROFILES.equals(decision.detail())));
  }

  @Test
  public void testKeywordRerankChecksItsSelectionOnlyWithHybridRead() {
    EmbeddingProfile documentDefault =
        new EmbeddingProfile(
            "doc_default",
            EmbeddingProfile.TEXT_CHUNKS,
            null,
            "cosine",
            Set.of("document"),
            Set.of(EmbeddingProfileRegistry.ELASTICSEARCH, EmbeddingProfileRegistry.OPENSEARCH));
    EmbeddingProfile connector =
        new EmbeddingProfile(
            "doc_connector",
            EmbeddingProfile.TEXT_CHUNKS,
            null,
            "cosine",
            Set.of("document"),
            Set.of(EmbeddingProfileRegistry.ELASTICSEARCH, EmbeddingProfileRegistry.OPENSEARCH));
    SearchTemplateProperties rerank = rung(r -> r.setCombine("keywordRerank"));
    rerank.setProfiles(
        new SearchTemplateProfiles()
            .setReplace(new StringArray(List.of(documentDefault.id(), connector.id()))));
    store("urn:li:searchTemplate:twoRerank", rerank);
    templates.getDefaults().put("fulltext", Map.of("default", "urn:li:searchTemplate:twoRerank"));
    SemanticSearchConfiguration semanticSearch = new SemanticSearchConfiguration();
    semanticSearch.setEnabled(true);
    semanticSearch.setEnabledEntities(Set.of("document"));
    semanticSearch.setModels(Map.of(documentDefault.id(), new ModelEmbeddingConfig()));
    EmbeddingProfileRegistry registry =
        new EmbeddingProfileRegistry(
            List.of(documentDefault, connector), Map.of("document", documentDefault.id()));

    for (boolean hybridRead : List.of(false, true)) {
      ElasticSearchConfiguration elastic = v3Config();
      elastic.getEntityIndex().setSemanticSearch(semanticSearch);
      SearchTemplateCoordinator coordinator =
          new SearchTemplateCoordinator(
              elastic,
              null,
              loader,
              ForkJoinPool.commonPool(),
              SearchTemplateComponentRegistry.empty(),
              new SearchTemplateGates(
                  registry,
                  semanticSearch,
                  hybridRead,
                  Set.of(SearchTemplateGates.DOCUMENT_RERANK),
                  Map.of(EmbeddingProfile.TEXT_CHUNKS, Set.of("document"))));
      if (hybridRead) {
        // The rerank runs here, and it cannot combine two profiles.
        assertThrows(
            SearchTemplateException.class,
            () -> coordinator.bind(opContext, "customer orders", false, List.of("document")));
      } else {
        // Without hybrid read the rung keeps keyword order, so its selection does not matter.
        coordinator.bind(opContext, "customer orders", false, List.of("document"));
      }
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

  // A build that runs vector retrieval for Dataset text chunks, with semantic search on for
  // Dataset.
  private static SearchTemplateGates datasetVectorBuild(EmbeddingProfile datasetProfile) {
    SemanticSearchConfiguration semanticSearch = new SemanticSearchConfiguration();
    semanticSearch.setEnabled(true);
    semanticSearch.setEnabledEntities(Set.of("dataset"));
    semanticSearch.setModels(Map.of(datasetProfile.id(), new ModelEmbeddingConfig()));
    return new SearchTemplateGates(
        new EmbeddingProfileRegistry(
            List.of(datasetProfile), Map.of("dataset", datasetProfile.id())),
        semanticSearch,
        true,
        Set.of(SearchTemplateGates.VECTOR_RETRIEVAL),
        Map.of(EmbeddingProfile.TEXT_CHUNKS, Set.of("dataset")));
  }

  private SearchTemplateCoordinator coordinator(boolean semanticRead) {
    ElasticSearchConfiguration elastic = v3Config();
    SemanticSearchConfiguration semanticSearch = new SemanticSearchConfiguration();
    semanticSearch.setEnabled(semanticRead);
    elastic.getEntityIndex().setSemanticSearch(semanticSearch);
    return new SearchTemplateCoordinator(
        elastic,
        null,
        loader,
        ForkJoinPool.commonPool(),
        SearchTemplateComponentRegistry.empty(),
        SearchTemplateGates.none());
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
        template,
        false,
        false,
        SearchTemplateComponentRegistry.empty(),
        SearchTemplateGates.none());
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

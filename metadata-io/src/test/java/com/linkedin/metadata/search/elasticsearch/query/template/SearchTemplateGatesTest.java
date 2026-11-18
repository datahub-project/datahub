package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateGates.DOCUMENT_RERANK;
import static com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateGates.ENTITY_PARTITION;
import static com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateGates.SIMILARITY;
import static com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateGates.VECTOR_RETRIEVAL;
import static com.linkedin.metadata.search.embedding.EmbeddingProfileRegistry.ELASTICSEARCH;
import static com.linkedin.metadata.search.embedding.EmbeddingProfileRegistry.OPENSEARCH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.embedding.EmbeddingProfile;
import com.linkedin.metadata.search.embedding.EmbeddingProfileRegistry;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateProfiles;
import com.linkedin.search.template.SearchTemplateRetrieval;
import com.linkedin.search.template.SearchTemplateScope;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class SearchTemplateGatesTest {
  private static final String URN = "urn:li:searchTemplate:gates";
  private static final EmbeddingProfile DOCUMENT_DEFAULT =
      new EmbeddingProfile(
          "text_embedding_3_large",
          EmbeddingProfile.TEXT_CHUNKS,
          "text-embedding-3-large",
          "cosinesimil",
          Set.of("document"),
          Set.of(ELASTICSEARCH, OPENSEARCH));
  private static final EmbeddingProfile DATASET_L2 =
      new EmbeddingProfile(
          "dataset_l2",
          EmbeddingProfile.TEXT_CHUNKS,
          null,
          "l2",
          Set.of("dataset"),
          Set.of(ELASTICSEARCH, OPENSEARCH));
  private static final EmbeddingProfile CONNECTOR =
      new EmbeddingProfile(
          "textChunks.v2",
          EmbeddingProfile.TEXT_CHUNKS,
          null,
          "cosine",
          Set.of("document"),
          Set.of(OPENSEARCH));
  private static final EmbeddingProfileRegistry REGISTRY =
      new EmbeddingProfileRegistry(
          List.of(DOCUMENT_DEFAULT, DATASET_L2, CONNECTOR),
          Map.of("document", DOCUMENT_DEFAULT.id(), "dataset", DATASET_L2.id()));

  /** Semantic search on for Document and Dataset. */
  private static final SemanticSearchConfiguration SEMANTIC = semantic("document", "dataset");

  /** A build that runs vector retrieval, partitioning, and rerank for Document and Dataset. */
  private static final SearchTemplateGates VECTOR_BUILD =
      new SearchTemplateGates(
          REGISTRY,
          SEMANTIC,
          true,
          Set.of(VECTOR_RETRIEVAL, ENTITY_PARTITION, DOCUMENT_RERANK),
          Map.of(EmbeddingProfile.TEXT_CHUNKS, Set.of("document", "dataset")));

  @Test
  public void testThisBuildAdvertisesDocumentRerankAndVectorRetrieval() {
    SearchTemplateGates gates = SearchTemplateGates.of(REGISTRY, SEMANTIC, true);
    assertNull(gates.rejectionCode(merged(retrieval("vector")).build(), true));
    // Anchor similarity needs vector retrieval and similarity.
    MergedSearchTemplate anchored =
        merged(retrieval("vector").setQuerySource("anchorEntity")).build();
    assertEquals(gates.rejectionCode(anchored, true), SIMILARITY);
    assertEquals(
        gates.rejectionCode(merged(retrieval("vectorThenKeyword")).build(), true),
        SearchTemplateGates.VECTOR_THEN_KEYWORD);
    assertEquals(
        gates.rejectionCode(
            merged(retrieval("keywordRerank"))
                .scope(new SearchTemplateScope().setEntityTypes(ids("document")))
                .build(),
            true),
        ENTITY_PARTITION);
    assertEquals(
        gates.rejectionCode(merged(null).query("{ {{&embeddingClausesJson}} }").build(), true),
        SearchTemplateGates.NATIVE_MIXED_QUERY);
    assertEquals(
        gates.rejectionCode(merged(retrieval("keywordRerank").setVectorBody("{}")).build(), true),
        SearchTemplateGates.VECTOR_BODY);
    assertEquals(
        gates.rejectionCode(merged(null).profiles(replace(DOCUMENT_DEFAULT.id())).build(), true),
        SearchTemplateGates.KEYWORD_PROFILES);
  }

  @Test
  public void testKeywordRerankBindsWhileTheRequestCannotRerank() {
    MergedSearchTemplate rerank =
        merged(retrieval("keywordRerank").setProfileScore("weightedSum"))
            .profiles(replace(DOCUMENT_DEFAULT.id(), CONNECTOR.id()))
            .build();
    // This build reranks only when the request can, so while it cannot, the selection only the
    // rerank uses does not fail the search.
    SearchTemplateGates gates = SearchTemplateGates.of(REGISTRY, SEMANTIC, true);
    assertTrue(gates.advertises(DOCUMENT_RERANK));
    assertNull(gates.rejectionCode(rerank, false));
    assertNull(gates.bindingRejection(rerank, List.of("document"), OPENSEARCH, false));
    // With hybrid read off it never reranks.
    SearchTemplateGates hybridOff = SearchTemplateGates.of(REGISTRY, SEMANTIC, false);
    assertNull(hybridOff.rejectionCode(rerank, true));
    assertNull(hybridOff.bindingRejection(rerank, List.of("document"), OPENSEARCH, true));

    // When it can, combining several profiles is a capability it does not run.
    assertEquals(gates.rejectionCode(rerank, true), SearchTemplateGates.SEVERAL_PROFILES);
    MergedSearchTemplate twoProfiles =
        merged(retrieval("keywordRerank"))
            .profiles(replace(DOCUMENT_DEFAULT.id(), CONNECTOR.id()))
            .build();
    assertNull(gates.rejectionCode(twoProfiles, true));
    assertEquals(
        gates.bindingRejection(twoProfiles, List.of("document"), OPENSEARCH, true),
        SearchTemplateGates.SEVERAL_PROFILES);
  }

  @Test
  public void testKeywordRerankPlansFromTheRenderedBlend() {
    // The write check renders a Mustache value in the blend, so the rerank reads it the same way.
    MergedSearchTemplate templated =
        merged(retrieval("keywordRerank")).knn("{ \"vectorWeight\": {{partialFactor}} }").build();
    assertTrue(
        SearchTemplateKeywordRerank.plan(
                    templated, VECTOR_BUILD, List.of("document"), OPENSEARCH, null)
                .merger()
            != null);
  }

  @Test
  public void testKeywordRerankOverAMixedRequestGatesEachEntity() {
    MergedSearchTemplate rerank = merged(retrieval("keywordRerank")).build();
    SearchTemplateGates gates = SearchTemplateGates.of(REGISTRY, SEMANTIC, true);
    List<String> mixed = List.of("document", "dataset");
    assertNull(gates.rejectionCode(rerank, true));
    assertNull(gates.bindingRejection(rerank, mixed, OPENSEARCH, true));
    // Document joins the rerank; this build does not rerank Dataset rows, so they keep their
    // places.
    Map<String, String> entityGates = gates.entityGates(rerank, mixed, OPENSEARCH);
    assertNull(entityGates.get("document"));
    assertEquals(entityGates.get("dataset"), SearchTemplateGates.ENTITY_NOT_ADVERTISED);
    assertEquals(gates.joinedProfileIds(rerank, mixed, OPENSEARCH), Set.of(DOCUMENT_DEFAULT.id()));
    // The rerank runs only with a joined profile the query embedding service embeds.
    assertNull(
        SearchTemplateKeywordRerank.keptReason(
            rerank, gates, mixed, OPENSEARCH, DOCUMENT_DEFAULT.id()));
    assertEquals(
        SearchTemplateKeywordRerank.keptReason(rerank, gates, mixed, OPENSEARCH, "another"),
        SearchTemplateKeywordRerank.PROFILE_NOT_EMBEDDABLE);
    assertEquals(
        SearchTemplateKeywordRerank.keptReason(
            rerank, gates, List.of("dataset"), OPENSEARCH, DOCUMENT_DEFAULT.id()),
        SearchTemplateKeywordRerank.NO_ELIGIBLE_ROWS);
  }

  @Test
  public void testProfileCountIsTakenAfterResolution() {
    // The dataset profile falls back to the Document default for Document, so one profile is left.
    MergedSearchTemplate named =
        merged(retrieval("vector"))
            .profiles(replace(DATASET_L2.id(), DOCUMENT_DEFAULT.id()))
            .build();
    assertNull(VECTOR_BUILD.rejectionCode(named, true));
    assertNull(VECTOR_BUILD.bindingRejection(named, List.of("document"), OPENSEARCH, true));
  }

  @Test
  public void testNativeMixedQueryResolvesItsProfilesWhenAdvertised() {
    SearchTemplateGates nativeBuild =
        new SearchTemplateGates(
            REGISTRY,
            SEMANTIC,
            true,
            Set.of(SearchTemplateGates.NATIVE_MIXED_QUERY),
            SearchTemplateGates.ADVERTISED_ENTITY_TYPES);
    MergedSearchTemplate mixed =
        merged(null)
            .query("{ \"bool\": { \"should\": [ {{&embeddingClausesJson}} ] } }")
            .profiles(new SearchTemplateProfiles().setAdd(ids(CONNECTOR.id())))
            .build();
    assertNull(nativeBuild.rejectionCode(mixed, true));
    assertEquals(
        nativeBuild.bindingRejection(mixed, List.of("document"), OPENSEARCH, true),
        SearchTemplateGates.SEVERAL_PROFILES);
  }

  @Test
  public void testResolvedProfilesFailTheTemplateOnlyWhenTheLegRuns() {
    MergedSearchTemplate vector = merged(retrieval("vector")).build();
    assertNull(VECTOR_BUILD.rejectionCode(vector, true));
    assertNull(VECTOR_BUILD.bindingRejection(vector, List.of("document"), OPENSEARCH, true));
    // Two entity defaults make two profiles in one leg.
    assertEquals(
        VECTOR_BUILD.bindingRejection(vector, List.of("document", "dataset"), OPENSEARCH, true),
        SearchTemplateGates.SEVERAL_PROFILES);
    // An entity that does not join the leg adds no profile.
    assertNull(
        VECTOR_BUILD.bindingRejection(vector, List.of("document", "chart"), OPENSEARCH, true));

    MergedSearchTemplate floor = merged(retrieval("vector").setMinSimilarity(0.8f)).build();
    assertNull(VECTOR_BUILD.bindingRejection(floor, List.of("document"), OPENSEARCH, true));
    assertEquals(
        VECTOR_BUILD.bindingRejection(floor, List.of("dataset"), OPENSEARCH, true),
        SearchTemplateGates.MIN_SIMILARITY);
    // This build runs the vector leg for Document only, so Dataset does not join it.
    assertNull(
        SearchTemplateGates.of(REGISTRY, SEMANTIC, true)
            .bindingRejection(floor, List.of("dataset"), OPENSEARCH, true));
  }

  @Test
  public void testTheDeploymentFloorAppliesToAVectorRungThatSetsNone() {
    SemanticSearchConfiguration floored = semantic("document", "dataset");
    floored.setMinSimilarity(0.5f);
    SearchTemplateGates gates =
        new SearchTemplateGates(
            REGISTRY,
            floored,
            true,
            Set.of(VECTOR_RETRIEVAL, DOCUMENT_RERANK),
            Map.of(EmbeddingProfile.TEXT_CHUNKS, Set.of("document", "dataset")));
    MergedSearchTemplate vector = merged(retrieval("vector")).build();
    assertEquals(gates.minSimilarity(vector), 0.5f);
    // An L2 profile cannot take the inherited cosine floor either
    assertEquals(
        gates.bindingRejection(vector, List.of("dataset"), OPENSEARCH, true),
        SearchTemplateGates.MIN_SIMILARITY);
    assertNull(gates.bindingRejection(vector, List.of("document"), OPENSEARCH, true));
    // A template floor replaces it, and keywordRerank drops nothing
    assertEquals(
        gates.minSimilarity(merged(retrieval("vector").setMinSimilarity(0.9f)).build()), 0.9f);
    assertNull(gates.minSimilarity(merged(retrieval("keywordRerank")).build()));
    assertNull(
        gates.bindingRejection(
            merged(retrieval("keywordRerank")).build(), List.of("dataset"), OPENSEARCH, true));
  }

  @Test
  public void testScopedNamedProfileMustCoverEveryScopedType() {
    SearchTemplateScope scope =
        new SearchTemplateScope().setEntityTypes(ids("document", "dataset"));
    assertEquals(
        VECTOR_BUILD.rejectionCode(
            merged(retrieval("vector"))
                .scope(scope)
                .profiles(replace(DOCUMENT_DEFAULT.id()))
                .build(),
            true),
        SearchTemplateGates.PROFILE_SCOPE);
    assertNull(
        VECTOR_BUILD.rejectionCode(
            merged(retrieval("vector"))
                .scope(new SearchTemplateScope().setEntityTypes(ids("document")))
                .profiles(replace(DOCUMENT_DEFAULT.id()))
                .build(),
            true));
  }

  @Test
  public void testEntityGatesReportWhyAnEntityLeavesTheLeg() {
    MergedSearchTemplate vector = merged(retrieval("vector")).build();
    SearchTemplateGates documentOnly =
        new SearchTemplateGates(
            REGISTRY,
            SEMANTIC,
            true,
            Set.of(VECTOR_RETRIEVAL),
            Map.of(EmbeddingProfile.TEXT_CHUNKS, Set.of("document")));

    Map<String, String> expected = new HashMap<>();
    expected.put("document", null);
    expected.put("dataset", SearchTemplateGates.ENTITY_NOT_ADVERTISED);
    expected.put("chart", SearchTemplateGates.ENTITY_NOT_ENABLED);
    assertEquals(
        documentOnly.entityGates(vector, List.of("document", "dataset", "chart"), OPENSEARCH),
        expected);
    // A profile that is not on this engine fails coverage and does not fall back.
    assertEquals(
        documentOnly
            .entityGates(
                merged(retrieval("vector")).profiles(replace(CONNECTOR.id())).build(),
                List.of("document"),
                ELASTICSEARCH)
            .get("document"),
        EmbeddingProfileRegistry.SYSTEM_UNAVAILABLE);
    // A scope keeps only its own types.
    assertEquals(
        documentOnly
            .entityGates(
                merged(retrieval("vector"))
                    .scope(new SearchTemplateScope().setEntityTypes(ids("document")))
                    .build(),
                List.of("document", "dataset"),
                OPENSEARCH)
            .keySet(),
        Set.of("document"));
  }

  @Test
  public void testOnlyEntitiesThatJoinTheLegCountTowardOneProfile() {
    MergedSearchTemplate vector = merged(retrieval("vector")).build();
    // Dataset is not run by this build, so its default does not make a second profile.
    SearchTemplateGates documentOnly =
        new SearchTemplateGates(
            REGISTRY,
            SEMANTIC,
            true,
            Set.of(VECTOR_RETRIEVAL),
            Map.of(EmbeddingProfile.TEXT_CHUNKS, Set.of("document")));
    assertNull(
        documentOnly.bindingRejection(vector, List.of("document", "dataset"), OPENSEARCH, true));
    // Nor when semantic search does not enable it.
    SearchTemplateGates documentEnabled =
        new SearchTemplateGates(
            REGISTRY,
            semantic("document"),
            true,
            Set.of(VECTOR_RETRIEVAL),
            Map.of(EmbeddingProfile.TEXT_CHUNKS, Set.of("document", "dataset")));
    assertNull(
        documentEnabled.bindingRejection(vector, List.of("document", "dataset"), OPENSEARCH, true));
  }

  @Test
  public void testASecondSelectedProfileCountsEvenWhenItIsUnavailable() {
    // The connector profile is on OpenSearch only; on Elasticsearch it still makes two profiles.
    MergedSearchTemplate added =
        merged(retrieval("vector"))
            .profiles(new SearchTemplateProfiles().setAdd(ids(CONNECTOR.id())))
            .build();
    assertEquals(
        VECTOR_BUILD.bindingRejection(added, List.of("document"), ELASTICSEARCH, true),
        SearchTemplateGates.SEVERAL_PROFILES);
    MergedSearchTemplate unknown =
        merged(retrieval("vector"))
            .profiles(new SearchTemplateProfiles().setAdd(ids("a", "b")))
            .build();
    assertEquals(
        VECTOR_BUILD.bindingRejection(unknown, List.of("document"), OPENSEARCH, true),
        SearchTemplateGates.SEVERAL_PROFILES);
  }

  @Test
  public void testScopedKeywordRerankIgnoresItsSelectionWhileTheRequestCannotRerank() {
    MergedSearchTemplate scoped =
        merged(retrieval("keywordRerank"))
            .scope(new SearchTemplateScope().setEntityTypes(ids("document")))
            .profiles(replace("cohere_embed_v3"))
            .build();
    assertNull(VECTOR_BUILD.rejectionCode(scoped, false));
    assertEquals(VECTOR_BUILD.rejectionCode(scoped, true), SearchTemplateGates.PROFILE_SCOPE);
  }

  @Test
  public void testDeprecatedModelAndFieldNameOneProfile() {
    MergedSearchTemplate named =
        merged(retrieval("keywordRerank"))
            .knn("{ \"model\": \"dataset_l2\", \"field\": \"dataset_l2\" }")
            .build();
    assertEquals(SearchTemplateGates.legacyProfileId(named), "dataset_l2");
    assertEquals(SearchTemplateGates.selection(named).getReplace(), List.of("dataset_l2"));

    MergedSearchTemplate split = merged(retrieval("vector").setModel("a").setField("b")).build();
    assertThrows(SearchTemplateException.class, () -> SearchTemplateGates.legacyProfileId(split));
    // Every explicit combine reads the knn names, and other Mustache tags do not hide them.
    assertEquals(
        SearchTemplateGates.legacyProfileId(
            merged(retrieval("vector")).knn("{ \"model\": \"dataset_l2\" }").build()),
        "dataset_l2");
    MergedSearchTemplate templated =
        merged(retrieval("keywordRerank"))
            .knn("{ \"model\": \"a\", \"field\": \"b\", \"k\": {{fuzzyMaxExpansions}} }")
            .build();
    assertThrows(
        SearchTemplateException.class, () -> SearchTemplateGates.legacyProfileId(templated));

    // A legacy knn with no combine keeps its field as opaque syntax.
    MergedSearchTemplate legacy =
        merged(null).knn("{ \"field\": \"embeddings.vector\", \"k\": 10 }").build();
    assertNull(SearchTemplateGates.legacyProfileId(legacy));
    // profiles wins over the deprecated names.
    assertEquals(
        SearchTemplateGates.selection(
                merged(retrieval("vector").setModel("a")).profiles(replace("b")).build())
            .getReplace(),
        List.of("b"));
  }

  @Test
  public void testKnnNamesOnAnExplicitCombineAreNotTheLegacyRerank() {
    MergedSearchTemplate named =
        merged(retrieval("vector")).knn("{ \"model\": \"dataset_l2\" }").build();
    assertNull(
        SearchTemplateCapabilities.rejectionCode(
            named, false, false, SearchTemplateComponentRegistry.empty(), VECTOR_BUILD));
    // The legacy shape, with no combine, still needs document rerank.
    MergedSearchTemplate legacy =
        merged(null).knn("{ \"field\": \"embeddings.vector\", \"k\": 10 }").build();
    assertEquals(
        SearchTemplateCapabilities.rejectionCode(
            legacy, false, false, SearchTemplateComponentRegistry.empty(), VECTOR_BUILD),
        "knn");
    assertNull(SearchTemplateCapabilities.effectiveCombine(legacy));

    // With both model and field it is the earlier keywordRerank shape: no document rerank needed,
    // and its names stay opaque, so it resolves the entity default.
    MergedSearchTemplate migrated =
        merged(null)
            .knn("{ \"model\": \"dataset_l2\", \"field\": \"embeddings.vector\", \"k\": 10 }")
            .build();
    assertTrue(SearchTemplateCapabilities.keywordRerank(migrated));
    assertNull(
        SearchTemplateCapabilities.rejectionCode(
            migrated, false, false, SearchTemplateComponentRegistry.empty(), VECTOR_BUILD));
    assertNull(SearchTemplateGates.legacyProfileId(migrated));
    assertEquals(
        VECTOR_BUILD.joinedProfileIds(migrated, List.of("document"), OPENSEARCH),
        Set.of(DOCUMENT_DEFAULT.id()));
    // A blank model is not the migrated shape.
    assertNull(
        SearchTemplateCapabilities.effectiveCombine(
            merged(null).knn("{ \"model\": \" \", \"field\": \"embeddings.vector\" }").build()));
  }

  @Test
  public void testASecondProfileOfAnotherClassStillCounts() {
    EmbeddingProfile card =
        new EmbeddingProfile(
            "entityMetadata.v1",
            "entityMetadata",
            null,
            "cosine",
            Set.of("document"),
            Set.of(ELASTICSEARCH, OPENSEARCH));
    SearchTemplateGates textChunksOnly =
        new SearchTemplateGates(
            new EmbeddingProfileRegistry(
                List.of(DOCUMENT_DEFAULT, card), Map.of("document", DOCUMENT_DEFAULT.id())),
            SEMANTIC,
            true,
            Set.of(VECTOR_RETRIEVAL),
            Map.of(EmbeddingProfile.TEXT_CHUNKS, Set.of("document")));
    MergedSearchTemplate mixed =
        merged(retrieval("vector"))
            .profiles(new SearchTemplateProfiles().setAdd(ids(card.id())))
            .build();
    assertEquals(
        textChunksOnly.bindingRejection(mixed, List.of("document"), OPENSEARCH, true),
        SearchTemplateGates.SEVERAL_PROFILES);
  }

  @Test
  public void testAProfilesResetSupersedesTheDeprecatedNames() {
    MergedSearchTemplate reset =
        merged(retrieval("keywordRerank"))
            .knn("{ \"model\": \"dataset_l2\" }")
            .profilesSet(true)
            .build();
    assertNull(SearchTemplateGates.selection(reset));
  }

  @Test
  public void testAdvertisedNamesAreKnownCapabilities() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new SearchTemplateGates(REGISTRY, SEMANTIC, true, Set.of("teleport"), Map.of()));
  }

  private static SemanticSearchConfiguration semantic(String... entityTypes) {
    SemanticSearchConfiguration semanticSearch = new SemanticSearchConfiguration();
    semanticSearch.setEnabled(true);
    semanticSearch.setEnabledEntities(Set.of(entityTypes));
    semanticSearch.setModels(Map.of(DOCUMENT_DEFAULT.id(), new ModelEmbeddingConfig()));
    return semanticSearch;
  }

  private static MergedSearchTemplate.MergedSearchTemplateBuilder merged(
      SearchTemplateRetrieval retrieval) {
    return MergedSearchTemplate.builder()
        .urn(URN)
        .dialect(SearchTemplateDialect.ELASTICSEARCH.name())
        .apiVersion(SearchTemplateCapabilities.SEMANTIC_API_VERSION)
        .queryFragments(Map.of())
        .parameters(Map.of())
        .chain(List.of(URN))
        .retrieval(retrieval);
  }

  private static SearchTemplateRetrieval retrieval(String combine) {
    return new SearchTemplateRetrieval().setCombine(combine);
  }

  private static SearchTemplateProfiles replace(String... ids) {
    return new SearchTemplateProfiles().setReplace(ids(ids));
  }

  private static StringArray ids(String... ids) {
    return new StringArray(List.of(ids));
  }
}

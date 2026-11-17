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
          Set.of(ELASTICSEARCH, OPENSEARCH));
  private static final EmbeddingProfileRegistry REGISTRY =
      new EmbeddingProfileRegistry(
          List.of(DOCUMENT_DEFAULT, DATASET_L2, CONNECTOR),
          Map.of("document", DOCUMENT_DEFAULT.id(), "dataset", DATASET_L2.id()));

  /** A build that runs vector retrieval and partitioning for Document and Dataset text chunks. */
  private static final SearchTemplateGates VECTOR_BUILD =
      new SearchTemplateGates(
          REGISTRY,
          Set.of(VECTOR_RETRIEVAL, ENTITY_PARTITION, DOCUMENT_RERANK),
          Map.of(EmbeddingProfile.TEXT_CHUNKS, Set.of("document")));

  @Test
  public void testThisBuildAdvertisesNoSemanticShapeYet() {
    SearchTemplateGates gates = SearchTemplateGates.of(REGISTRY);
    assertEquals(gates.rejectionCode(merged(retrieval("vector")).build(), true), VECTOR_RETRIEVAL);
    // Anchor similarity needs vector retrieval and similarity.
    MergedSearchTemplate anchored =
        merged(retrieval("vector").setQuerySource("anchorEntity")).build();
    assertEquals(gates.rejectionCode(anchored, true), VECTOR_RETRIEVAL);
    assertEquals(VECTOR_BUILD.rejectionCode(anchored, true), SIMILARITY);
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
    // This build does not rerank, so the selection only the rerank uses does not fail the search.
    SearchTemplateGates gates = SearchTemplateGates.of(REGISTRY);
    assertNull(gates.rejectionCode(rerank, true));
    assertNull(gates.bindingRejection(rerank, List.of("document"), OPENSEARCH, true));

    // A build that reranks still keeps keyword order when this request cannot rerank.
    SearchTemplateGates reranking =
        new SearchTemplateGates(
            REGISTRY, Set.of(DOCUMENT_RERANK), SearchTemplateGates.ADVERTISED_ENTITY_TYPES);
    assertNull(reranking.rejectionCode(rerank, false));
    assertNull(reranking.bindingRejection(rerank, List.of("document"), OPENSEARCH, false));

    // When it can, combining several profiles is a capability it does not run.
    assertEquals(reranking.rejectionCode(rerank, true), SearchTemplateGates.SEVERAL_PROFILES);
    MergedSearchTemplate twoProfiles =
        merged(retrieval("keywordRerank"))
            .profiles(replace(DOCUMENT_DEFAULT.id(), CONNECTOR.id()))
            .build();
    assertNull(reranking.rejectionCode(twoProfiles, true));
    assertEquals(
        reranking.bindingRejection(twoProfiles, List.of("document"), OPENSEARCH, true),
        SearchTemplateGates.SEVERAL_PROFILES);
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
    // An entity that fails coverage adds no profile.
    assertNull(
        VECTOR_BUILD.bindingRejection(vector, List.of("document", "chart"), OPENSEARCH, true));

    MergedSearchTemplate floor = merged(retrieval("vector").setMinSimilarity(0.8f)).build();
    assertNull(VECTOR_BUILD.bindingRejection(floor, List.of("document"), OPENSEARCH, true));
    assertEquals(
        VECTOR_BUILD.bindingRejection(floor, List.of("dataset"), OPENSEARCH, true),
        SearchTemplateGates.MIN_SIMILARITY);
    // The same template on a build without vector retrieval fails earlier, by capability.
    assertNull(
        SearchTemplateGates.of(REGISTRY)
            .bindingRejection(floor, List.of("dataset"), OPENSEARCH, true));
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
    SemanticSearchConfiguration semanticSearch = new SemanticSearchConfiguration();
    semanticSearch.setEnabled(true);
    semanticSearch.setEnabledEntities(Set.of("document", "dataset"));
    semanticSearch.setModels(Map.of(DOCUMENT_DEFAULT.id(), new ModelEmbeddingConfig()));
    MergedSearchTemplate vector = merged(retrieval("vector")).build();

    Map<String, String> expected = new HashMap<>();
    expected.put("document", null);
    expected.put("dataset", SearchTemplateGates.ENTITY_NOT_ADVERTISED);
    expected.put("chart", EmbeddingProfileRegistry.NO_DEFAULT);
    assertEquals(
        VECTOR_BUILD.entityGates(
            vector, List.of("document", "dataset", "chart"), OPENSEARCH, semanticSearch),
        expected);

    semanticSearch.setEnabledEntities(Set.of("dataset"));
    assertEquals(
        VECTOR_BUILD
            .entityGates(vector, List.of("document"), OPENSEARCH, semanticSearch)
            .get("document"),
        SearchTemplateGates.ENTITY_NOT_ENABLED);
    // A scope keeps only its own types.
    assertEquals(
        VECTOR_BUILD
            .entityGates(
                merged(retrieval("vector"))
                    .scope(new SearchTemplateScope().setEntityTypes(ids("document")))
                    .build(),
                List.of("document", "dataset"),
                OPENSEARCH,
                semanticSearch)
            .keySet(),
        Set.of("document"));
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
  public void testAdvertisedNamesAreKnownCapabilities() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new SearchTemplateGates(REGISTRY, Set.of("teleport"), Map.of()));
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

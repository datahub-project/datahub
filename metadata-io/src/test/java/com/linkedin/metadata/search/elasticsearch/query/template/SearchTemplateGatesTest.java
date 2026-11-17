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
  private static final EmbeddingProfileRegistry REGISTRY =
      new EmbeddingProfileRegistry(
          List.of(DOCUMENT_DEFAULT, DATASET_L2),
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
    assertEquals(gates.rejectionCode(merged(retrieval("vector")).build()), VECTOR_RETRIEVAL);
    // Anchor similarity needs vector retrieval and similarity.
    MergedSearchTemplate anchored =
        merged(retrieval("vector").setQuerySource("anchorEntity")).build();
    assertEquals(gates.rejectionCode(anchored), VECTOR_RETRIEVAL);
    assertEquals(VECTOR_BUILD.rejectionCode(anchored), SIMILARITY);
    assertEquals(
        gates.rejectionCode(merged(retrieval("vectorThenKeyword")).build()),
        SearchTemplateGates.VECTOR_THEN_KEYWORD);
    assertEquals(
        gates.rejectionCode(
            merged(retrieval("keywordRerank"))
                .scope(new SearchTemplateScope().setEntityTypes(ids("document")))
                .build()),
        ENTITY_PARTITION);
    assertEquals(
        gates.rejectionCode(merged(null).query("{ {{&embeddingClausesJson}} }").build()),
        SearchTemplateGates.NATIVE_MIXED_QUERY);
    assertEquals(
        gates.rejectionCode(merged(retrieval("keywordRerank").setVectorBody("{}")).build()),
        SearchTemplateGates.VECTOR_BODY);
    assertEquals(
        gates.rejectionCode(merged(null).profiles(replace(DOCUMENT_DEFAULT.id())).build()),
        SearchTemplateGates.KEYWORD_PROFILES);
  }

  @Test
  public void testKeywordRerankBindsWhileThisBuildCannotRerank() {
    SearchTemplateGates gates = SearchTemplateGates.of(REGISTRY);
    MergedSearchTemplate rerank =
        merged(retrieval("keywordRerank").setProfileScore("weightedSum"))
            .profiles(replace("a", "b"))
            .build();
    assertNull(gates.rejectionCode(rerank));
    assertNull(gates.bindingRejection(rerank, List.of("document"), ELASTICSEARCH));

    // Once the build reranks, a second profile is a capability it does not run.
    SearchTemplateGates reranking =
        new SearchTemplateGates(
            REGISTRY, Set.of(DOCUMENT_RERANK), SearchTemplateGates.ADVERTISED_ENTITY_TYPES);
    assertEquals(reranking.rejectionCode(rerank), SearchTemplateGates.SEVERAL_PROFILES);
  }

  @Test
  public void testResolvedProfilesFailTheTemplateOnlyWhenTheLegRuns() {
    MergedSearchTemplate vector = merged(retrieval("vector")).build();
    assertNull(VECTOR_BUILD.rejectionCode(vector));
    assertNull(VECTOR_BUILD.bindingRejection(vector, List.of("document"), OPENSEARCH));
    // Two entity defaults make two profiles in one leg.
    assertEquals(
        VECTOR_BUILD.bindingRejection(vector, List.of("document", "dataset"), OPENSEARCH),
        SearchTemplateGates.SEVERAL_PROFILES);
    // An entity that fails coverage adds no profile.
    assertNull(VECTOR_BUILD.bindingRejection(vector, List.of("document", "chart"), OPENSEARCH));

    MergedSearchTemplate floor = merged(retrieval("vector").setMinSimilarity(0.8f)).build();
    assertNull(VECTOR_BUILD.bindingRejection(floor, List.of("document"), OPENSEARCH));
    assertEquals(
        VECTOR_BUILD.bindingRejection(floor, List.of("dataset"), OPENSEARCH),
        SearchTemplateGates.MIN_SIMILARITY);
    // The same template on a build without vector retrieval fails earlier, by capability.
    assertNull(
        SearchTemplateGates.of(REGISTRY).bindingRejection(floor, List.of("dataset"), OPENSEARCH));
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
                .build()),
        SearchTemplateGates.PROFILE_SCOPE);
    assertNull(
        VECTOR_BUILD.rejectionCode(
            merged(retrieval("vector"))
                .scope(new SearchTemplateScope().setEntityTypes(ids("document")))
                .profiles(replace(DOCUMENT_DEFAULT.id()))
                .build()));
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

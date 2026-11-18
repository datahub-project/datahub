package com.linkedin.metadata.search.embedding;

import static com.linkedin.metadata.search.embedding.EmbeddingProfileRegistry.ELASTICSEARCH;
import static com.linkedin.metadata.search.embedding.EmbeddingProfileRegistry.OPENSEARCH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.config.search.EmbeddingProviderConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.search.template.SearchTemplateProfiles;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class EmbeddingProfileRegistryTest {
  private static final EmbeddingProfile DOCUMENT_DEFAULT =
      new EmbeddingProfile(
          "text_embedding_3_large",
          EmbeddingProfile.TEXT_CHUNKS,
          "text-embedding-3-large",
          "cosinesimil",
          Set.of("document"),
          Set.of(ELASTICSEARCH, OPENSEARCH));
  private static final EmbeddingProfile CONNECTOR =
      new EmbeddingProfile(
          "textChunks.v2",
          EmbeddingProfile.TEXT_CHUNKS,
          null,
          "cosine",
          Set.of("document"),
          Set.of(OPENSEARCH));
  private static final EmbeddingProfile DATASET_CARD =
      new EmbeddingProfile(
          "entityMetadata.v1",
          "entityMetadata",
          null,
          "l2",
          Set.of("dataset"),
          Set.of(ELASTICSEARCH, OPENSEARCH));

  private static final EmbeddingProfileRegistry REGISTRY =
      new EmbeddingProfileRegistry(
          List.of(DOCUMENT_DEFAULT, CONNECTOR, DATASET_CARD),
          Map.of("document", DOCUMENT_DEFAULT.id(), "dataset", DATASET_CARD.id()));

  @Test
  public void testUnsetAndPatchesResolveFromTheEntityDefault() {
    assertEquals(resolve(null, "document", OPENSEARCH), List.of(DOCUMENT_DEFAULT.id()));
    assertEquals(
        resolve(new SearchTemplateProfiles().setAdd(ids("textChunks.v2")), "document", OPENSEARCH),
        List.of(DOCUMENT_DEFAULT.id(), CONNECTOR.id()));
    SearchTemplateProfiles swap =
        new SearchTemplateProfiles()
            .setRemove(ids(DOCUMENT_DEFAULT.id()))
            .setAdd(ids("textChunks.v2"));
    assertEquals(resolve(swap, "document", OPENSEARCH), List.of(CONNECTOR.id()));
    assertEquals(
        REGISTRY
            .resolve(
                new SearchTemplateProfiles().setRemove(ids(DOCUMENT_DEFAULT.id())),
                "document",
                OPENSEARCH)
            .failure(),
        EmbeddingProfileRegistry.EMPTY_SELECTION);
  }

  @Test
  public void testNamedProfileOutsideItsTypesFallsBackToTheDefaultOnce() {
    // The dataset card does not cover Document, so Document resolves to its own default, once.
    SearchTemplateProfiles named =
        new SearchTemplateProfiles().setReplace(ids("entityMetadata.v1", DOCUMENT_DEFAULT.id()));
    assertEquals(resolve(named, "document", ELASTICSEARCH), List.of(DOCUMENT_DEFAULT.id()));
    assertEquals(resolve(named, "dataset", ELASTICSEARCH), List.of(DATASET_CARD.id()));
  }

  @Test
  public void testARemovedDefaultIsNotAddedBackAsAFallback() {
    // Removing Document's default and adding the dataset card leaves Document nothing to search
    EmbeddingProfileRegistry.Resolution removed =
        REGISTRY.resolve(
            new SearchTemplateProfiles()
                .setRemove(ids(DOCUMENT_DEFAULT.id()))
                .setAdd(ids(DATASET_CARD.id())),
            "document",
            ELASTICSEARCH);
    assertEquals(removed.failure(), EmbeddingProfileRegistry.EMPTY_SELECTION);
    // A covering id that remains is still selected
    assertEquals(
        resolve(
            new SearchTemplateProfiles()
                .setRemove(ids(DOCUMENT_DEFAULT.id()))
                .setAdd(ids(DATASET_CARD.id(), CONNECTOR.id())),
            "document",
            OPENSEARCH),
        List.of(CONNECTOR.id()));
  }

  @Test
  public void testNamedProfileOutsideItsTypesIsDroppedWhenTheEntityHasNoDefault() {
    EmbeddingProfile chartCard =
        new EmbeddingProfile(
            "chartMetadata.v1",
            "entityMetadata",
            null,
            "cosine",
            Set.of("chart"),
            Set.of(ELASTICSEARCH, OPENSEARCH));
    EmbeddingProfileRegistry registry =
        new EmbeddingProfileRegistry(
            List.of(DATASET_CARD, chartCard), Map.of("dataset", DATASET_CARD.id()));

    // Chart has no default: the dataset card does not cover it and is dropped, and the chart card
    // still resolves.
    EmbeddingProfileRegistry.Resolution mixed =
        registry.resolve(
            new SearchTemplateProfiles().setReplace(ids(DATASET_CARD.id(), chartCard.id())),
            "chart",
            OPENSEARCH);
    assertTrue(mixed.covered());
    assertEquals(mixed.profileIds(), List.of(chartCard.id()));

    // With no selected profile covering it, the entity needs the default it does not have.
    assertEquals(
        registry
            .resolve(
                new SearchTemplateProfiles().setReplace(ids(DATASET_CARD.id())),
                "chart",
                OPENSEARCH)
            .failure(),
        EmbeddingProfileRegistry.NO_DEFAULT);
  }

  @Test
  public void testUncoveredSystemAndUnknownIdsDoNotFallBack() {
    SearchTemplateProfiles connector =
        new SearchTemplateProfiles().setReplace(ids("textChunks.v2"));
    assertEquals(
        REGISTRY.resolve(connector, "document", ELASTICSEARCH).failure(),
        EmbeddingProfileRegistry.SYSTEM_UNAVAILABLE);
    assertEquals(
        REGISTRY
            .resolve(
                new SearchTemplateProfiles().setReplace(ids("missing")), "document", OPENSEARCH)
            .failure(),
        EmbeddingProfileRegistry.UNKNOWN_PROFILE);
    assertEquals(
        REGISTRY.resolve(null, "chart", OPENSEARCH).failure(), EmbeddingProfileRegistry.NO_DEFAULT);
    assertEquals(
        REGISTRY.resolve(null, "document", null).failure(),
        EmbeddingProfileRegistry.SYSTEM_UNAVAILABLE);
  }

  @Test
  public void testDefaultsMustCoverTheirEntity() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new EmbeddingProfileRegistry(List.of(CONNECTOR), Map.of("dataset", CONNECTOR.id())));
    assertThrows(
        IllegalArgumentException.class,
        () -> new EmbeddingProfileRegistry(List.of(CONNECTOR, CONNECTOR), Map.of()));
  }

  @Test
  public void testRegistryFromTheSemanticSearchConfiguration() {
    SemanticSearchConfiguration semanticSearch = new SemanticSearchConfiguration();
    semanticSearch.setEnabled(true);
    semanticSearch.setEnabledEntities(Set.of("document", "dataset"));
    ModelEmbeddingConfig model = new ModelEmbeddingConfig();
    model.setSpaceType("l2");
    semanticSearch.setModels(Map.of("cohere_embed_v3", model));
    EmbeddingProviderConfiguration provider = new EmbeddingProviderConfiguration();
    provider.setType("aws-bedrock");
    semanticSearch.setEmbeddingProvider(provider);

    EmbeddingProfileRegistry registry =
        EmbeddingProfileRegistry.fromSemanticSearch(semanticSearch, "cohere_embed_v3");
    EmbeddingProfile profile = registry.profile("cohere_embed_v3");
    assertEquals(profile.profileClass(), EmbeddingProfile.TEXT_CHUNKS);
    assertEquals(profile.entityTypes(), Set.of("document"));
    assertEquals(profile.systems(), Set.of(ELASTICSEARCH, OPENSEARCH));
    assertTrue(!profile.cosine());
    assertEquals(registry.defaultFor("document"), "cohere_embed_v3");
    // enabledEntities is a separate gate; it does not widen the profile.
    assertNull(registry.defaultFor("dataset"));

    assertNull(
        EmbeddingProfileRegistry.fromSemanticSearch(semanticSearch, "other_key")
            .defaultFor("document"));
    semanticSearch.setEnabled(false);
    assertNull(
        EmbeddingProfileRegistry.fromSemanticSearch(semanticSearch, "cohere_embed_v3")
            .defaultFor("document"));
  }

  @Test
  public void testSystemNames() {
    assertEquals(
        EmbeddingProfileRegistry.system(SearchClientShim.SearchEngineType.OPENSEARCH_3),
        OPENSEARCH);
    assertEquals(
        EmbeddingProfileRegistry.system(SearchClientShim.SearchEngineType.ELASTICSEARCH_8),
        ELASTICSEARCH);
    assertNull(EmbeddingProfileRegistry.system(SearchClientShim.SearchEngineType.UNKNOWN));
    assertNull(EmbeddingProfileRegistry.system(null));
  }

  private static List<String> resolve(
      SearchTemplateProfiles selection, String entityType, String system) {
    EmbeddingProfileRegistry.Resolution resolution =
        REGISTRY.resolve(selection, entityType, system);
    assertTrue(resolution.covered(), String.valueOf(resolution.failure()));
    return resolution.profileIds();
  }

  private static StringArray ids(String... ids) {
    return new StringArray(List.of(ids));
  }
}

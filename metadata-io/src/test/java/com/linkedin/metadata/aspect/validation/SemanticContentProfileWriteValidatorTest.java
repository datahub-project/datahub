package com.linkedin.metadata.aspect.validation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.EmbeddingChunk;
import com.linkedin.common.EmbeddingChunkArray;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.EmbeddingModelDataMap;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.SemanticVectorState;
import com.linkedin.common.SemanticWorkState;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.FloatArray;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.semantic.profile.ContentBinding;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileRegistry;
import com.linkedin.metadata.search.semantic.profile.SemanticVectorGate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class SemanticContentProfileWriteValidatorTest {

  @Test
  public void testMarkerOnlyChangeIsAllowedWhileGenerationIsOn() {
    EmbeddingModelData before = profile(false);
    EmbeddingModelData after = profile(false).setCurrent(false);

    assertTrue(messages(content("svc", before), content("svc", after), true, false).isEmpty());
  }

  @Test
  public void testSourceDigestChangeIsRejectedForAServiceProfile() {
    EmbeddingModelData before = profile(false);
    EmbeddingModelData after = profile(false).setSourceTextSha256("abc");

    assertFalse(messages(content("svc", before), content("svc", after), true, false).isEmpty());
    assertTrue(messages(content("svc", before), content("svc", after), true, true).isEmpty());
  }

  @Test
  public void testVectorChangeIsRejectedForAServiceProfile() {
    EmbeddingModelData before = profile(false);
    EmbeddingModelData after = profile(true);

    assertFalse(messages(content("svc", before), content("svc", after), true, false).isEmpty());
  }

  @Test
  public void testVectorChangeIsAllowedWhenGenerationIsOff() {
    assertTrue(
        messages(content("svc", profile(false)), content("svc", profile(true)), false, false)
            .isEmpty());
  }

  @Test
  public void testApplyMayChangeServiceProfileVectors() {
    assertTrue(
        messages(content("svc", profile(false)), content("svc", profile(true)), true, true)
            .isEmpty());
  }

  @Test
  public void testWorkStateChangeIsRejectedForAServiceProfile() {
    EmbeddingModelData after = profile(false).setWorkState(SemanticWorkState.PENDING);

    assertFalse(
        messages(content("svc", profile(false)), content("svc", after), true, false).isEmpty());
  }

  @Test
  public void testReadyRequiresChunks() {
    EmbeddingModelData empty =
        profile(false)
            .setVectorState(SemanticVectorState.READY)
            .setChunks(new EmbeddingChunkArray());

    assertFalse(messages(null, content("other", empty), false, false).isEmpty());
  }

  @Test
  public void testAnUnconfiguredProfileCannotBeAdded() {
    assertFalse(
        configuredMessages(null, content("retired", profile(true)), false, true, false).isEmpty());
    assertFalse(
        configuredMessages(null, content("retired", profile(true)), false, true, true).isEmpty());
  }

  @Test
  public void testAStoredUnconfiguredProfileCannotChangeItsVectors() {
    assertFalse(
        configuredMessages(
                content("retired", profile(false)),
                content("retired", profile(true)),
                false,
                true,
                false)
            .isEmpty());
  }

  @Test
  public void testAStoredUnconfiguredProfileAcceptsMarkerChanges() {
    assertTrue(
        configuredMessages(
                content("retired", profile(false)),
                content("retired", profile(false).setCurrent(false)),
                false,
                true,
                false)
            .isEmpty());
  }

  @Test
  public void testAStoredUnconfiguredProfileCanBeRemoved() {
    assertTrue(
        configuredMessages(
                content("retired", profile(true)), new SemanticContent(), false, true, false)
            .isEmpty());
  }

  @Test
  public void testAnUnconfiguredProfileIsAcceptedWhenNotRequired() {
    assertTrue(
        configuredMessages(null, content("retired", profile(true)), false, false, false).isEmpty());
  }

  @Test
  public void testMarkerChangeWithStoredDoubleVectorsIsAllowed() {
    EmbeddingModelData stored = profileWithJsonVector("0.1", "0.2");
    EmbeddingModelData proposed = profileWithVector(0.1f, 0.2f).setCurrent(false);

    assertTrue(messages(content("svc", stored), content("svc", proposed), true, false).isEmpty());
  }

  @Test
  public void testChangedVectorValueIsRejectedWhenStoredAsDouble() {
    EmbeddingModelData stored = profileWithJsonVector("0.1", "0.2");
    EmbeddingModelData proposed = profileWithVector(0.1f, 0.3f);

    assertFalse(messages(content("svc", stored), content("svc", proposed), true, false).isEmpty());
  }

  @Test
  public void testAVectorWithTheWrongDimensionIsRejected() {
    EmbeddingModelData wide = profileWithVector(0.1f, 0.2f);

    assertFalse(
        gatedMessages(null, content("svc", wide), false, false, true, false, entity(true))
            .isEmpty());
  }

  @Test
  public void testAProfileOnAnEntityThatIsNotEnabledCannotBeAddedOrChanged() {
    EntitySpec disabled = entity(false);

    assertFalse(
        gatedMessages(null, content("svc", profile(true)), false, false, true, false, disabled)
            .isEmpty());
    assertFalse(
        gatedMessages(
                content("svc", profile(false)),
                content("svc", profile(true)),
                false,
                false,
                true,
                false,
                disabled)
            .isEmpty());
  }

  @Test
  public void testAStoredProfileOnADisabledEntityCannotTakeMarkerChanges() {
    assertFalse(
        gatedMessages(
                content("svc", profile(true)),
                content("svc", profile(true).setCurrent(false)),
                false,
                false,
                true,
                false,
                entity(false))
            .isEmpty());
  }

  @Test
  public void testAProfileOnADisabledEntityCanBeRemovedOrDeleted() {
    EntitySpec disabled = entity(false);

    assertTrue(
        gatedMessages(
                content("svc", profile(true)),
                new SemanticContent(),
                false,
                false,
                true,
                false,
                disabled)
            .isEmpty());
    assertTrue(
        gatedMessages(content("svc", profile(true)), null, true, false, true, false, disabled)
            .isEmpty());
  }

  @Test
  public void testAConfiguredProfileIsUnaffected() {
    assertTrue(
        configuredMessages(null, content("svc", profile(true)), false, true, true).isEmpty());
  }

  private static Set<String> messages(
      SemanticContent previous, SemanticContent proposed, boolean generation, boolean apply) {
    return configuredMessages(previous, proposed, generation, false, apply);
  }

  private static Set<String> configuredMessages(
      SemanticContent previous,
      SemanticContent proposed,
      boolean generation,
      boolean requireConfigured,
      boolean apply) {
    return gatedMessages(
        previous, proposed, false, generation, requireConfigured, apply, entity(true));
  }

  private static Set<String> gatedMessages(
      SemanticContent previous,
      SemanticContent proposed,
      boolean delete,
      boolean generation,
      boolean requireConfigured,
      boolean apply,
      EntitySpec entitySpec) {
    return SemanticContentProfileWriteValidator.violationMessages(
        "document",
        previous,
        proposed,
        delete,
        apply,
        requireConfigured,
        registry(generation),
        new SemanticVectorGate(Set.of(), Map.of("svc", 1)),
        entitySpec);
  }

  private static EntitySpec entity(boolean eligible) {
    EntitySpec entitySpec = mock(EntitySpec.class);
    when(entitySpec.getName()).thenReturn("document");
    when(entitySpec.getSearchGroup()).thenReturn(eligible ? "primary" : "default");
    return entitySpec;
  }

  private static SemanticContent content(String key, EmbeddingModelData profile) {
    EmbeddingModelDataMap embeddings = new EmbeddingModelDataMap();
    embeddings.put(key, profile);
    return new SemanticContent().setEmbeddings(embeddings);
  }

  private static EmbeddingModelData profile(boolean withChunk) {
    EmbeddingModelData profile =
        new EmbeddingModelData()
            .setModelVersion("model")
            .setGeneratedAt(1L)
            .setTotalChunks(withChunk ? 1 : 0)
            .setChunks(new EmbeddingChunkArray());
    if (withChunk) {
      profile
          .getChunks()
          .add(new EmbeddingChunk().setPosition(0).setVector(new FloatArray(List.of(0.1f))));
    }
    return profile;
  }

  /** A profile as read back from stored JSON, where vector elements are {@code Double}. */
  private static EmbeddingModelData profileWithJsonVector(String... values) {
    DataList vector = new DataList();
    for (String value : values) {
      vector.add(Double.parseDouble(value));
    }
    DataMap chunk = new DataMap();
    chunk.put("position", 0);
    chunk.put("vector", vector);
    EmbeddingChunkArray chunks = new EmbeddingChunkArray();
    chunks.add(new EmbeddingChunk(chunk));
    return profile(false).setTotalChunks(1).setChunks(chunks);
  }

  private static EmbeddingModelData profileWithVector(float... values) {
    List<Float> vector = new java.util.ArrayList<>();
    for (float value : values) {
      vector.add(value);
    }
    EmbeddingChunkArray chunks = new EmbeddingChunkArray();
    chunks.add(new EmbeddingChunk().setPosition(0).setVector(new FloatArray(vector)));
    return profile(false).setTotalChunks(1).setChunks(chunks);
  }

  private static SemanticProfileRegistry registry(boolean generation) {
    return new SemanticProfileRegistry() {
      @Override
      public boolean generationEnabled() {
        return generation;
      }

      @Override
      public boolean isServiceProfile(String entityType, String profileKey) {
        return "svc".equals(profileKey);
      }

      @Override
      public boolean isConfigured(String entityType, String profileKey) {
        return "svc".equals(profileKey);
      }

      @Override
      public ContentBinding bindingFor(String entityType, String profileKey) {
        return ContentBinding.EXTERNAL;
      }
    };
  }
}

package com.linkedin.metadata.aspect.validation;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.EmbeddingChunk;
import com.linkedin.common.EmbeddingChunkArray;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.EmbeddingModelDataMap;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.SemanticVectorState;
import com.linkedin.common.SemanticWorkState;
import com.linkedin.data.template.FloatArray;
import com.linkedin.metadata.search.semantic.profile.ContentBinding;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileRegistry;
import java.util.List;
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

  private static Set<String> messages(
      SemanticContent previous, SemanticContent proposed, boolean generation, boolean apply) {
    return SemanticContentProfileWriteValidator.violationMessages(
        "document", previous, proposed, false, apply, registry(generation));
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
      public ContentBinding bindingFor(String entityType, String profileKey) {
        return ContentBinding.EXTERNAL;
      }
    };
  }
}

package com.linkedin.metadata.search.semantic.profile;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.EmbeddingChunk;
import com.linkedin.common.EmbeddingChunkArray;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.SemanticVectorState;
import com.linkedin.common.SemanticWorkState;
import java.util.List;
import org.testng.annotations.Test;

public class SemanticProfileStatesTest {

  @Test
  public void testLegacyChunksReadAsReady() {
    EmbeddingModelData profile = base().setChunks(chunks(1));

    assertEquals(
        SemanticProfileStates.vectorState(profile).orElseThrow(), SemanticVectorState.READY);
  }

  @Test
  public void testLegacyEmptyChunksAreAbsent() {
    assertTrue(SemanticProfileStates.vectorState(base()).isEmpty());
  }

  @Test
  public void testExplicitStateWins() {
    EmbeddingModelData profile =
        base().setChunks(chunks(1)).setVectorState(SemanticVectorState.EXCLUDED);

    assertEquals(
        SemanticProfileStates.vectorState(profile).orElseThrow(), SemanticVectorState.EXCLUDED);
  }

  @Test
  public void testWorkStateWithoutVectorsIsStillAbsent() {
    EmbeddingModelData profile = base().setWorkState(SemanticWorkState.PENDING);

    assertTrue(SemanticProfileStates.vectorState(profile).isEmpty());
    assertEquals(profile.getWorkState(), SemanticWorkState.PENDING);
  }

  private static EmbeddingModelData base() {
    return new EmbeddingModelData()
        .setModelVersion("model")
        .setGeneratedAt(1L)
        .setTotalChunks(0)
        .setChunks(new EmbeddingChunkArray());
  }

  private static EmbeddingChunkArray chunks(int count) {
    EmbeddingChunkArray array = new EmbeddingChunkArray();
    for (int i = 0; i < count; i++) {
      array.add(
          new EmbeddingChunk()
              .setPosition(i)
              .setVector(new com.linkedin.data.template.FloatArray(List.of(0.1f))));
    }
    return array;
  }
}

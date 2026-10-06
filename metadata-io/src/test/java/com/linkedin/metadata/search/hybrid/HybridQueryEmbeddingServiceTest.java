package com.linkedin.metadata.search.hybrid;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.embedding.EmbeddingTaskType;
import org.testng.annotations.Test;

public class HybridQueryEmbeddingServiceTest {

  @Test
  public void testEmbedsWithConfiguredModelAsRetrievalQuery() {
    EmbeddingProvider provider = mock(EmbeddingProvider.class);
    when(provider.embed("revenue", "text-embedding-3-small", EmbeddingTaskType.QUERY))
        .thenReturn(new float[] {0.1f, 0.2f});

    HybridQueryEmbeddingService.QueryEmbedding embedding =
        new HybridQueryEmbeddingService(
                provider, "text-embedding-3-small", "text_embedding_3_small", 2)
            .embed("revenue");

    assertEquals(embedding.modelEmbeddingKey(), "text_embedding_3_small");
    assertEquals(embedding.vector(), new float[] {0.1f, 0.2f});
  }

  @Test
  public void testRepeatQueryReusesTheEmbedding() {
    EmbeddingProvider provider = mock(EmbeddingProvider.class);
    when(provider.embed("revenue", null, EmbeddingTaskType.QUERY))
        .thenReturn(new float[] {0.1f, 0.2f});
    HybridQueryEmbeddingService service =
        new HybridQueryEmbeddingService(provider, null, "text_embedding_3_small", 2);

    service.embed("revenue");
    service.embed("revenue");

    verify(provider, times(1)).embed("revenue", null, EmbeddingTaskType.QUERY);
  }

  @Test
  public void testRejectsQueryVectorWithWrongDimension() {
    EmbeddingProvider provider = mock(EmbeddingProvider.class);
    when(provider.embed("query", "text-embedding-3-small", EmbeddingTaskType.QUERY))
        .thenReturn(new float[] {0.1f});

    HybridQueryEmbeddingService service =
        new HybridQueryEmbeddingService(
            provider, "text-embedding-3-small", "text_embedding_3_small", 2);

    assertThrows(IllegalStateException.class, () -> service.embed("query"));
  }

  @Test
  public void testQueryEmbeddingDefensivelyCopiesVector() {
    float[] vector = new float[] {0.1f, 0.2f};
    HybridQueryEmbeddingService.QueryEmbedding embedding =
        new HybridQueryEmbeddingService.QueryEmbedding("model", vector);

    vector[0] = 9f;
    float[] returned = embedding.vector();
    returned[1] = 8f;

    assertEquals(embedding.vector(), new float[] {0.1f, 0.2f});
  }
}

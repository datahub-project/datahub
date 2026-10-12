package com.linkedin.metadata.search.hybrid;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.google.common.util.concurrent.UncheckedTimeoutException;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.embedding.EmbeddingTaskType;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class HybridQueryEmbeddingServiceTest {

  private static long inSeconds(long seconds) {
    return System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
  }

  @Test
  public void testEmbedsWithConfiguredModelAsRetrievalQuery() {
    EmbeddingProvider provider = mock(EmbeddingProvider.class);
    when(provider.embed(
            eq("revenue"),
            eq("text-embedding-3-small"),
            eq(EmbeddingTaskType.QUERY),
            any(Duration.class)))
        .thenReturn(new float[] {0.1f, 0.2f});

    HybridQueryEmbeddingService.QueryEmbedding embedding =
        new HybridQueryEmbeddingService(
                provider, "text-embedding-3-small", "text_embedding_3_small", 2)
            .embed("revenue", inSeconds(60));

    assertEquals(embedding.modelEmbeddingKey(), "text_embedding_3_small");
    assertEquals(embedding.vector(), new float[] {0.1f, 0.2f});
  }

  @Test
  public void testProviderGetsTheTimeLeftBeforeTheDeadline() {
    EmbeddingProvider provider = mock(EmbeddingProvider.class);
    when(provider.embed(eq("revenue"), isNull(), eq(EmbeddingTaskType.QUERY), any(Duration.class)))
        .thenReturn(new float[] {0.1f, 0.2f});

    new HybridQueryEmbeddingService(provider, null, "text_embedding_3_small", 2)
        .embed("revenue", inSeconds(10));

    ArgumentCaptor<Duration> timeout = ArgumentCaptor.forClass(Duration.class);
    verify(provider).embed(eq("revenue"), isNull(), eq(EmbeddingTaskType.QUERY), timeout.capture());
    // Close to the whole 10 seconds, and never more
    assertTrue(timeout.getValue().compareTo(Duration.ofSeconds(5)) > 0);
    assertTrue(timeout.getValue().compareTo(Duration.ofSeconds(10)) <= 0);
  }

  @Test
  public void testPassedDeadlineSkipsTheProvider() {
    EmbeddingProvider provider = mock(EmbeddingProvider.class);
    HybridQueryEmbeddingService service =
        new HybridQueryEmbeddingService(provider, null, "text_embedding_3_small", 2);

    assertThrows(
        UncheckedTimeoutException.class, () -> service.embed("revenue", System.nanoTime()));
    verifyNoInteractions(provider);
  }

  @Test
  public void testRepeatQueryReusesTheEmbedding() {
    EmbeddingProvider provider = mock(EmbeddingProvider.class);
    when(provider.embed(eq("revenue"), isNull(), eq(EmbeddingTaskType.QUERY), any(Duration.class)))
        .thenReturn(new float[] {0.1f, 0.2f});
    HybridQueryEmbeddingService service =
        new HybridQueryEmbeddingService(provider, null, "text_embedding_3_small", 2);

    service.embed("revenue", inSeconds(60));
    service.embed("revenue", inSeconds(60));

    verify(provider, times(1))
        .embed(eq("revenue"), isNull(), eq(EmbeddingTaskType.QUERY), any(Duration.class));
  }

  @Test
  public void testRecordsWhenTheProviderLastAnswered() {
    EmbeddingProvider provider = mock(EmbeddingProvider.class);
    when(provider.embed(eq("revenue"), isNull(), eq(EmbeddingTaskType.QUERY), any(Duration.class)))
        .thenThrow(new IllegalStateException("provider down"))
        .thenReturn(new float[] {0.1f, 0.2f});
    HybridQueryEmbeddingService service =
        new HybridQueryEmbeddingService(provider, null, "text_embedding_3_small", 2);
    long before = System.nanoTime();

    assertFalse(service.providerSucceededSince(before));
    assertThrows(IllegalStateException.class, () -> service.embed("revenue", inSeconds(60)));
    // A failed call is no answer
    assertFalse(service.providerSucceededSince(before));
    service.embed("revenue", inSeconds(60));
    assertTrue(service.providerSucceededSince(before));
    // A cached embedding makes no call
    long afterLoad = System.nanoTime() + 1;
    service.embed("revenue", inSeconds(60));
    assertFalse(service.providerSucceededSince(afterLoad));
  }

  @Test
  public void testAnswerAfterTheDeadlineIsCachedButNotRecorded() {
    EmbeddingProvider provider = mock(EmbeddingProvider.class);
    when(provider.embed(eq("revenue"), isNull(), eq(EmbeddingTaskType.QUERY), any(Duration.class)))
        .thenAnswer(
            invocation -> {
              // Answers after the time it was given, as an in-process provider can
              Thread.sleep(((Duration) invocation.getArgument(3)).toMillis() + 50);
              return new float[] {0.1f, 0.2f};
            });
    HybridQueryEmbeddingService service =
        new HybridQueryEmbeddingService(provider, null, "text_embedding_3_small", 2);
    long before = System.nanoTime();

    service.embed("revenue", System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50));
    service.embed("revenue", inSeconds(60));

    // Too slow to say the provider is healthy, but kept for the repeat query
    assertFalse(service.providerSucceededSince(before));
    verify(provider, times(1))
        .embed(eq("revenue"), isNull(), eq(EmbeddingTaskType.QUERY), any(Duration.class));
  }

  @Test
  public void testFailedEmbeddingIsRetriedNotCached() {
    EmbeddingProvider provider = mock(EmbeddingProvider.class);
    when(provider.embed(eq("revenue"), isNull(), eq(EmbeddingTaskType.QUERY), any(Duration.class)))
        .thenThrow(new IllegalStateException("provider down"))
        .thenReturn(new float[] {0.1f})
        .thenReturn(new float[] {0.1f, 0.2f});
    HybridQueryEmbeddingService service =
        new HybridQueryEmbeddingService(provider, null, "text_embedding_3_small", 2);

    assertThrows(IllegalStateException.class, () -> service.embed("revenue", inSeconds(60)));
    // A wrong-sized vector is rejected and not cached either
    assertThrows(IllegalStateException.class, () -> service.embed("revenue", inSeconds(60)));
    assertEquals(service.embed("revenue", inSeconds(60)).vector(), new float[] {0.1f, 0.2f});
  }

  @Test
  public void testRejectsQueryVectorWithWrongDimension() {
    EmbeddingProvider provider = mock(EmbeddingProvider.class);
    when(provider.embed(
            eq("query"),
            eq("text-embedding-3-small"),
            eq(EmbeddingTaskType.QUERY),
            any(Duration.class)))
        .thenReturn(new float[] {0.1f});

    HybridQueryEmbeddingService service =
        new HybridQueryEmbeddingService(
            provider, "text-embedding-3-small", "text_embedding_3_small", 2);

    assertThrows(IllegalStateException.class, () -> service.embed("query", inSeconds(60)));
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

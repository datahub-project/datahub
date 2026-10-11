package com.linkedin.metadata.graph.cache.store;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.graph.cache.config.EntityGraphModel.LocalEvictionLimits;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphView;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

public class EntityGraphLocalViewCacheTest {

  private static final String GRAPH_ID = "domain-partial";
  private static final LocalEvictionLimits LIMITS =
      LocalEvictionLimits.builder().enabled(true).maxViews(16).maxEstimatedBytes(1_000L).build();

  @Test
  public void evictsUntilEstimatedBytesUnderCap() {
    EntityGraphLocalViewCache cache = new EntityGraphLocalViewCache();
    for (int i = 0; i < 4; i++) {
      cache.put(GRAPH_ID, "key-" + i, viewWithEstimatedBytes(320), 1L, LIMITS);
    }

    int present = countPresentKeys(cache);
    assertTrue(present >= 1);
    assertTrue(present * 320L <= LIMITS.getMaxEstimatedBytes());
  }

  @Test
  public void maxViewsEvictsLeastRecentlyUsedView() throws InterruptedException {
    EntityGraphLocalViewCache cache = new EntityGraphLocalViewCache();
    LocalEvictionLimits limits =
        LocalEvictionLimits.builder().enabled(true).maxViews(2).maxEstimatedBytes(0L).build();

    cache.put(GRAPH_ID, "hot", viewWithEstimatedBytes(320), 1L, limits);
    Thread.sleep(5);
    cache.put(GRAPH_ID, "cold", viewWithEstimatedBytes(320), 1L, limits);
    cache.get("hot", 1L);
    Thread.sleep(5);
    cache.put(GRAPH_ID, "new", viewWithEstimatedBytes(320), 1L, limits);

    assertTrue(cache.get("hot", 1L).isPresent());
    assertTrue(cache.get("new", 1L).isPresent());
    assertFalse(cache.get("cold", 1L).isPresent());
  }

  private static int countPresentKeys(EntityGraphLocalViewCache cache) {
    int count = 0;
    for (int i = 0; i < 4; i++) {
      if (cache.get("key-" + i, 1L).isPresent()) {
        count++;
      }
    }
    return count;
  }

  /** Builds a view whose byte estimate is at least {@code targetBytes} (two vertices per edge). */
  private static EntityGraphView viewWithEstimatedBytes(long targetBytes) {
    long edgeCount = Math.max(1L, (targetBytes + 319L) / 320L);
    List<DirectedEdge> edges = new ArrayList<>();
    for (int i = 0; i < edgeCount; i++) {
      edges.add(
          DirectedEdge.builder()
              .sourceUrn("urn:li:domain:child-" + i)
              .destinationUrn("urn:li:domain:parent-" + i)
              .relationshipType("IsPartOf")
              .build());
    }
    EntityGraphView view = new EntityGraphView(edges);
    long estimated = (long) view.vertexCount() * 128L + (long) view.edgeCount() * 64L;
    assertTrue(estimated >= targetBytes);
    return view;
  }
}

package com.linkedin.metadata.graph.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Collections;
import java.util.Set;
import org.testng.annotations.Test;

public class GraphReadResultTest {

  @Test
  public void fromVerticesReturnsEmptyHitForEmptySet() {
    GraphReadResult result = GraphReadResult.fromVertices(Collections.emptySet());

    assertTrue(result instanceof GraphReadResult.EmptyHit);
    assertTrue(result.isHit());
    assertFalse(result.isMiss());
    assertTrue(result.verticesOrEmpty().isEmpty());
  }

  @Test
  public void fromVerticesReturnsHitForNonEmptySet() {
    GraphReadResult result = GraphReadResult.fromVertices(Set.of("urn:li:domain:root"));

    assertTrue(result instanceof GraphReadResult.Hit);
    assertEquals(result.verticesOrEmpty(), Set.of("urn:li:domain:root"));
  }

  @Test
  public void scopeTruncatedHitStaysAHitUntilTheCallerRejectsIt() {
    GraphReadResult prefix =
        GraphReadResult.fromVertices(Set.of("urn:li:glossaryNode:child"), true);

    assertTrue(prefix.isHit());
    assertTrue(prefix.isScopeTruncated());
    assertEquals(prefix.verticesOrEmpty(), Set.of("urn:li:glossaryNode:child"));

    GraphReadResult rejected = prefix.missIfScopeTruncated();
    assertTrue(rejected.isMiss());
    assertEquals(((GraphReadResult.Miss) rejected).reason(), ReadMissReason.TRUNCATED);
    assertFalse(
        GraphReadResult.fromVertices(Set.of("urn:li:glossaryNode:child")).isScopeTruncated());
  }

  @Test
  public void missReturnsEmptyVertices() {
    GraphReadResult result = GraphReadResult.miss(ReadMissReason.ABSENT);

    assertTrue(result.isMiss());
    assertFalse(result.isHit());
    assertTrue(result.verticesOrEmpty().isEmpty());
  }
}

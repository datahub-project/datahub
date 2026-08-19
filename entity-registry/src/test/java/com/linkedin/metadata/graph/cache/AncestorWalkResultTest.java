package com.linkedin.metadata.graph.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;
import org.testng.annotations.Test;

public class AncestorWalkResultTest {

  @Test
  public void fromAncestorsReturnsHit() {
    AncestorWalkResult result = AncestorWalkResult.fromAncestors(List.of("urn:li:domain:parent"));

    assertTrue(result.isHit());
    assertFalse(result.isMiss());
    assertEquals(result.ancestorsOrEmpty(), List.of("urn:li:domain:parent"));
  }

  @Test
  public void missReturnsEmptyAncestors() {
    AncestorWalkResult result = AncestorWalkResult.miss(ReadMissReason.DISABLED);

    assertTrue(result.isMiss());
    assertTrue(result.ancestorsOrEmpty().isEmpty());
  }
}

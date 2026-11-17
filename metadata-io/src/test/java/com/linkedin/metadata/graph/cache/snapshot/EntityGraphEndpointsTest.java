package com.linkedin.metadata.graph.cache.snapshot;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.util.List;
import org.testng.annotations.Test;

public class EntityGraphEndpointsTest {

  private static final String CHILD = "urn:li:foo:a";
  private static final String PARENT = "urn:li:foo:b";

  @Test
  public void parseTreatsMissingAndJsonNullAsAbsent() {
    assertNull(EntityGraphEndpoints.parse(null));
    assertNull(EntityGraphEndpoints.parse(""));
    assertNull(EntityGraphEndpoints.parse("   "));
    assertNull(EntityGraphEndpoints.parse("null"));
    assertNull(EntityGraphEndpoints.parse("NULL"));
    assertNull(EntityGraphEndpoints.parse("\"null\""));
  }

  @Test
  public void parseAcceptsQuotedAndRawUrns() {
    assertEquals(EntityGraphEndpoints.parse(CHILD), CHILD);
    assertEquals(EntityGraphEndpoints.parse("\"" + PARENT + "\""), PARENT);
    assertEquals(EntityGraphEndpoints.parse("  " + CHILD + "  "), CHILD);
  }

  @Test
  public void parseSkipsUnparsableTokensWithoutThrowing() {
    assertNull(EntityGraphEndpoints.parse("not-a-urn"));
    assertNull(EntityGraphEndpoints.parse("urn:garbage"));
    assertNull(EntityGraphEndpoints.toUrn("not-a-urn"));
  }

  @Test
  public void isValidEdgeRequiresBothEndpoints() {
    assertTrue(EntityGraphEndpoints.isValidEdge(CHILD, PARENT));
    assertFalse(EntityGraphEndpoints.isValidEdge(CHILD, "null"));
    assertFalse(EntityGraphEndpoints.isValidEdge(null, PARENT));
    assertFalse(EntityGraphEndpoints.isValidEdge(CHILD, "not-a-urn"));
  }

  @Test
  public void toUrnListAndSetDropInvalidAndKeepOrder() {
    Urn child = UrnUtils.getUrn(CHILD);
    Urn parent = UrnUtils.getUrn(PARENT);

    assertEquals(EntityGraphEndpoints.toUrnList(List.of("null")), List.of());
    assertEquals(
        EntityGraphEndpoints.toUrnList(List.of(CHILD, "null", PARENT, "not-a-urn")),
        List.of(child, parent));
    assertEquals(
        EntityGraphEndpoints.toUrnSet(List.of("null", CHILD, "null", PARENT)),
        java.util.Set.of(child, parent));
  }
}

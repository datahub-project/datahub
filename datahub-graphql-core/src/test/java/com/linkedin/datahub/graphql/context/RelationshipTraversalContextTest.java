package com.linkedin.datahub.graphql.context;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class RelationshipTraversalContextTest {

  @Test
  public void testUnderCapProceeds() {
    RelationshipTraversalContext context = new RelationshipTraversalContext(2);
    assertTrue(context.tryVisit("urn:li:mlModel:a"));
    assertTrue(context.isVisited("urn:li:mlModel:a"));
    assertFalse(context.isVisited("urn:li:mlModel:b"));
  }

  @Test
  public void testAtCapNewUrnShortCircuits() {
    RelationshipTraversalContext context = new RelationshipTraversalContext(1);
    assertTrue(context.tryVisit("urn:li:mlModel:a"));
    assertFalse(context.tryVisit("urn:li:mlModel:b"));
    assertFalse(context.isVisited("urn:li:mlModel:b"));
  }

  @Test
  public void testAtCapAlreadyVisitedUrnProceeds() {
    // The same entity resolves several relationship types (e.g. features and trainedBy); each one
    // calls tryVisit. Hitting the cap must not cut off the second type for an entity already in.
    RelationshipTraversalContext context = new RelationshipTraversalContext(1);
    assertTrue(context.tryVisit("urn:li:mlModel:a"));
    assertTrue(context.tryVisit("urn:li:mlModel:a"));
  }
}

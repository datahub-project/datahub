package com.linkedin.datahub.graphql.types.common.mappers.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import java.util.List;
import org.testng.annotations.Test;

public class KnownEntitiesTest {

  private static final Urn DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)");
  private static final Urn FROM_NEWER_BUILD = UrnUtils.getUrn("urn:li:entityFromNewerBuild:x");

  private final QueryContext context = TestUtils.getMockAllowContext();

  @Test
  public void testMapAllKeepsOrderAndDropsUnrepresentable() {
    List<Entity> entities =
        KnownEntities.mapAll(context, List.of(FROM_NEWER_BUILD, DATASET, FROM_NEWER_BUILD));

    assertEquals(entities.size(), 1);
    assertEquals(entities.get(0).getUrn(), DATASET.toString());
  }

  @Test
  public void testMapFirst() {
    assertEquals(
        KnownEntities.mapFirst(context, List.of(FROM_NEWER_BUILD, DATASET)).getUrn(),
        DATASET.toString());
    assertNull(KnownEntities.mapFirst(context, List.of(FROM_NEWER_BUILD)));
  }

  @Test
  public void testIsKnownLooksInsideKeys() {
    assertTrue(KnownEntities.isKnown(context, DATASET));
    assertFalse(KnownEntities.isKnown(context, FROM_NEWER_BUILD));
    assertFalse(
        KnownEntities.isKnown(
            context, UrnUtils.getUrn("urn:li:monitor:(urn:li:entityFromNewerBuild:x,m)")));
    // Without a registry nothing can be judged, so everything counts as known.
    assertTrue(KnownEntities.isKnown(null, FROM_NEWER_BUILD));
  }
}

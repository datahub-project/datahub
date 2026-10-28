package com.linkedin.metadata.service;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.lifecycle.LifecycleStageTransitionPolicy;
import java.util.List;
import org.testng.annotations.Test;

public class LifecycleStagesTest {

  private static final Urn DRAFT = UrnUtils.getUrn("urn:li:lifecycleStageType:DRAFT");
  private static final Urn IN_REVIEW = UrnUtils.getUrn("urn:li:lifecycleStageType:IN_REVIEW");
  private static final Urn PUBLISHED = UrnUtils.getUrn("urn:li:lifecycleStageType:PUBLISHED");
  private static final Urn NONE = UrnUtils.getUrn(LifecycleStages.NO_STAGE_SENTINEL);

  private static LifecycleStageTransitionPolicy allowedFrom(Urn... stages) {
    return new LifecycleStageTransitionPolicy()
        .setAllowedPreviousStages(new UrnArray(List.of(stages)));
  }

  @Test
  public void testOpenEntryWhenNoPolicyOrNoList() {
    assertTrue(LifecycleStages.isTransitionAllowed(DRAFT, PUBLISHED, null));
    assertTrue(
        LifecycleStages.isTransitionAllowed(
            DRAFT, PUBLISHED, new LifecycleStageTransitionPolicy()));
  }

  @Test
  public void testAllowedPreviousStagesAreEnforced() {
    assertTrue(LifecycleStages.isTransitionAllowed(IN_REVIEW, PUBLISHED, allowedFrom(IN_REVIEW)));
    assertFalse(LifecycleStages.isTransitionAllowed(DRAFT, PUBLISHED, allowedFrom(IN_REVIEW)));
  }

  @Test
  public void testNoStageUsesSentinel() {
    assertTrue(LifecycleStages.isTransitionAllowed(null, DRAFT, allowedFrom(NONE)));
    assertFalse(LifecycleStages.isTransitionAllowed(null, PUBLISHED, allowedFrom(IN_REVIEW)));
  }

  @Test
  public void testEmptyListIsUnreachableButSameStageIsAlwaysAllowed() {
    assertFalse(LifecycleStages.isTransitionAllowed(DRAFT, PUBLISHED, allowedFrom()));
    assertTrue(LifecycleStages.isTransitionAllowed(PUBLISHED, PUBLISHED, allowedFrom()));
  }
}

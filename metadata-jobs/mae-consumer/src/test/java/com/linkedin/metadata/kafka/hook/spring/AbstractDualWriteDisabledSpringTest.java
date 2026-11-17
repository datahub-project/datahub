package com.linkedin.metadata.kafka.hook.spring;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import com.linkedin.metadata.service.IncrementalReindexWriteTargets;
import com.linkedin.metadata.service.UpdateIndicesService;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

/**
 * Assertions shared by the disabled-flag cases in both deployment modes: indexing still comes up,
 * no rollback dual-write strategy is created, and no rollback target is loaded — so no extra ES
 * write happens on deployments that have not opted in.
 */
public abstract class AbstractDualWriteDisabledSpringTest extends AbstractTestNGSpringContextTests {

  @Autowired private UpdateIndicesService updateIndicesService;

  protected void assertNoDualWriteStrategy() {
    assertNotNull(updateIndicesService);
    assertFalse(
        "dual-write strategy must not be registered when rollbackDualWriteEnabled is false",
        applicationContext.containsBean("updateIndicesUpgradeStrategy"));
    // The target bean itself stays, since a rebuild's fill target is mirrored regardless; what the
    // flag withholds is the rollback target, even for a completed Phase 1 index that recorded one.
    final IncrementalReindexWriteTargets targets =
        applicationContext.getBean(
            "incrementalReindexWriteTargets", IncrementalReindexWriteTargets.class);
    assertEquals(
        "no rollback target may be loaded when rollbackDualWriteEnabled is false",
        Optional.empty(),
        targets.oldBackingIndexFor(DualWriteTestSupport.DATASET_INDEX));
  }
}

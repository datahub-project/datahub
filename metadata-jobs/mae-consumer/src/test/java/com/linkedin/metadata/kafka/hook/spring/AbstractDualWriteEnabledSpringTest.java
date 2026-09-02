package com.linkedin.metadata.kafka.hook.spring;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.service.UpdateIndicesStrategy;
import com.linkedin.metadata.service.UpdateIndicesUpgradeStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

/**
 * Assertions shared by the enabled-flag cases in both deployment modes. Asserting the resolved
 * target — not merely that the bean exists — is the point: the bean can come up perfectly while
 * holding zero targets, in which case dual-write is silently doing nothing.
 */
public abstract class AbstractDualWriteEnabledSpringTest extends AbstractTestNGSpringContextTests {

  @Autowired private UpdateIndicesService updateIndicesService;

  protected void assertDualWriteResolvesItsTarget() {
    assertNotNull(updateIndicesService);

    final UpdateIndicesStrategy bean =
        (UpdateIndicesStrategy) applicationContext.getBean("updateIndicesUpgradeStrategy");
    assertTrue(
        "expected the ZDU rollback dual-write strategy",
        bean instanceof UpdateIndicesUpgradeStrategy);

    final UpdateIndicesUpgradeStrategy strategy = (UpdateIndicesUpgradeStrategy) bean;
    assertTrue("dual-write should be active for the completed Phase 1 index", strategy.isEnabled());
    assertEquals(
        "dataset should map to the recorded old backing index",
        DualWriteTestSupport.OLD_BACKING_INDEX,
        strategy.getOldIndexTargets().get("dataset"));
  }
}

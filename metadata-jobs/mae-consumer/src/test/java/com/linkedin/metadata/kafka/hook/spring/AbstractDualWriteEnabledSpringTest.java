package com.linkedin.metadata.kafka.hook.spring;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
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

    // The production factory resolved a MIXED-CASE entity's index into a target. This half is about
    // IndexConvention: it derives the name from the lowercased physical index, so the key here is
    // "aiagent", never the registry's "aiAgent".
    assertEquals(
        "the mixed-case entity should map to its recorded old backing index",
        DualWriteTestSupport.AI_AGENT_OLD_BACKING_INDEX,
        strategy.getOldIndexTargets().get(DualWriteTestSupport.AI_AGENT_DERIVED_NAME));

    assertDualWriteLookupIsCaseNormalised(strategy);
  }

  /**
   * Guards the half that actually regresses.
   *
   * <p>Asserting the map contents above is NOT enough on its own: the factory always produces
   * lowercase keys, so those assertions hold whether or not the lookup normalises. The defect is on
   * the read side — writes resolve their target with {@code EntitySpec.getName()}, the REGISTRY
   * name ({@code aiAgent}), against a map keyed {@code aiagent}. Without normalisation that misses,
   * and every entity whose registered name is not all-lowercase silently never dual-writes.
   *
   * <p>{@code removeTarget} is used as the probe because it is the one public entry point that
   * takes a registry-cased entity name and resolves it against the map exactly as the write path
   * does. Revert the normalisation and the mixed-case removal below stops matching, leaving the
   * target in place and failing this assertion — which is the whole point of covering it here.
   */
  private static void assertDualWriteLookupIsCaseNormalised(
      final UpdateIndicesUpgradeStrategy strategy) {
    assertNotNull(
        "precondition: the mixed-case target must be present before probing the lookup",
        strategy.getOldIndexTargets().get(DualWriteTestSupport.AI_AGENT_DERIVED_NAME));

    strategy.removeTarget(DualWriteTestSupport.AI_AGENT_REGISTRY_NAME);

    assertNull(
        "a registry-cased lookup ("
            + DualWriteTestSupport.AI_AGENT_REGISTRY_NAME
            + ") must resolve against the lowercased key ("
            + DualWriteTestSupport.AI_AGENT_DERIVED_NAME
            + "); it did not, so mixed-case entities would never dual-write",
        strategy.getOldIndexTargets().get(DualWriteTestSupport.AI_AGENT_DERIVED_NAME));
  }
}

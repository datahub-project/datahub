package com.linkedin.metadata.kafka.hook.spring;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import com.linkedin.metadata.service.IncrementalReindexWriteTargets;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.service.UpdateIndicesStrategy;
import com.linkedin.metadata.service.UpdateIndicesUpgradeStrategy;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

/**
 * Assertions shared by the enabled-flag cases in both deployment modes. Asserting the resolved
 * target, not merely that the beans exist, is the point: the beans can come up perfectly while
 * holding zero targets, in which case dual-write is silently doing nothing.
 */
public abstract class AbstractDualWriteEnabledSpringTest extends AbstractTestNGSpringContextTests {

  @Autowired private UpdateIndicesService updateIndicesService;

  @Autowired
  @Qualifier("systemOperationContext")
  private OperationContext systemOperationContext;

  protected void assertDualWriteResolvesItsTarget() {
    assertNotNull(updateIndicesService);

    final UpdateIndicesStrategy bean =
        (UpdateIndicesStrategy) applicationContext.getBean("updateIndicesUpgradeStrategy");
    assertTrue(
        "expected the ZDU rollback dual-write strategy",
        bean instanceof UpdateIndicesUpgradeStrategy);
    assertTrue("dual-write should be active for the completed Phase 1 index", bean.isEnabled());

    final IncrementalReindexWriteTargets targets =
        applicationContext.getBean(
            "incrementalReindexWriteTargets", IncrementalReindexWriteTargets.class);
    assertEquals(
        "the dataset alias should map to the recorded old backing index",
        Optional.of(DualWriteTestSupport.OLD_BACKING_INDEX),
        targets.oldBackingIndexFor(DualWriteTestSupport.DATASET_INDEX));

    // Targets are keyed by the alias the index convention derives, so a registry name with
    // capitals resolves through the same lowercased alias Phase 1 recorded. This is the lookup the
    // write path performs.
    final String aiAgentAlias =
        systemOperationContext
            .getSearchContext()
            .getIndexConvention()
            .getEntityIndexName(
                systemOperationContext, DualWriteTestSupport.AI_AGENT_REGISTRY_NAME);
    assertEquals(DualWriteTestSupport.AI_AGENT_INDEX, aiAgentAlias);
    assertEquals(
        "the mixed-case entity's alias should map to its recorded old backing index",
        Optional.of(DualWriteTestSupport.AI_AGENT_OLD_BACKING_INDEX),
        targets.oldBackingIndexFor(aiAgentAlias));
  }
}

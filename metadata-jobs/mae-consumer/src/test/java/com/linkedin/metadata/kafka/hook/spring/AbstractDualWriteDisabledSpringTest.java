package com.linkedin.metadata.kafka.hook.spring;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import com.linkedin.metadata.service.UpdateIndicesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

/**
 * Assertions shared by the disabled-flag cases in both deployment modes: indexing still comes up,
 * and no dual-write bean is created — so no state read and no extra ES write happen on deployments
 * that have not opted in.
 */
public abstract class AbstractDualWriteDisabledSpringTest extends AbstractTestNGSpringContextTests {

  @Autowired private UpdateIndicesService updateIndicesService;

  protected void assertNoDualWriteStrategy() {
    assertNotNull(updateIndicesService);
    assertFalse(
        "dual-write strategy must not be registered when rollbackDualWriteEnabled is false",
        applicationContext.containsBean("updateIndicesUpgradeStrategy"));
  }
}

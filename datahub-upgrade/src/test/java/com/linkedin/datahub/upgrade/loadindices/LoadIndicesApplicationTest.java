package com.linkedin.datahub.upgrade.loadindices;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;

import com.linkedin.datahub.upgrade.UpgradeCliApplication;
import com.linkedin.datahub.upgrade.UpgradeCliApplicationTestConfiguration;
import com.linkedin.datahub.upgrade.restoreindices.RestoreIndices;
import com.linkedin.datahub.upgrade.system.SystemUpdate;
import com.linkedin.datahub.upgrade.system.SystemUpdateBlocking;
import com.linkedin.datahub.upgrade.system.SystemUpdateNonBlocking;
import javax.inject.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@ActiveProfiles("test")
@SpringBootTest(
    classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class},
    args = {"-u", "LoadIndices"})
public class LoadIndicesApplicationTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Named("loadIndices")
  private LoadIndices loadIndices;

  @Autowired(required = false)
  @Named("restoreIndices")
  private RestoreIndices restoreIndices;

  @Autowired(required = false)
  @Named("systemUpdate")
  private SystemUpdate systemUpdate;

  @Autowired(required = false)
  @Named("systemUpdateBlocking")
  private SystemUpdateBlocking systemUpdateBlocking;

  @Autowired(required = false)
  @Named("systemUpdateNonBlocking")
  private SystemUpdateNonBlocking systemUpdateNonBlocking;

  @Test
  public void testInit() {
    assertNotNull(loadIndices);
    assertNull(restoreIndices, "Expected no additional execution components.");
    assertNull(systemUpdate, "Expected no additional execution components.");
    assertNull(systemUpdateBlocking, "Expected no additional execution components.");
    assertNull(systemUpdateNonBlocking, "Expected no additional execution components.");
  }

  @Test
  public void testSteps() {
    assertTrue(loadIndices.steps().size() >= 2);
  }
}

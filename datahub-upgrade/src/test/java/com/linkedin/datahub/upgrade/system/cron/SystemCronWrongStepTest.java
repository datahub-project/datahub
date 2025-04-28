package com.linkedin.datahub.upgrade.system.cron;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.fail;

import com.linkedin.datahub.upgrade.UpgradeCliApplication;
import com.linkedin.datahub.upgrade.UpgradeCliApplicationTestConfiguration;
import com.linkedin.datahub.upgrade.UpgradeStep;
import javax.inject.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@ActiveProfiles("test")
@SpringBootTest(
    classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class},
    args = {"-u", "SystemUpdateCron", "-a", "stepType=BADStep"})
public class SystemCronWrongStepTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Named("systemUpdateCron")
  private SystemUpdateCron systemUpdateCron;

  @Test
  public void testInit() {
    assertNotNull(systemUpdateCron);
    assertEquals(systemUpdateCron.steps().size(), 1);
    // should not reach this
    UpgradeStep step = systemUpdateCron.steps().get(0);
    fail();
  }
}

package com.linkedin.datahub.upgrade.system.cron;

import com.linkedin.datahub.upgrade.UpgradeCliApplication;
import com.linkedin.datahub.upgrade.UpgradeCliApplicationTestConfiguration;
import javax.inject.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@ActiveProfiles("test")
@SpringBootTest(
    classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class},
    args = {"-u", "SystemUpdateCron", "-a", "stepType=TweakReplicasStep", "-a", "dryRun=true"})
public class SystemCronDryRunTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Named("systemUpdateCron")
  private SystemUpdateCron systemUpdateCron;

  @Test
  public void testInit() {
    //    // Setup custom args
    //    parsedArgs.put(RestoreIndices.BATCH_SIZE_ARG_NAME, Optional.of("500"));
    //    parsedArgs.put(RestoreIndices.NUM_THREADS_ARG_NAME, Optional.of("2"));
    //    parsedArgs.put(RestoreIndices.BATCH_DELAY_MS_ARG_NAME, Optional.of("100"));
    //    parsedArgs.put(RestoreIndices.STARTING_OFFSET_ARG_NAME, Optional.of("100"));
    //    parsedArgs.put(RestoreIndices.URN_BASED_PAGINATION_ARG_NAME, Optional.of("true"));
    //    parsedArgs.put(RestoreIndices.CREATE_DEFAULT_ASPECTS_ARG_NAME, Optional.of("true"));
    //    parsedArgs.put(RestoreIndices.ASPECT_NAME_ARG_NAME, Optional.of("testAspect"));
    //    parsedArgs.put(RestoreIndices.URN_ARG_NAME, Optional.of("testUrn"));
    //    UpgradeStepResult result = sendMAEStep.executable().apply(mockContext);
    //
    //    assertNotNull(systemUpdateCron);
    //    assertEquals(systemUpdateCron.steps().size(), 1);
    //    UpgradeStep step = systemUpdateCron.steps().get(0);
    //    assertTrue(step instanceof TweakReplicasStep);
    //    // TweakReplicasStep specific
    //    TweakReplicasStep trs = (TweakReplicasStep) step;
    //    trs.createArgs(con)
    //    assertTrue(trs.getArgs().dryRun);
  }
}

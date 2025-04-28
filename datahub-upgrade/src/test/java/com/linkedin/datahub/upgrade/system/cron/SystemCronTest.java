package com.linkedin.datahub.upgrade.system.cron;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import com.linkedin.datahub.upgrade.UpgradeCliApplication;
import com.linkedin.datahub.upgrade.UpgradeCliApplicationTestConfiguration;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.cron.steps.TweakReplicasStep;
import javax.inject.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@ActiveProfiles("test")
@SpringBootTest(
    classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class},
    args = {"-u", "SystemUpdateCron", "-a", "stepType=TweakReplicasStep"})
public class SystemCronTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Named("systemUpdateCron")
  private SystemUpdateCron systemUpdateCron;

  //  @Mock private EntityService<?> mockEntityService;
  //  @Mock private UpgradeContext mockContext;
  //  @Mock private UpgradeReport mockReport;
  //  @Mock private OperationContext mockOpContext;
  //  private Map<String, Optional<String>> parsedArgs;
  //
  //  @BeforeMethod
  //  public void setup() {
  //    MockitoAnnotations.openMocks(this);
  //    // Create the real SendMAEStep with the test database
  //    sendMAEStep = new SendMAEStep(database, mockEntityService);
  //
  //    parsedArgs = new HashMap<>();
  //
  //    when(mockContext.parsedArgs()).thenReturn(parsedArgs);
  //    when(mockContext.report()).thenReturn(mockReport);
  //    when(mockContext.opContext()).thenReturn(mockOpContext);
  //
  //    // Setup default result for entityService
  //    RestoreIndicesResult mockResult = new RestoreIndicesResult();
  //    mockResult.rowsMigrated = 0;
  //    mockResult.ignored = 0;
  //
  //    when(mockEntityService.restoreIndices(eq(mockOpContext), any(RestoreIndicesArgs.class),
  // any()))
  //            .thenReturn(Collections.singletonList(mockResult));
  //  }

  @Test
  public void testInit() {
    assertNotNull(systemUpdateCron);
    assertEquals(systemUpdateCron.steps().size(), 1);
    UpgradeStep step = systemUpdateCron.steps().get(0);
    assertTrue(step instanceof TweakReplicasStep);
    // TweakReplicasStep specific
    TweakReplicasStep trs = (TweakReplicasStep) step;
    assertFalse(!trs.getArgs().dryRun);
  }
}

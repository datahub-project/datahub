package com.linkedin.datahub.upgrade.system.cron;


import com.linkedin.datahub.upgrade.UpgradeCliApplication;
import com.linkedin.datahub.upgrade.UpgradeCliApplicationTestConfiguration;
import com.linkedin.datahub.upgrade.system.cron.steps.TweakReplicasStep;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import javax.inject.Named;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;


@ActiveProfiles("test")
@SpringBootTest(
        classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class},
        args = {"-u", "SystemUpdateCron"})
public class SystemCronTest extends AbstractTestNGSpringContextTests {

    @Autowired
    @Named("systemUpdateCron")
    private SystemUpdateCron systemUpdateCron;

    @Test
    public void testInit() {
        assertNotNull(systemUpdateCron);
        assertEquals(systemUpdateCron.steps().size(), 1);
        assertTrue(systemUpdateCron.steps().get(0) instanceof TweakReplicasStep);
    }
}

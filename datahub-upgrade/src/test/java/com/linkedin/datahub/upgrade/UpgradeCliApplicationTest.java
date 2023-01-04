package com.linkedin.datahub.upgrade;

import com.linkedin.datahub.upgrade.restoreindices.RestoreIndices;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import javax.inject.Named;

import static org.testng.AssertJUnit.assertEquals;

@ActiveProfiles("test")
@SpringBootTest(classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class})
public class UpgradeCliApplicationTest extends AbstractTestNGSpringContextTests {

    @Autowired
    @Named("restoreIndices")
    private RestoreIndices restoreIndices;

    @Test
    public void testKafkaHealthCheck() {
        /*
          This might seem like a simple test however it does exercise the spring autowiring of the kafka health check bean
         */
        assertEquals(3, restoreIndices.steps().size());
    }
}

package com.linkedin.datahub.upgrade;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import com.linkedin.datahub.upgrade.system.SystemUpdate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@ActiveProfiles("test")
@SpringBootTest(
    classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class},
    properties = {
      "kafka.schemaRegistry.type=INTERNAL",
      "DATAHUB_UPGRADE_HISTORY_TOPIC_NAME=test_due_topic"
    })
public class DatahubUpgradeNoSchemaRegistryTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Named("systemUpdate")
  private SystemUpdate systemUpdate;

  @Test
  public void testSystemUpdateInit() {
    assertNotNull(systemUpdate);
  }

  @Test
  public void testSystemUpdateSend() {
    UpgradeStepResult.Result result =
        systemUpdate.steps().stream()
            .filter(s -> s.id().equals("DataHubStartupStep"))
            .findFirst()
            .get()
            .executable()
            .apply(
                new UpgradeContext() {
                  @Override
                  public Upgrade upgrade() {
                    return null;
                  }

                  @Override
                  public List<UpgradeStepResult> stepResults() {
                    return null;
                  }

                  @Override
                  public UpgradeReport report() {
                    return null;
                  }

                  @Override
                  public List<String> args() {
                    return null;
                  }

                  @Override
                  public Map<String, Optional<String>> parsedArgs() {
                    return null;
                  }
                })
            .result();
    assertEquals("SUCCEEDED", result.toString());
  }
}

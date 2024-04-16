package com.linkedin.datahub.upgrade;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.AssertJUnit.assertNotNull;

import com.linkedin.datahub.upgrade.impl.DefaultUpgradeManager;
import com.linkedin.datahub.upgrade.system.SystemUpdateNonBlocking;
import com.linkedin.datahub.upgrade.system.vianodes.ReindexDataJobViaNodesCLL;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.mxe.Topics;
import java.util.List;
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
      "BOOTSTRAP_SYSTEM_UPDATE_DATA_JOB_NODE_CLL_ENABLED=true",
      "kafka.schemaRegistry.type=INTERNAL",
      "DATAHUB_UPGRADE_HISTORY_TOPIC_NAME=" + Topics.DATAHUB_UPGRADE_HISTORY_TOPIC_NAME,
      "METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME=" + Topics.METADATA_CHANGE_LOG_VERSIONED,
    },
    args = {"-u", "SystemUpdateNonBlocking"})
public class DatahubUpgradeNonBlockingTest extends AbstractTestNGSpringContextTests {

  @Autowired(required = false)
  @Named("systemUpdateNonBlocking")
  private SystemUpdateNonBlocking systemUpdateNonBlocking;

  @Autowired
  @Test
  public void testSystemUpdateNonBlockingInit() {
    assertNotNull(systemUpdateNonBlocking);
  }

  @Test
  public void testReindexDataJobViaNodesCLLPaging() {
    EntityService<?> mockService = mock(EntityService.class);
    ReindexDataJobViaNodesCLL cllUpgrade = new ReindexDataJobViaNodesCLL(mockService, true, 10);
    SystemUpdateNonBlocking upgrade =
        new SystemUpdateNonBlocking(List.of(), List.of(cllUpgrade), null);
    DefaultUpgradeManager manager = new DefaultUpgradeManager();
    manager.register(upgrade);
    manager.execute("SystemUpdateNonBlocking", List.of());
    verify(mockService, times(1))
        .streamRestoreIndices(
            eq(
                new RestoreIndicesArgs()
                    .batchSize(10)
                    .limit(0)
                    .aspectName("dataJobInputOutput")
                    .urnLike("urn:li:dataJob:%")),
            any());
  }
}

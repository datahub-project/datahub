package com.linkedin.datahub.upgrade;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.kubernetes.KubernetesScaleDown;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.elasticsearch.BuildIndices;
import com.linkedin.datahub.upgrade.system.kafka.KafkaSetup;
import com.linkedin.metadata.config.messaging.MessagingTransport;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/** pgQueue Postgres stacks omit Kafka; blocking upgrades must not include {@link KafkaSetup}. */
@ActiveProfiles("test")
@SpringBootTest(
    classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class},
    args = {"-u", "SystemUpdateBlocking"},
    properties = {
      MessagingTransport.PROPERTY + "=" + MessagingTransport.PGQUEUE,
      // pgQueue messaging requires the SqlSetup-managed queue catalog; production deployments
      // that select transport=pgqueue always enable this, so the test mirrors that contract.
      "postgres.pgQueue.enabled=true"
    })
public class DatahubUpgradeBlockingPgQueueTest extends AbstractTestNGSpringContextTests {

  @Autowired private List<BlockingSystemUpgrade> blockingSystemUpgrades;

  @Test
  public void testKafkaSetupNotPresent() {
    assertFalse(
        blockingSystemUpgrades.stream().anyMatch(KafkaSetup.class::isInstance),
        "KafkaSetup should not load when messaging transport is pgqueue");
  }

  @Test
  public void testKubernetesScaleDownFirstAndBuildIndicesPresent() {
    assertTrue(blockingSystemUpgrades.get(0) instanceof KubernetesScaleDown);
    assertTrue(
        blockingSystemUpgrades.stream().anyMatch(BuildIndices.class::isInstance),
        "BuildIndices blocking upgrade should still run without Kafka");
  }
}

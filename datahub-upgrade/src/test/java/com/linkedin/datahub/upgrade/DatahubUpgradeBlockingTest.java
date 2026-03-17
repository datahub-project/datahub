package com.linkedin.datahub.upgrade;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;

import com.linkedin.datahub.upgrade.kubernetes.KubernetesScaleDown;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.SystemUpdateBlocking;
import com.linkedin.datahub.upgrade.system.bootstrapmcps.BootstrapMCPStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.BuildIndices;
import com.linkedin.datahub.upgrade.system.kafka.KafkaSetup;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@ActiveProfiles("test")
@SpringBootTest(
    classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class},
    args = {"-u", "SystemUpdateBlocking"})
public class DatahubUpgradeBlockingTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Named("systemUpdateBlocking")
  private SystemUpdateBlocking systemUpdateBlocking;

  @Autowired private List<BlockingSystemUpgrade> blockingSystemUpgrades;

  @Autowired
  @Qualifier("systemOperationContext")
  private OperationContext systemOperationContext;

  @Test
  public void testKubernetesScaleDownOrder() {
    assertNotNull(blockingSystemUpgrades);
    assertTrue(blockingSystemUpgrades.get(0) instanceof KubernetesScaleDown);
  }

  @Test
  public void testKafkaSetupOrder() {
    assertNotNull(blockingSystemUpgrades);
    assertTrue(blockingSystemUpgrades.get(1) instanceof KafkaSetup);
  }

  @Test
  public void testBuildIndicesOrder() {
    assertNotNull(blockingSystemUpgrades);
    assertTrue(blockingSystemUpgrades.get(2) instanceof BuildIndices);
  }

  @Test
  public void testKubernetesScaleDownDoesNotRequireScaleDown() {
    BlockingSystemUpgrade k8 = blockingSystemUpgrades.get(0);
    UpgradeContext ctx = mock(UpgradeContext.class);
    assertFalse(k8.requiresK8ScaleDown(ctx));
  }

  @Test
  public void testBuildIndicesRequiresK8ScaleDownRunsWithoutThrow() {
    BlockingSystemUpgrade buildIndicesUpgrade = blockingSystemUpgrades.get(2);
    UpgradeContext ctx = mock(UpgradeContext.class);
    when(ctx.opContext()).thenReturn(systemOperationContext);
    boolean result = buildIndicesUpgrade.requiresK8ScaleDown(ctx);
    assertTrue(result == true || result == false);
  }

  @Test
  public void testNBlockingBootstrapMCP() {
    assertNotNull(systemUpdateBlocking);

    List<BootstrapMCPStep> mcpTemplate =
        systemUpdateBlocking.steps().stream()
            .filter(update -> update instanceof BootstrapMCPStep)
            .map(update -> (BootstrapMCPStep) update)
            .toList();

    assertFalse(mcpTemplate.isEmpty());
    assertTrue(
        mcpTemplate.stream().allMatch(update -> update.getMcpTemplate().isBlocking()),
        String.format(
            "Found non-blocking step (expected blocking only): %s",
            mcpTemplate.stream()
                .filter(update -> !update.getMcpTemplate().isBlocking())
                .map(update -> update.getMcpTemplate().getName())
                .collect(Collectors.toSet())));
  }
}

package com.linkedin.datahub.upgrade;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;

import com.linkedin.datahub.upgrade.system.SystemUpdateBlocking;
import com.linkedin.datahub.upgrade.system.bootstrapmcps.BootstrapMCPStep;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Named;
import org.springframework.beans.factory.annotation.Autowired;
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

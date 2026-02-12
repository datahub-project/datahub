package com.linkedin.datahub.upgrade.config;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.SystemUpdateNonBlocking;
import com.linkedin.datahub.upgrade.system.bootstrapmcps.BootstrapMCP;
import java.util.List;
import org.springframework.boot.ApplicationArguments;
import org.testng.annotations.Test;

public class SystemUpdateConfigNonBlockingFilterTest {

  @Test
  public void testNoFilterRunsAllUpgrades() {
    ApplicationArguments appArgs = mock(ApplicationArguments.class);
    when(appArgs.getOptionValues("n")).thenReturn(null);
    when(appArgs.getOptionValues("nonblocking-classname")).thenReturn(null);

    NonBlockingSystemUpgrade upgrade1 = createMockUpgrade("Upgrade1");
    NonBlockingSystemUpgrade upgrade2 = createMockUpgrade("Upgrade2");
    BootstrapMCP bootstrapMCP = mock(BootstrapMCP.class);
    when(bootstrapMCP.steps()).thenReturn(List.of());
    when(bootstrapMCP.cleanupSteps()).thenReturn(List.of());

    SystemUpdateConfig config = new SystemUpdateConfig();
    SystemUpdateNonBlocking result =
        config.systemUpdateNonBlocking(List.of(upgrade1, upgrade2), bootstrapMCP, appArgs);

    assertEquals(result.steps().size(), 2);
  }

  @Test
  public void testFilterByShortOption() {
    ApplicationArguments appArgs = mock(ApplicationArguments.class);
    when(appArgs.getOptionValues("n")).thenReturn(List.of("Upgrade1"));
    when(appArgs.getOptionValues("nonblocking-classname")).thenReturn(null);

    NonBlockingSystemUpgrade upgrade1 = createMockUpgrade("Upgrade1");
    NonBlockingSystemUpgrade upgrade2 = createMockUpgrade("Upgrade2");
    BootstrapMCP bootstrapMCP = mock(BootstrapMCP.class);
    when(bootstrapMCP.steps()).thenReturn(List.of());
    when(bootstrapMCP.cleanupSteps()).thenReturn(List.of());

    SystemUpdateConfig config = new SystemUpdateConfig();
    SystemUpdateNonBlocking result =
        config.systemUpdateNonBlocking(List.of(upgrade1, upgrade2), bootstrapMCP, appArgs);

    assertEquals(result.steps().size(), 1);
  }

  @Test
  public void testFilterByLongOption() {
    ApplicationArguments appArgs = mock(ApplicationArguments.class);
    when(appArgs.getOptionValues("n")).thenReturn(null);
    when(appArgs.getOptionValues("nonblocking-classname")).thenReturn(List.of("Upgrade2"));

    NonBlockingSystemUpgrade upgrade1 = createMockUpgrade("Upgrade1");
    NonBlockingSystemUpgrade upgrade2 = createMockUpgrade("Upgrade2");
    BootstrapMCP bootstrapMCP = mock(BootstrapMCP.class);
    when(bootstrapMCP.steps()).thenReturn(List.of());
    when(bootstrapMCP.cleanupSteps()).thenReturn(List.of());

    SystemUpdateConfig config = new SystemUpdateConfig();
    SystemUpdateNonBlocking result =
        config.systemUpdateNonBlocking(List.of(upgrade1, upgrade2), bootstrapMCP, appArgs);

    assertEquals(result.steps().size(), 1);
  }

  @Test
  public void testFilterWithNoMatchReturnsEmpty() {
    ApplicationArguments appArgs = mock(ApplicationArguments.class);
    when(appArgs.getOptionValues("n")).thenReturn(List.of("NonExistentUpgrade"));
    when(appArgs.getOptionValues("nonblocking-classname")).thenReturn(null);

    NonBlockingSystemUpgrade upgrade1 = createMockUpgrade("Upgrade1");
    NonBlockingSystemUpgrade upgrade2 = createMockUpgrade("Upgrade2");
    BootstrapMCP bootstrapMCP = mock(BootstrapMCP.class);
    when(bootstrapMCP.steps()).thenReturn(List.of());
    when(bootstrapMCP.cleanupSteps()).thenReturn(List.of());

    SystemUpdateConfig config = new SystemUpdateConfig();
    SystemUpdateNonBlocking result =
        config.systemUpdateNonBlocking(List.of(upgrade1, upgrade2), bootstrapMCP, appArgs);

    assertTrue(result.steps().isEmpty());
  }

  @Test
  public void testFilterByCommaDelimitedList() {
    ApplicationArguments appArgs = mock(ApplicationArguments.class);
    when(appArgs.getOptionValues("n")).thenReturn(List.of("Upgrade1,Upgrade3"));
    when(appArgs.getOptionValues("nonblocking-classname")).thenReturn(null);

    NonBlockingSystemUpgrade upgrade1 = createMockUpgrade("Upgrade1");
    NonBlockingSystemUpgrade upgrade2 = createMockUpgrade("Upgrade2");
    NonBlockingSystemUpgrade upgrade3 = createMockUpgrade("Upgrade3");
    BootstrapMCP bootstrapMCP = mock(BootstrapMCP.class);
    when(bootstrapMCP.steps()).thenReturn(List.of());
    when(bootstrapMCP.cleanupSteps()).thenReturn(List.of());

    SystemUpdateConfig config = new SystemUpdateConfig();
    SystemUpdateNonBlocking result =
        config.systemUpdateNonBlocking(
            List.of(upgrade1, upgrade2, upgrade3), bootstrapMCP, appArgs);

    assertEquals(result.steps().size(), 2);
  }

  @Test
  public void testFilterByCommaDelimitedListWithSpaces() {
    ApplicationArguments appArgs = mock(ApplicationArguments.class);
    when(appArgs.getOptionValues("n")).thenReturn(List.of("Upgrade1 , Upgrade2"));
    when(appArgs.getOptionValues("nonblocking-classname")).thenReturn(null);

    NonBlockingSystemUpgrade upgrade1 = createMockUpgrade("Upgrade1");
    NonBlockingSystemUpgrade upgrade2 = createMockUpgrade("Upgrade2");
    NonBlockingSystemUpgrade upgrade3 = createMockUpgrade("Upgrade3");
    BootstrapMCP bootstrapMCP = mock(BootstrapMCP.class);
    when(bootstrapMCP.steps()).thenReturn(List.of());
    when(bootstrapMCP.cleanupSteps()).thenReturn(List.of());

    SystemUpdateConfig config = new SystemUpdateConfig();
    SystemUpdateNonBlocking result =
        config.systemUpdateNonBlocking(
            List.of(upgrade1, upgrade2, upgrade3), bootstrapMCP, appArgs);

    assertEquals(result.steps().size(), 2);
  }

  private NonBlockingSystemUpgrade createMockUpgrade(String id) {
    NonBlockingSystemUpgrade upgrade = mock(NonBlockingSystemUpgrade.class);
    when(upgrade.id()).thenReturn(id);
    UpgradeStep step = mock(UpgradeStep.class);
    when(step.id()).thenReturn(id + "Step");
    when(upgrade.steps()).thenReturn(List.of(step));
    when(upgrade.cleanupSteps()).thenReturn(List.of());
    return upgrade;
  }
}

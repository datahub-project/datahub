package com.linkedin.datahub.upgrade.kubernetes;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeResult;
import com.linkedin.metadata.config.kubernetes.KubernetesScaleDownConfiguration;
import com.linkedin.upgrade.DataHubUpgradeState;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import uk.org.webcompere.systemstubs.testng.SystemStubsListener;

@Listeners(SystemStubsListener.class)
public class KubernetesScaleDownCleanupStepTest {

  @Mock private UpgradeContext upgradeContext;
  @Mock private UpgradeResult upgradeResult;

  private KubernetesScaleDownCleanupStep step;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testId() {
    step = new KubernetesScaleDownCleanupStep(new KubernetesScaleDownConfiguration());
    assertEquals(step.id(), "KubernetesScaleDownCleanupStep");
  }

  @Test
  public void testExecutableDoesNotThrowWhenNotInK8() {
    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKubernetesServiceHost(null);
    when(upgradeResult.result()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    step = new KubernetesScaleDownCleanupStep(config);

    step.executable().accept(upgradeContext, upgradeResult);
  }

  @Test
  public void testExecutableDoesNotThrowWhenInK8AndEnabledAndJavaImplementationEnabled() {
    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKubernetesServiceHost("10.0.0.1");
    config.setEnabled(true);
    config.setUseJavaImplementation(true);
    step = new KubernetesScaleDownCleanupStep(config);
    when(upgradeResult.result()).thenReturn(DataHubUpgradeState.SUCCEEDED);

    step.executable().accept(upgradeContext, upgradeResult);
  }

  @Test
  public void testExecutableSkipsWhenUseJavaImplementationDisabled() {
    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKubernetesServiceHost("10.0.0.1");
    config.setEnabled(true);
    config.setUseJavaImplementation(false);
    step = new KubernetesScaleDownCleanupStep(config);
    when(upgradeResult.result()).thenReturn(DataHubUpgradeState.SUCCEEDED);

    step.executable().accept(upgradeContext, upgradeResult);
  }
}

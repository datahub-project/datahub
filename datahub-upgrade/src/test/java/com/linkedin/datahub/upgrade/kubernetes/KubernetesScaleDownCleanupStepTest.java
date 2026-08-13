package com.linkedin.datahub.upgrade.kubernetes;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeResult;
import com.linkedin.metadata.config.kubernetes.KubernetesScaleDownConfiguration;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.mockito.InOrder;
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

  @Test
  public void testExecutableRestoresSavedStateBeforeDeletingConfigMap() {
    KubernetesApiAccessor accessor = mock(KubernetesApiAccessor.class);
    ScaleDownState state =
        ScaleDownState.builder()
            .attempt(1)
            .deployments(
                List.of(
                    ScaleDownState.DeploymentReplicas.builder()
                        .name("datahub-gms")
                        .replicas(2)
                        .build()))
            .envBeforeByDeployment(
                Map.of(
                    "datahub-gms",
                    Map.of(
                        "MCE_CONSUMER_ENABLED", "true",
                        "PRE_PROCESS_HOOKS_UI_ENABLED", "")))
            .build();
    configureSuccessfulCleanup(accessor, state);

    step.executable().accept(upgradeContext, upgradeResult);

    InOrder order = inOrder(accessor);
    order.verify(accessor).scaleDeployment("datahub-gms", "default", 2);
    order
        .verify(accessor)
        .restoreDeploymentEnv(
            "datahub-gms",
            "default",
            Map.of(
                "MCE_CONSUMER_ENABLED", "true", "PRE_PROCESS_HOOKS_UI_ENABLED", ""));
    order.verify(accessor).waitForRollout("datahub-gms", "default");
    order.verify(accessor).deleteConfigMap(anyString(), eq("default"));
  }

  @Test
  public void testExecutableLeavesConfigMapWhenRestoreFails() {
    KubernetesApiAccessor accessor = mock(KubernetesApiAccessor.class);
    ScaleDownState state =
        ScaleDownState.builder()
            .attempt(1)
            .envBeforeByDeployment(Map.of("datahub-gms", Map.of("MCE_CONSUMER_ENABLED", "true")))
            .build();
    configureSuccessfulCleanup(accessor, state);
    doThrow(new RuntimeException("restore failed"))
        .when(accessor)
        .restoreDeploymentEnv(anyString(), anyString(), any());

    assertThrows(
        RuntimeException.class, () -> step.executable().accept(upgradeContext, upgradeResult));

    verify(accessor, never()).deleteConfigMap(anyString(), anyString());
  }

  @Test
  public void testExecutableDoesNotDeleteConfigMapWhenStateIsMissing() {
    KubernetesApiAccessor accessor = mock(KubernetesApiAccessor.class);
    KubernetesScaleDownConfiguration config = enabledConfig();
    OperationContext operationContext = mock(OperationContext.class);
    when(operationContext.getObjectMapper()).thenReturn(new ObjectMapper());
    when(upgradeContext.opContext()).thenReturn(operationContext);
    when(upgradeResult.result()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(accessor.getConfigMapState(anyString(), anyString(), any(ObjectMapper.class)))
        .thenReturn(Optional.empty());
    step = new KubernetesScaleDownCleanupStep(config, () -> Optional.of(accessor));

    step.executable().accept(upgradeContext, upgradeResult);

    verify(accessor, never()).deleteConfigMap(anyString(), anyString());
  }

  private void configureSuccessfulCleanup(
      KubernetesApiAccessor accessor, ScaleDownState state) {
    KubernetesScaleDownConfiguration config = enabledConfig();
    OperationContext operationContext = mock(OperationContext.class);
    when(operationContext.getObjectMapper()).thenReturn(new ObjectMapper());
    when(upgradeContext.opContext()).thenReturn(operationContext);
    when(upgradeResult.result()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(accessor.getConfigMapState(anyString(), anyString(), any(ObjectMapper.class)))
        .thenReturn(Optional.of(state));
    step = new KubernetesScaleDownCleanupStep(config, () -> Optional.of(accessor));
  }

  private static KubernetesScaleDownConfiguration enabledConfig() {
    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKubernetesServiceHost("10.0.0.1");
    config.setEnabled(true);
    config.setUseJavaImplementation(true);
    return config;
  }
}

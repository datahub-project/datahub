package com.linkedin.datahub.upgrade.kubernetes;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.config.kubernetes.KubernetesScaleDownConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import uk.org.webcompere.systemstubs.testng.SystemStubsListener;

@Listeners(SystemStubsListener.class)
public class KubernetesScaleDownStepTest {

  @Mock private UpgradeContext upgradeContext;

  private KubernetesScaleDownStep step;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testId() {
    step = new KubernetesScaleDownStep(new KubernetesScaleDownConfiguration());
    assertEquals(step.id(), "KubernetesScaleDownStep");
  }

  @Test
  public void testSkipWhenNotInK8() {
    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKubernetesServiceHost(null);
    config.setEnabled(true);
    step = new KubernetesScaleDownStep(config);

    assertTrue(step.skip(upgradeContext));
  }

  @Test
  public void testSkipWhenInK8ButDisabled() {
    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKubernetesServiceHost("10.0.0.1");
    config.setEnabled(false);
    step = new KubernetesScaleDownStep(config);

    assertTrue(step.skip(upgradeContext));
  }

  @Test
  public void testSkipWhenInK8AndEnabledButJavaImplementationDisabled() {
    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKubernetesServiceHost("10.0.0.1");
    config.setEnabled(true);
    config.setUseJavaImplementation(false);
    step = new KubernetesScaleDownStep(config);

    assertTrue(step.skip(upgradeContext));
  }

  @Test
  public void testDoNotSkipWhenInK8AndEnabledAndJavaImplementationEnabled() {
    when(upgradeContext.isScaleDownRequired()).thenReturn(true);
    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKubernetesServiceHost("10.0.0.1");
    config.setEnabled(true);
    config.setUseJavaImplementation(true);
    step = new KubernetesScaleDownStep(config);

    assertFalse(step.skip(upgradeContext));
  }

  @Test
  public void testSkipWhenScaleDownNotRequired() {
    when(upgradeContext.isScaleDownRequired()).thenReturn(false);
    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKubernetesServiceHost("10.0.0.1");
    config.setEnabled(true);
    config.setUseJavaImplementation(true);
    step = new KubernetesScaleDownStep(config);

    assertTrue(step.skip(upgradeContext));
  }

  @Test
  public void testSkipWhenConfigNullInK8() {
    step = new KubernetesScaleDownStep(null);
    // Null config yields Java defaults (kubernetesServiceHost=null), so step skips
    assertTrue(step.skip(upgradeContext));
  }

  @Test
  public void testExecutableUsesOperationContextObjectMapper() {
    when(upgradeContext.isScaleDownRequired()).thenReturn(true);
    OperationContext mockOpContext = mock(OperationContext.class);
    when(mockOpContext.getObjectMapper()).thenReturn(new ObjectMapper());
    when(upgradeContext.opContext()).thenReturn(mockOpContext);

    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKubernetesServiceHost("10.0.0.1");
    config.setEnabled(true);
    config.setUseJavaImplementation(true);
    config.setDeploymentEnvUpdates(
        "[{\"labelSelector\":\"app.kubernetes.io/name=datahub-gms\",\"env\":{\"PRE_PROCESS_HOOKS_UI_ENABLED\":\"false\",\"MCE_CONSUMER_ENABLED\":\"false\"}}]");
    step = new KubernetesScaleDownStep(config);

    // executable() runs; without in-cluster K8 it fails early (no client). Verifies context
    // provides opContext().getObjectMapper() for when captureAndScaleDown runs.
    step.executable().apply(upgradeContext);
  }

  @Test
  public void testExecutableWithExistingStateUnderMaxRetriesIncrementsAndPersists()
      throws Exception {
    ScaleDownState existingState =
        ScaleDownState.builder()
            .attempt(1)
            .scaleDownLabelSelectors(List.of("app=mae", "app=mce"))
            .scaleDownDeploymentNames(List.of("mae-deploy", "mce-deploy"))
            .build();
    KubernetesApiAccessor mockAccessor = mock(KubernetesApiAccessor.class);
    when(mockAccessor.getConfigMapState(anyString(), anyString(), any(ObjectMapper.class)))
        .thenReturn(Optional.of(existingState));
    when(upgradeContext.isScaleDownRequired()).thenReturn(true);
    when(upgradeContext.opContext()).thenReturn(mock(OperationContext.class));
    when(upgradeContext.opContext().getObjectMapper()).thenReturn(new ObjectMapper());

    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKubernetesServiceHost("10.0.0.1");
    config.setEnabled(true);
    config.setUseJavaImplementation(true);
    config.setMaxRetries(3);
    config.setDeploymentEnvUpdates("[{\"labelSelector\":\"app=gms\",\"env\":{\"KEY\":\"value\"}}]");
    step = new KubernetesScaleDownStep(config, () -> Optional.of(mockAccessor));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), com.linkedin.upgrade.DataHubUpgradeState.SUCCEEDED);
    verify(mockAccessor)
        .createOrReplaceConfigMap(
            anyString(), anyString(), any(ScaleDownState.class), any(ObjectMapper.class));
    assertEquals(existingState.getAttempt(), 2);
  }

  /**
   * When attempt exceeds maxRetries we restore saved state (scale + GMS env + waitForRollout),
   * delete the config map so a bad state does not block future runs, and fail the step.
   */
  @Test
  public void testExecutableWithExistingStateOverMaxRetriesRestoresAndFails() throws Exception {
    ScaleDownState existingState =
        ScaleDownState.builder()
            .attempt(5)
            .scaleDownLabelSelectors(List.of())
            .scaleDownDeploymentNames(List.of())
            .deployments(
                List.of(
                    ScaleDownState.DeploymentReplicas.builder().name("gms").replicas(1).build()))
            .build();
    KubernetesApiAccessor mockAccessor = mock(KubernetesApiAccessor.class);
    when(mockAccessor.getConfigMapState(anyString(), anyString(), any(ObjectMapper.class)))
        .thenReturn(Optional.of(existingState));
    when(upgradeContext.isScaleDownRequired()).thenReturn(true);
    when(upgradeContext.opContext()).thenReturn(mock(OperationContext.class));
    when(upgradeContext.opContext().getObjectMapper()).thenReturn(new ObjectMapper());

    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKubernetesServiceHost("10.0.0.1");
    config.setEnabled(true);
    config.setUseJavaImplementation(true);
    config.setMaxRetries(3);
    config.setDeploymentEnvUpdates("[{\"labelSelector\":\"app=gms\",\"env\":{\"KEY\":\"value\"}}]");
    step = new KubernetesScaleDownStep(config, () -> Optional.of(mockAccessor));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), com.linkedin.upgrade.DataHubUpgradeState.FAILED);
    verify(mockAccessor).scaleDeployment(eq("gms"), anyString(), eq(1));
    verify(mockAccessor).waitForRollout(eq("gms"), anyString());
    verify(mockAccessor).deleteConfigMap(anyString(), anyString());
  }

  /**
   * Over maxRetries with env in state (envBeforeByDeployment): restore scales back, restores env
   * per deployment, waits for rollouts, deletes config map, returns FAILED.
   */
  @Test
  public void testExecutableOverMaxRetriesRestoresGmsEnvDeletesConfigMapAndFails()
      throws Exception {
    ScaleDownState existingState =
        ScaleDownState.builder()
            .attempt(4)
            .scaleDownLabelSelectors(List.of())
            .scaleDownDeploymentNames(List.of())
            .deployments(
                List.of(
                    ScaleDownState.DeploymentReplicas.builder().name("gms").replicas(2).build()))
            .envBeforeByDeployment(Map.of("gms", Map.of("PRE_PROCESS_HOOKS_UI_ENABLED", "true")))
            .build();
    KubernetesApiAccessor mockAccessor = mock(KubernetesApiAccessor.class);
    when(mockAccessor.getConfigMapState(anyString(), anyString(), any(ObjectMapper.class)))
        .thenReturn(Optional.of(existingState));
    when(upgradeContext.isScaleDownRequired()).thenReturn(true);
    when(upgradeContext.opContext()).thenReturn(mock(OperationContext.class));
    when(upgradeContext.opContext().getObjectMapper()).thenReturn(new ObjectMapper());

    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKubernetesServiceHost("10.0.0.1");
    config.setEnabled(true);
    config.setUseJavaImplementation(true);
    config.setMaxRetries(3);
    config.setDeploymentEnvUpdates("[{\"labelSelector\":\"app=gms\",\"env\":{\"KEY\":\"value\"}}]");
    step = new KubernetesScaleDownStep(config, () -> Optional.of(mockAccessor));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), com.linkedin.upgrade.DataHubUpgradeState.FAILED);
    verify(mockAccessor).scaleDeployment(eq("gms"), anyString(), eq(2));
    verify(mockAccessor)
        .setDeploymentEnv(
            eq("gms"), anyString(), eq(Map.of("PRE_PROCESS_HOOKS_UI_ENABLED", "true")));
    verify(mockAccessor).waitForRollout(eq("gms"), anyString());
    verify(mockAccessor).deleteConfigMap(anyString(), anyString());
  }

  @Test
  public void testExecutableWithNoExistingStateCapturesAndPersists() throws Exception {
    KubernetesApiAccessor mockAccessor = mock(KubernetesApiAccessor.class);
    when(mockAccessor.getConfigMapState(anyString(), anyString(), any(ObjectMapper.class)))
        .thenReturn(Optional.empty());
    when(mockAccessor.getDeploymentNameByLabel(eq("app=gms"), anyString()))
        .thenReturn("gms-deploy");
    when(mockAccessor.getDeploymentNameByLabel(eq("app=mae"), anyString()))
        .thenReturn("datahub-mae-consumer");
    when(mockAccessor.getDeploymentNameByLabel(eq("app=mce"), anyString()))
        .thenReturn("datahub-mce-consumer");
    when(mockAccessor.getDeploymentReplicas(anyString(), anyString())).thenReturn(1);
    when(mockAccessor.getDeploymentEnv(anyString(), anyString())).thenReturn(Map.of("KEY", "old"));
    when(mockAccessor.listActiveJobNamesExceptSystemUpdate(anyString()))
        .thenReturn(Collections.emptyList());

    when(upgradeContext.isScaleDownRequired()).thenReturn(true);
    when(upgradeContext.opContext()).thenReturn(mock(OperationContext.class));
    when(upgradeContext.opContext().getObjectMapper()).thenReturn(new ObjectMapper());

    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKubernetesServiceHost("10.0.0.1");
    config.setEnabled(true);
    config.setUseJavaImplementation(true);
    config.setScaleDownDeploymentLabelSelectors("app=mae,app=mce");
    config.setDeploymentEnvUpdates("[{\"labelSelector\":\"app=gms\",\"env\":{\"KEY\":\"value\"}}]");
    step = new KubernetesScaleDownStep(config, () -> Optional.of(mockAccessor));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), com.linkedin.upgrade.DataHubUpgradeState.SUCCEEDED);
    verify(mockAccessor)
        .createOrReplaceConfigMap(
            anyString(), anyString(), any(ScaleDownState.class), any(ObjectMapper.class));
    verify(mockAccessor).scaleDeployment(eq("datahub-mae-consumer"), anyString(), eq(0));
    verify(mockAccessor).scaleDeployment(eq("datahub-mce-consumer"), anyString(), eq(0));
    verify(mockAccessor).setDeploymentEnv(eq("gms-deploy"), anyString(), any(Map.class));
  }

  @Test
  public void testExecutableFailsWhenAccessorEmpty() throws Exception {
    when(upgradeContext.isScaleDownRequired()).thenReturn(true);
    when(upgradeContext.opContext()).thenReturn(mock(OperationContext.class));
    when(upgradeContext.opContext().getObjectMapper()).thenReturn(new ObjectMapper());

    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKubernetesServiceHost("10.0.0.1");
    config.setEnabled(true);
    config.setUseJavaImplementation(true);
    step = new KubernetesScaleDownStep(config, () -> Optional.empty());

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), com.linkedin.upgrade.DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExecutableFailsWhenDeploymentEnvUpdatesInvalidJson() throws Exception {
    KubernetesApiAccessor mockAccessor = mock(KubernetesApiAccessor.class);
    when(mockAccessor.getConfigMapState(anyString(), anyString(), any(ObjectMapper.class)))
        .thenReturn(Optional.empty());
    when(mockAccessor.getDeploymentNameByLabel(anyString(), anyString())).thenReturn("gms-deploy");
    when(mockAccessor.getDeploymentReplicas(anyString(), anyString())).thenReturn(1);
    when(mockAccessor.getDeploymentEnv(anyString(), anyString()))
        .thenReturn(Collections.emptyMap());
    when(mockAccessor.listActiveJobNamesExceptSystemUpdate(anyString()))
        .thenReturn(Collections.emptyList());

    when(upgradeContext.isScaleDownRequired()).thenReturn(true);
    when(upgradeContext.opContext()).thenReturn(mock(OperationContext.class));
    when(upgradeContext.opContext().getObjectMapper()).thenReturn(new ObjectMapper());

    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKubernetesServiceHost("10.0.0.1");
    config.setEnabled(true);
    config.setUseJavaImplementation(true);
    config.setDeploymentEnvUpdates("not-valid-json-array");
    step = new KubernetesScaleDownStep(config, () -> Optional.of(mockAccessor));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), com.linkedin.upgrade.DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExecutableFailsWhenExceptionInCapture() throws Exception {
    KubernetesApiAccessor mockAccessor = mock(KubernetesApiAccessor.class);
    when(mockAccessor.getConfigMapState(anyString(), anyString(), any(ObjectMapper.class)))
        .thenReturn(Optional.empty());
    when(mockAccessor.getDeploymentNameByLabel(anyString(), anyString()))
        .thenThrow(new RuntimeException("API error"));

    when(upgradeContext.isScaleDownRequired()).thenReturn(true);
    when(upgradeContext.opContext()).thenReturn(mock(OperationContext.class));
    when(upgradeContext.opContext().getObjectMapper()).thenReturn(new ObjectMapper());

    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKubernetesServiceHost("10.0.0.1");
    config.setEnabled(true);
    config.setUseJavaImplementation(true);
    config.setDeploymentEnvUpdates("[{\"labelSelector\":\"app=gms\",\"env\":{\"KEY\":\"value\"}}]");
    step = new KubernetesScaleDownStep(config, () -> Optional.of(mockAccessor));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), com.linkedin.upgrade.DataHubUpgradeState.FAILED);
  }
}

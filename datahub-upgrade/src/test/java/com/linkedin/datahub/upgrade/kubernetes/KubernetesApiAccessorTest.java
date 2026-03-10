package com.linkedin.datahub.upgrade.kubernetes;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.kubernetes.KubernetesScaleDownConfiguration;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.testng.SystemStubsListener;

@Listeners(SystemStubsListener.class)
@SuppressWarnings({"unchecked", "rawtypes"})
public class KubernetesApiAccessorTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String NAMESPACE = "datahub-ns";

  @Test
  public void testResolveStateConfigMapNameUsesConfigWhenSet() {
    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setStateConfigMapName("my-custom-state-cm");
    assertEquals(
        KubernetesApiAccessor.resolveStateConfigMapName(config, NAMESPACE), "my-custom-state-cm");
  }

  @Test
  public void testResolveStateConfigMapNameUsesHelmReleaseWhenEnvSet() throws Exception {
    new EnvironmentVariables("HELM_RELEASE_NAME", "my-release")
        .execute(
            () -> {
              KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
              config.setStateConfigMapName(null);
              assertEquals(
                  KubernetesApiAccessor.resolveStateConfigMapName(config, NAMESPACE),
                  "my-release-system-update-scale-down-state");
            });
  }

  @Test
  public void testResolveStateConfigMapNameDefaultWhenNoConfigOrEnv() {
    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setStateConfigMapName(null);
    assertEquals(
        KubernetesApiAccessor.resolveStateConfigMapName(config, NAMESPACE),
        "datahub-system-update-scale-down-state");
  }

  @Test
  public void testGetNamespaceFromEnvironmentUsesNamespaceEnv() throws Exception {
    new EnvironmentVariables("NAMESPACE", "from-namespace-env")
        .execute(
            () ->
                assertEquals(
                    KubernetesApiAccessor.getNamespaceFromEnvironment(), "from-namespace-env"));
  }

  @Test
  public void testGetConfigMapStateEmptyWhenNoConfigMap() {
    KubernetesClient client = mock(KubernetesClient.class);
    MixedOperation configMapOps = mock(MixedOperation.class);
    Resource resource = mock(Resource.class);
    when(client.configMaps()).thenReturn(configMapOps);
    when(configMapOps.inNamespace(anyString())).thenReturn(configMapOps);
    when(configMapOps.withName(anyString())).thenReturn(resource);
    when(resource.get()).thenReturn(null);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);

    assertTrue(accessor.getConfigMapState("cm", NAMESPACE, MAPPER).isEmpty());
  }

  @Test
  public void testGetConfigMapStateEmptyWhenConfigMapHasNoStateKey() {
    ConfigMap cm = new ConfigMapBuilder().withData(Map.of("other", "value")).build();
    KubernetesClient client = mock(KubernetesClient.class);
    MixedOperation configMapOps = mock(MixedOperation.class);
    Resource resource = mock(Resource.class);
    when(client.configMaps()).thenReturn(configMapOps);
    when(configMapOps.inNamespace(anyString())).thenReturn(configMapOps);
    when(configMapOps.withName(anyString())).thenReturn(resource);
    when(resource.get()).thenReturn(cm);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);

    assertTrue(accessor.getConfigMapState("cm", NAMESPACE, MAPPER).isEmpty());
  }

  @Test
  public void testGetConfigMapStateReturnsStateWhenPresent() throws Exception {
    ScaleDownState state =
        ScaleDownState.builder()
            .attempt(1)
            .scaleDownLabelSelectors(List.of("app=mae"))
            .scaleDownDeploymentNames(List.of("mae-deploy"))
            .build();
    String json = MAPPER.writeValueAsString(state);
    ConfigMap cm = new ConfigMapBuilder().withData(Map.of("state", json)).build();
    KubernetesClient client = mock(KubernetesClient.class);
    MixedOperation configMapOps = mock(MixedOperation.class);
    Resource resource = mock(Resource.class);
    when(client.configMaps()).thenReturn(configMapOps);
    when(configMapOps.inNamespace(anyString())).thenReturn(configMapOps);
    when(configMapOps.withName(anyString())).thenReturn(resource);
    when(resource.get()).thenReturn(cm);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);

    assertTrue(accessor.getConfigMapState("cm", NAMESPACE, MAPPER).isPresent());
    assertEquals(accessor.getConfigMapState("cm", NAMESPACE, MAPPER).get().getAttempt(), 1);
    assertEquals(
        accessor.getConfigMapState("cm", NAMESPACE, MAPPER).get().getScaleDownDeploymentNames(),
        List.of("mae-deploy"));
  }

  @Test
  public void testCreateOrReplaceConfigMapCallsClient() throws Exception {
    KubernetesClient client = mock(KubernetesClient.class);
    MixedOperation configMapOps = mock(MixedOperation.class);
    when(client.configMaps()).thenReturn(configMapOps);
    when(configMapOps.inNamespace(anyString())).thenReturn(configMapOps);

    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    ScaleDownState state = ScaleDownState.builder().attempt(1).build();
    accessor.createOrReplaceConfigMap("state-cm", NAMESPACE, state, MAPPER);

    verify(configMapOps).createOrReplace(any(ConfigMap.class));
  }

  @Test
  public void testDeleteConfigMapCallsClient() {
    KubernetesClient client = mock(KubernetesClient.class);
    Resource resource = mock(Resource.class);
    MixedOperation configMapOps = mock(MixedOperation.class);
    when(client.configMaps()).thenReturn(configMapOps);
    when(configMapOps.inNamespace(anyString())).thenReturn(configMapOps);
    when(configMapOps.withName(anyString())).thenReturn(resource);

    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    accessor.deleteConfigMap("state-cm", NAMESPACE);

    verify(resource).delete();
  }

  @Test
  public void testCreateInClusterReturnsEmptyWhenNoK8Env() throws Exception {
    new EnvironmentVariables("KUBERNETES_SERVICE_HOST", "")
        .execute(
            () -> {
              assertTrue(KubernetesApiAccessor.createInCluster(null).isEmpty());
            });
  }

  @Test
  public void testGetNamespaceFromEnvironmentUsesPodNamespaceWhenNamespaceUnset() throws Exception {
    new EnvironmentVariables("NAMESPACE", "")
        .execute(
            () ->
                new EnvironmentVariables("POD_NAMESPACE", "from-pod-namespace")
                    .execute(
                        () ->
                            assertEquals(
                                KubernetesApiAccessor.getNamespaceFromEnvironment(),
                                "from-pod-namespace")));
  }

  @Test
  public void testGetNamespaceFromEnvironmentReturnsDefaultWhenNoEnv() throws Exception {
    new EnvironmentVariables("NAMESPACE", "")
        .execute(
            () ->
                new EnvironmentVariables("POD_NAMESPACE", "")
                    .execute(
                        () ->
                            assertEquals(
                                KubernetesApiAccessor.getNamespaceFromEnvironment(), "default")));
  }

  @Test
  public void testGetDeploymentNameByLabelReturnsNameWhenMatch() {
    Deployment deployment =
        new DeploymentBuilder()
            .withNewMetadata()
            .withName("gms-deployment")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();
    DeploymentList list = new DeploymentList();
    list.setItems(List.of(deployment));

    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.list(any())).thenReturn(list);

    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertEquals(accessor.getDeploymentNameByLabel("app=gms", NAMESPACE), "gms-deployment");
  }

  @Test
  public void testGetDeploymentNameByLabelReturnsNullWhenEmptyList() {
    DeploymentList list = new DeploymentList();
    list.setItems(List.of());

    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.list(any())).thenReturn(list);

    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertNull(accessor.getDeploymentNameByLabel("app=gms", NAMESPACE));
  }

  @Test
  public void testGetDeploymentByNamePatternReturnsNullWhenPatternNull() {
    KubernetesClient client = mock(KubernetesClient.class);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertNull(accessor.getDeploymentByNamePattern(null, NAMESPACE));
  }

  @Test
  public void testGetDeploymentByNamePatternReturnsNullWhenPatternEmpty() {
    KubernetesClient client = mock(KubernetesClient.class);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertNull(accessor.getDeploymentByNamePattern("", NAMESPACE));
  }

  @Test
  public void testGetDeploymentByNamePatternReturnsNameWhenMatch() {
    Deployment deployment =
        new DeploymentBuilder()
            .withNewMetadata()
            .withName("datahub-mae-consumer")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();
    DeploymentList list = new DeploymentList();
    list.setItems(List.of(deployment));

    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.list()).thenReturn(list);

    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertEquals(accessor.getDeploymentByNamePattern("mae", NAMESPACE), "datahub-mae-consumer");
  }

  @Test
  public void testGetDeploymentReplicasReturnsReplicasFromDeployment() {
    Deployment deployment =
        new DeploymentBuilder()
            .withNewMetadata()
            .withName("gms")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .withReplicas(3)
            .endSpec()
            .build();

    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    RollableScalableResource<Deployment> resource = mock(RollableScalableResource.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.withName("gms")).thenReturn(resource);
    when(resource.get()).thenReturn(deployment);

    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertEquals(accessor.getDeploymentReplicas("gms", NAMESPACE), 3);
  }

  @Test
  public void testGetDeploymentReplicasReturnsZeroWhenDeploymentNull() {
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    RollableScalableResource<Deployment> resource = mock(RollableScalableResource.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.withName(anyString())).thenReturn(resource);
    when(resource.get()).thenReturn(null);

    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertEquals(accessor.getDeploymentReplicas("gms", NAMESPACE), 0);
  }

  @Test
  public void testGetDeploymentEnvReturnsEnvMapFromDeployment() {
    Deployment deployment =
        new DeploymentBuilder()
            .withNewMetadata()
            .withName("gms")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .withNewTemplate()
            .withNewSpec()
            .addNewContainer()
            .withName("main")
            .withEnv(new EnvVar("VAR1", "value1", null), new EnvVar("VAR2", "value2", null))
            .endContainer()
            .endSpec()
            .endTemplate()
            .endSpec()
            .build();

    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    RollableScalableResource<Deployment> resource = mock(RollableScalableResource.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.withName("gms")).thenReturn(resource);
    when(resource.get()).thenReturn(deployment);

    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    Map<String, String> env = accessor.getDeploymentEnv("gms", NAMESPACE);
    assertEquals(env.get("VAR1"), "value1");
    assertEquals(env.get("VAR2"), "value2");
  }

  @Test
  public void testScaleDeploymentCallsScale() {
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    RollableScalableResource<Deployment> resource = mock(RollableScalableResource.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.withName("gms")).thenReturn(resource);

    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    accessor.scaleDeployment("gms", NAMESPACE, 0);
    verify(resource).scale(0);
  }

  @Test
  public void testSetDeploymentEnvCallsEdit() {
    Deployment deployment =
        new DeploymentBuilder()
            .withNewMetadata()
            .withName("gms")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .withNewTemplate()
            .withNewSpec()
            .addNewContainer()
            .withName("main")
            .endContainer()
            .endSpec()
            .endTemplate()
            .endSpec()
            .build();

    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    RollableScalableResource<Deployment> resource = mock(RollableScalableResource.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.withName("gms")).thenReturn(resource);
    when(resource.get()).thenReturn(deployment);
    when(resource.edit(any(UnaryOperator.class))).thenReturn(deployment);

    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    accessor.setDeploymentEnv("gms", NAMESPACE, Map.of("KEY", "value"));
    verify(resource).edit(any(UnaryOperator.class));
  }

  @Test
  public void testListActiveJobNamesExceptSystemUpdateReturnsActiveJobsOnly() {
    io.fabric8.kubernetes.api.model.batch.v1.Job activeJob =
        new io.fabric8.kubernetes.api.model.batch.v1.JobBuilder()
            .withNewMetadata()
            .withName("other-job")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewStatus()
            .withActive(1)
            .endStatus()
            .build();
    io.fabric8.kubernetes.api.model.batch.v1.JobList jobList =
        new io.fabric8.kubernetes.api.model.batch.v1.JobList();
    jobList.setItems(List.of(activeJob));

    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.BatchAPIGroupDSL batchApi =
        mock(io.fabric8.kubernetes.client.dsl.BatchAPIGroupDSL.class);
    io.fabric8.kubernetes.client.dsl.V1BatchAPIGroupDSL batchV1 =
        mock(io.fabric8.kubernetes.client.dsl.V1BatchAPIGroupDSL.class);
    MixedOperation<
            io.fabric8.kubernetes.api.model.batch.v1.Job,
            io.fabric8.kubernetes.api.model.batch.v1.JobList,
            ScalableResource<io.fabric8.kubernetes.api.model.batch.v1.Job>>
        jobsOp = mock(MixedOperation.class);
    when(client.batch()).thenReturn(batchApi);
    when(batchApi.v1()).thenReturn(batchV1);
    when(batchV1.jobs()).thenReturn(jobsOp);
    when(jobsOp.inNamespace(anyString())).thenReturn(jobsOp);
    when(jobsOp.list()).thenReturn(jobList);

    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    List<String> names = accessor.listActiveJobNamesExceptSystemUpdate(NAMESPACE);
    assertEquals(names.size(), 1);
    assertTrue(names.contains("other-job"));
  }

  @Test
  public void testListActiveJobNamesExceptSystemUpdateFiltersSystemUpdateJobs() {
    io.fabric8.kubernetes.api.model.batch.v1.Job systemUpdateJob =
        new io.fabric8.kubernetes.api.model.batch.v1.JobBuilder()
            .withNewMetadata()
            .withName("datahub-system-update-xyz")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewStatus()
            .withActive(1)
            .endStatus()
            .build();
    io.fabric8.kubernetes.api.model.batch.v1.JobList jobList =
        new io.fabric8.kubernetes.api.model.batch.v1.JobList();
    jobList.setItems(List.of(systemUpdateJob));

    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.BatchAPIGroupDSL batchApi =
        mock(io.fabric8.kubernetes.client.dsl.BatchAPIGroupDSL.class);
    io.fabric8.kubernetes.client.dsl.V1BatchAPIGroupDSL batchV1 =
        mock(io.fabric8.kubernetes.client.dsl.V1BatchAPIGroupDSL.class);
    MixedOperation<
            io.fabric8.kubernetes.api.model.batch.v1.Job,
            io.fabric8.kubernetes.api.model.batch.v1.JobList,
            ScalableResource<io.fabric8.kubernetes.api.model.batch.v1.Job>>
        jobsOp = mock(MixedOperation.class);
    when(client.batch()).thenReturn(batchApi);
    when(batchApi.v1()).thenReturn(batchV1);
    when(batchV1.jobs()).thenReturn(jobsOp);
    when(jobsOp.inNamespace(anyString())).thenReturn(jobsOp);
    when(jobsOp.list()).thenReturn(jobList);

    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    List<String> names = accessor.listActiveJobNamesExceptSystemUpdate(NAMESPACE);
    assertTrue(names.isEmpty());
  }

  @Test
  public void testDeleteJobCallsClient() {
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.BatchAPIGroupDSL batchApi =
        mock(io.fabric8.kubernetes.client.dsl.BatchAPIGroupDSL.class);
    io.fabric8.kubernetes.client.dsl.V1BatchAPIGroupDSL batchV1 =
        mock(io.fabric8.kubernetes.client.dsl.V1BatchAPIGroupDSL.class);
    MixedOperation<
            io.fabric8.kubernetes.api.model.batch.v1.Job,
            io.fabric8.kubernetes.api.model.batch.v1.JobList,
            ScalableResource<io.fabric8.kubernetes.api.model.batch.v1.Job>>
        jobsOp = mock(MixedOperation.class);
    ScalableResource<io.fabric8.kubernetes.api.model.batch.v1.Job> jobResource =
        mock(ScalableResource.class);
    when(client.batch()).thenReturn(batchApi);
    when(batchApi.v1()).thenReturn(batchV1);
    when(batchV1.jobs()).thenReturn(jobsOp);
    when(jobsOp.inNamespace(anyString())).thenReturn(jobsOp);
    when(jobsOp.withName("my-job")).thenReturn(jobResource);

    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    accessor.deleteJob("my-job", NAMESPACE);
    verify(jobResource).delete();
  }

  @Test
  public void testGetConfigMapStateReturnsEmptyWhenGetThrowsNonNotFound() {
    KubernetesClient client = mock(KubernetesClient.class);
    MixedOperation configMapOps = mock(MixedOperation.class);
    Resource resource = mock(Resource.class);
    when(client.configMaps()).thenReturn(configMapOps);
    when(configMapOps.inNamespace(anyString())).thenReturn(configMapOps);
    when(configMapOps.withName(anyString())).thenReturn(resource);
    when(resource.get()).thenThrow(new RuntimeException("Server error 500"));

    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertTrue(accessor.getConfigMapState("cm", NAMESPACE, MAPPER).isEmpty());
  }

  @Test
  public void testFullConfigForKedaAndRollout() {
    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKedaGroup("keda.sh");
    config.setKedaVersion("v1alpha1");
    config.setKedaScaledObjectsPlural("scaledobjects");
    config.setRolloutPollSeconds(5);
    config.setRolloutMaxWaitSeconds(300);
    assertNotNull(config.getKedaGroup());
    assertNotNull(config.getKedaVersion());
    assertNotNull(config.getKedaScaledObjectsPlural());
    assertFalse(config.getRolloutPollSeconds() <= 0);
    assertFalse(config.getRolloutMaxWaitSeconds() <= 0);
  }

  @Test
  public void testGetConfigMapStateEmptyWhenDataNull() {
    ConfigMap cm = new ConfigMapBuilder().build();
    cm.setData(null);
    KubernetesClient client = mock(KubernetesClient.class);
    MixedOperation configMapOps = mock(MixedOperation.class);
    Resource resource = mock(Resource.class);
    when(client.configMaps()).thenReturn(configMapOps);
    when(configMapOps.inNamespace(anyString())).thenReturn(configMapOps);
    when(configMapOps.withName(anyString())).thenReturn(resource);
    when(resource.get()).thenReturn(cm);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertTrue(accessor.getConfigMapState("cm", NAMESPACE, MAPPER).isEmpty());
  }

  @Test
  public void testGetConfigMapStateEmptyWhenGetThrowsNotFound() {
    KubernetesClient client = mock(KubernetesClient.class);
    MixedOperation configMapOps = mock(MixedOperation.class);
    Resource resource = mock(Resource.class);
    when(client.configMaps()).thenReturn(configMapOps);
    when(configMapOps.inNamespace(anyString())).thenReturn(configMapOps);
    when(configMapOps.withName(anyString())).thenReturn(resource);
    when(resource.get()).thenThrow(new RuntimeException("404 Not Found"));
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertTrue(accessor.getConfigMapState("cm", NAMESPACE, MAPPER).isEmpty());
  }

  @Test
  public void testGetConfigMapStateEmptyWhenInvalidJsonInState() {
    ConfigMap cm = new ConfigMapBuilder().withData(Map.of("state", "not-valid-json")).build();
    KubernetesClient client = mock(KubernetesClient.class);
    MixedOperation configMapOps = mock(MixedOperation.class);
    Resource resource = mock(Resource.class);
    when(client.configMaps()).thenReturn(configMapOps);
    when(configMapOps.inNamespace(anyString())).thenReturn(configMapOps);
    when(configMapOps.withName(anyString())).thenReturn(resource);
    when(resource.get()).thenReturn(cm);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertTrue(accessor.getConfigMapState("cm", NAMESPACE, MAPPER).isEmpty());
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*serialize.*")
  public void testCreateOrReplaceConfigMapThrowsWhenSerializationFails() throws Exception {
    ObjectMapper failingMapper = mock(ObjectMapper.class);
    when(failingMapper.writeValueAsString(any())).thenThrow(new RuntimeException("serialize fail"));
    KubernetesClient client = mock(KubernetesClient.class);
    when(client.configMaps()).thenReturn(mock(MixedOperation.class));
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    accessor.createOrReplaceConfigMap(
        "cm", NAMESPACE, ScaleDownState.builder().attempt(1).build(), failingMapper);
  }

  @Test
  public void testDeleteConfigMapSwallowsNonNotFoundException() {
    KubernetesClient client = mock(KubernetesClient.class);
    Resource resource = mock(Resource.class);
    MixedOperation configMapOps = mock(MixedOperation.class);
    when(client.configMaps()).thenReturn(configMapOps);
    when(configMapOps.inNamespace(anyString())).thenReturn(configMapOps);
    when(configMapOps.withName(anyString())).thenReturn(resource);
    when(resource.delete()).thenThrow(new RuntimeException("Server error 500"));
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    accessor.deleteConfigMap("state-cm", NAMESPACE);
  }

  @Test
  public void testGetDeploymentNameByLabelReturnsNullWhenListThrows() {
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.list(any())).thenThrow(new RuntimeException("API error"));
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertNull(accessor.getDeploymentNameByLabel("app=gms", NAMESPACE));
  }

  @Test
  public void testGetDeploymentNameByLabelReturnsNullWhenFirstItemHasNullMetadata() {
    Deployment deployment = new DeploymentBuilder().withNewSpec().endSpec().build();
    deployment.setMetadata(null);
    DeploymentList list = new DeploymentList();
    list.setItems(List.of(deployment));
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.list(any())).thenReturn(list);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertNull(accessor.getDeploymentNameByLabel("app=gms", NAMESPACE));
  }

  @Test
  public void testGetDeploymentByNamePatternReturnsNullWhenListNull() {
    DeploymentList list = new DeploymentList();
    list.setItems(null);
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.list()).thenReturn(list);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertNull(accessor.getDeploymentByNamePattern("mae", NAMESPACE));
  }

  @Test
  public void testGetDeploymentByNamePatternReturnsNullWhenListThrows() {
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.list()).thenThrow(new RuntimeException("API error"));
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertNull(accessor.getDeploymentByNamePattern("mae", NAMESPACE));
  }

  @Test
  public void testGetDeploymentByNamePatternReturnsNullWhenNoMatch() {
    Deployment deployment =
        new DeploymentBuilder()
            .withNewMetadata()
            .withName("other-app")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();
    DeploymentList list = new DeploymentList();
    list.setItems(List.of(deployment));
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.list()).thenReturn(list);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertNull(accessor.getDeploymentByNamePattern("mae", NAMESPACE));
  }

  @Test
  public void testGetDeploymentReplicasReturnsZeroWhenSpecNull() {
    Deployment deployment =
        new DeploymentBuilder().withNewMetadata().withName("gms").endMetadata().build();
    deployment.setSpec(null);
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    RollableScalableResource<Deployment> resource = mock(RollableScalableResource.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.withName("gms")).thenReturn(resource);
    when(resource.get()).thenReturn(deployment);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertEquals(accessor.getDeploymentReplicas("gms", NAMESPACE), 0);
  }

  @Test
  public void testGetDeploymentReplicasReturnsZeroWhenReplicasNull() {
    Deployment deployment =
        new DeploymentBuilder()
            .withNewMetadata()
            .withName("gms")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();
    deployment.getSpec().setReplicas(null);
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    RollableScalableResource<Deployment> resource = mock(RollableScalableResource.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.withName("gms")).thenReturn(resource);
    when(resource.get()).thenReturn(deployment);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertEquals(accessor.getDeploymentReplicas("gms", NAMESPACE), 0);
  }

  @Test
  public void testGetDeploymentReplicasReturnsZeroWhenException() {
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    RollableScalableResource<Deployment> resource = mock(RollableScalableResource.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.withName(anyString())).thenReturn(resource);
    when(resource.get()).thenThrow(new RuntimeException("API error"));
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertEquals(accessor.getDeploymentReplicas("gms", NAMESPACE), 0);
  }

  @Test
  public void testGetDeploymentEnvReturnsEmptyWhenDeploymentNull() {
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    RollableScalableResource<Deployment> resource = mock(RollableScalableResource.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.withName("gms")).thenReturn(resource);
    when(resource.get()).thenReturn(null);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertTrue(accessor.getDeploymentEnv("gms", NAMESPACE).isEmpty());
  }

  @Test
  public void testGetDeploymentEnvReturnsEmptyWhenNoContainers() {
    Deployment deployment =
        new DeploymentBuilder()
            .withNewMetadata()
            .withName("gms")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .withNewTemplate()
            .withNewSpec()
            .endSpec()
            .endTemplate()
            .endSpec()
            .build();
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    RollableScalableResource<Deployment> resource = mock(RollableScalableResource.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.withName("gms")).thenReturn(resource);
    when(resource.get()).thenReturn(deployment);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertTrue(accessor.getDeploymentEnv("gms", NAMESPACE).isEmpty());
  }

  @Test
  public void testGetDeploymentEnvTreatsNullValueAsEmptyString() {
    Deployment deployment =
        new DeploymentBuilder()
            .withNewMetadata()
            .withName("gms")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .withNewTemplate()
            .withNewSpec()
            .addNewContainer()
            .withName("main")
            .withEnv(new EnvVar("VAR1", null, null))
            .endContainer()
            .endSpec()
            .endTemplate()
            .endSpec()
            .build();
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    RollableScalableResource<Deployment> resource = mock(RollableScalableResource.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.withName("gms")).thenReturn(resource);
    when(resource.get()).thenReturn(deployment);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    Map<String, String> env = accessor.getDeploymentEnv("gms", NAMESPACE);
    assertEquals(env.get("VAR1"), "");
  }

  @Test
  public void testGetDeploymentEnvReturnsEmptyWhenException() {
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    RollableScalableResource<Deployment> resource = mock(RollableScalableResource.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.withName("gms")).thenReturn(resource);
    when(resource.get()).thenThrow(new RuntimeException("API error"));
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertTrue(accessor.getDeploymentEnv("gms", NAMESPACE).isEmpty());
  }

  @Test
  public void testSetDeploymentEnvNoOpWhenNoContainers() {
    Deployment deployment =
        new DeploymentBuilder()
            .withNewMetadata()
            .withName("gms")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .withNewTemplate()
            .withNewSpec()
            .endSpec()
            .endTemplate()
            .endSpec()
            .build();
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    RollableScalableResource<Deployment> resource = mock(RollableScalableResource.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.withName("gms")).thenReturn(resource);
    when(resource.get()).thenReturn(deployment);
    when(resource.edit(any(UnaryOperator.class)))
        .thenAnswer(inv -> ((UnaryOperator<Deployment>) inv.getArgument(0)).apply(deployment));
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    accessor.setDeploymentEnv("gms", NAMESPACE, Map.of("KEY", "value"));
    verify(resource).edit(any(UnaryOperator.class));
  }

  @Test
  public void testSetDeploymentEnvWithNullEnvCreatesNewList() {
    Deployment deployment =
        new DeploymentBuilder()
            .withNewMetadata()
            .withName("gms")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .withNewTemplate()
            .withNewSpec()
            .addNewContainer()
            .withName("main")
            .withEnv((List<EnvVar>) null)
            .endContainer()
            .endSpec()
            .endTemplate()
            .endSpec()
            .build();
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    RollableScalableResource<Deployment> resource = mock(RollableScalableResource.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.withName("gms")).thenReturn(resource);
    when(resource.get()).thenReturn(deployment);
    when(resource.edit(any(UnaryOperator.class)))
        .thenAnswer(inv -> ((UnaryOperator<Deployment>) inv.getArgument(0)).apply(deployment));
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    accessor.setDeploymentEnv("gms", NAMESPACE, Map.of("NEW_KEY", "newVal"));
    verify(resource).edit(any(UnaryOperator.class));
  }

  @Test
  public void testWaitForRolloutReturnsWhenDesiredEqualsUpdated() {
    Deployment deployment =
        new DeploymentBuilder()
            .withNewMetadata()
            .withName("gms")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .withReplicas(2)
            .endSpec()
            .withNewStatus()
            .withUpdatedReplicas(2)
            .endStatus()
            .build();
    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setRolloutPollSeconds(1);
    config.setRolloutMaxWaitSeconds(10);
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL apps =
        mock(io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL.class);
    MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentsOp =
        mock(MixedOperation.class);
    RollableScalableResource<Deployment> resource = mock(RollableScalableResource.class);
    when(client.apps()).thenReturn(apps);
    when(apps.deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(anyString())).thenReturn(deploymentsOp);
    when(deploymentsOp.withName("gms")).thenReturn(resource);
    when(resource.get()).thenReturn(deployment);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client, config);
    accessor.waitForRollout("gms", NAMESPACE);
  }

  @Test
  public void testDeleteScaledObjectSwallowsWhenConfigNull() {
    KubernetesClient client = mock(KubernetesClient.class);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    accessor.deleteScaledObject("so-name", NAMESPACE);
    verify(client, never()).genericKubernetesResources(any());
  }

  @Test
  public void testDeleteScaledObjectSuccessWithFullConfig() {
    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKedaGroup("keda.sh");
    config.setKedaVersion("v1alpha1");
    config.setKedaScaledObjectsPlural("scaledobjects");
    config.setRolloutPollSeconds(1);
    config.setRolloutMaxWaitSeconds(60);
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.MixedOperation genericOp =
        mock(io.fabric8.kubernetes.client.dsl.MixedOperation.class);
    io.fabric8.kubernetes.client.dsl.NonNamespaceOperation nsOp =
        mock(io.fabric8.kubernetes.client.dsl.NonNamespaceOperation.class);
    Resource genericResource = mock(Resource.class);
    when(client.genericKubernetesResources(any())).thenReturn(genericOp);
    when(genericOp.inNamespace(anyString())).thenReturn(nsOp);
    when(nsOp.withName("my-scaledobject")).thenReturn(genericResource);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client, config);
    accessor.deleteScaledObject("my-scaledobject", NAMESPACE);
    verify(genericResource).delete();
  }

  @Test
  public void testDeleteScaledObjectSwallowsKedaUnavailableException() {
    KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
    config.setKedaGroup("keda.sh");
    config.setKedaVersion("v1alpha1");
    config.setKedaScaledObjectsPlural("scaledobjects");
    config.setRolloutPollSeconds(1);
    config.setRolloutMaxWaitSeconds(60);
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.MixedOperation genericOp =
        mock(io.fabric8.kubernetes.client.dsl.MixedOperation.class);
    io.fabric8.kubernetes.client.dsl.NonNamespaceOperation nsOp =
        mock(io.fabric8.kubernetes.client.dsl.NonNamespaceOperation.class);
    Resource genericResource = mock(Resource.class);
    when(client.genericKubernetesResources(any())).thenReturn(genericOp);
    when(genericOp.inNamespace(anyString())).thenReturn(nsOp);
    when(nsOp.withName(anyString())).thenReturn(genericResource);
    when(genericResource.delete())
        .thenThrow(new RuntimeException("the server could not find the requested resource"));
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client, config);
    accessor.deleteScaledObject("so-name", NAMESPACE);
  }

  @Test
  public void testListActiveJobNamesExceptSystemUpdateReturnsEmptyWhenItemsNull() {
    io.fabric8.kubernetes.api.model.batch.v1.JobList jobList =
        new io.fabric8.kubernetes.api.model.batch.v1.JobList();
    jobList.setItems(null);
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.BatchAPIGroupDSL batchApi =
        mock(io.fabric8.kubernetes.client.dsl.BatchAPIGroupDSL.class);
    io.fabric8.kubernetes.client.dsl.V1BatchAPIGroupDSL batchV1 =
        mock(io.fabric8.kubernetes.client.dsl.V1BatchAPIGroupDSL.class);
    MixedOperation<
            io.fabric8.kubernetes.api.model.batch.v1.Job,
            io.fabric8.kubernetes.api.model.batch.v1.JobList,
            ScalableResource<io.fabric8.kubernetes.api.model.batch.v1.Job>>
        jobsOp = mock(MixedOperation.class);
    when(client.batch()).thenReturn(batchApi);
    when(batchApi.v1()).thenReturn(batchV1);
    when(batchV1.jobs()).thenReturn(jobsOp);
    when(jobsOp.inNamespace(anyString())).thenReturn(jobsOp);
    when(jobsOp.list()).thenReturn(jobList);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertTrue(accessor.listActiveJobNamesExceptSystemUpdate(NAMESPACE).isEmpty());
  }

  @Test
  public void testListActiveJobNamesExceptSystemUpdateSkipsJobWithNullName() {
    io.fabric8.kubernetes.api.model.batch.v1.Job job =
        new io.fabric8.kubernetes.api.model.batch.v1.JobBuilder()
            .withNewMetadata()
            .withName(null)
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewStatus()
            .withActive(1)
            .endStatus()
            .build();
    io.fabric8.kubernetes.api.model.batch.v1.JobList jobList =
        new io.fabric8.kubernetes.api.model.batch.v1.JobList();
    jobList.setItems(List.of(job));
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.BatchAPIGroupDSL batchApi =
        mock(io.fabric8.kubernetes.client.dsl.BatchAPIGroupDSL.class);
    io.fabric8.kubernetes.client.dsl.V1BatchAPIGroupDSL batchV1 =
        mock(io.fabric8.kubernetes.client.dsl.V1BatchAPIGroupDSL.class);
    MixedOperation<
            io.fabric8.kubernetes.api.model.batch.v1.Job,
            io.fabric8.kubernetes.api.model.batch.v1.JobList,
            ScalableResource<io.fabric8.kubernetes.api.model.batch.v1.Job>>
        jobsOp = mock(MixedOperation.class);
    when(client.batch()).thenReturn(batchApi);
    when(batchApi.v1()).thenReturn(batchV1);
    when(batchV1.jobs()).thenReturn(jobsOp);
    when(jobsOp.inNamespace(anyString())).thenReturn(jobsOp);
    when(jobsOp.list()).thenReturn(jobList);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertTrue(accessor.listActiveJobNamesExceptSystemUpdate(NAMESPACE).isEmpty());
  }

  @Test
  public void testListActiveJobNamesExceptSystemUpdateSkipsInactiveJob() {
    io.fabric8.kubernetes.api.model.batch.v1.Job job =
        new io.fabric8.kubernetes.api.model.batch.v1.JobBuilder()
            .withNewMetadata()
            .withName("completed-job")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewStatus()
            .withActive(0)
            .endStatus()
            .build();
    io.fabric8.kubernetes.api.model.batch.v1.JobList jobList =
        new io.fabric8.kubernetes.api.model.batch.v1.JobList();
    jobList.setItems(List.of(job));
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.BatchAPIGroupDSL batchApi =
        mock(io.fabric8.kubernetes.client.dsl.BatchAPIGroupDSL.class);
    io.fabric8.kubernetes.client.dsl.V1BatchAPIGroupDSL batchV1 =
        mock(io.fabric8.kubernetes.client.dsl.V1BatchAPIGroupDSL.class);
    MixedOperation<
            io.fabric8.kubernetes.api.model.batch.v1.Job,
            io.fabric8.kubernetes.api.model.batch.v1.JobList,
            ScalableResource<io.fabric8.kubernetes.api.model.batch.v1.Job>>
        jobsOp = mock(MixedOperation.class);
    when(client.batch()).thenReturn(batchApi);
    when(batchApi.v1()).thenReturn(batchV1);
    when(batchV1.jobs()).thenReturn(jobsOp);
    when(jobsOp.inNamespace(anyString())).thenReturn(jobsOp);
    when(jobsOp.list()).thenReturn(jobList);
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertTrue(accessor.listActiveJobNamesExceptSystemUpdate(NAMESPACE).isEmpty());
  }

  @Test
  public void testListActiveJobNamesExceptSystemUpdateReturnsEmptyWhenListThrows() {
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.BatchAPIGroupDSL batchApi =
        mock(io.fabric8.kubernetes.client.dsl.BatchAPIGroupDSL.class);
    io.fabric8.kubernetes.client.dsl.V1BatchAPIGroupDSL batchV1 =
        mock(io.fabric8.kubernetes.client.dsl.V1BatchAPIGroupDSL.class);
    MixedOperation<
            io.fabric8.kubernetes.api.model.batch.v1.Job,
            io.fabric8.kubernetes.api.model.batch.v1.JobList,
            ScalableResource<io.fabric8.kubernetes.api.model.batch.v1.Job>>
        jobsOp = mock(MixedOperation.class);
    when(client.batch()).thenReturn(batchApi);
    when(batchApi.v1()).thenReturn(batchV1);
    when(batchV1.jobs()).thenReturn(jobsOp);
    when(jobsOp.inNamespace(anyString())).thenReturn(jobsOp);
    when(jobsOp.list()).thenThrow(new RuntimeException("API error"));
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    assertTrue(accessor.listActiveJobNamesExceptSystemUpdate(NAMESPACE).isEmpty());
  }

  @Test
  public void testDeleteJobSwallowsException() {
    KubernetesClient client = mock(KubernetesClient.class);
    io.fabric8.kubernetes.client.dsl.BatchAPIGroupDSL batchApi =
        mock(io.fabric8.kubernetes.client.dsl.BatchAPIGroupDSL.class);
    io.fabric8.kubernetes.client.dsl.V1BatchAPIGroupDSL batchV1 =
        mock(io.fabric8.kubernetes.client.dsl.V1BatchAPIGroupDSL.class);
    MixedOperation<
            io.fabric8.kubernetes.api.model.batch.v1.Job,
            io.fabric8.kubernetes.api.model.batch.v1.JobList,
            ScalableResource<io.fabric8.kubernetes.api.model.batch.v1.Job>>
        jobsOp = mock(MixedOperation.class);
    ScalableResource<io.fabric8.kubernetes.api.model.batch.v1.Job> jobResource =
        mock(ScalableResource.class);
    when(client.batch()).thenReturn(batchApi);
    when(batchApi.v1()).thenReturn(batchV1);
    when(batchV1.jobs()).thenReturn(jobsOp);
    when(jobsOp.inNamespace(anyString())).thenReturn(jobsOp);
    when(jobsOp.withName("my-job")).thenReturn(jobResource);
    when(jobResource.delete()).thenThrow(new RuntimeException("API error"));
    KubernetesApiAccessor accessor = new KubernetesApiAccessor(client);
    accessor.deleteJob("my-job", NAMESPACE);
  }

  @Test
  public void testResolveStateConfigMapNameUsesHelmWhenConfigEmptyString() throws Exception {
    new EnvironmentVariables("HELM_RELEASE_NAME", "my-release")
        .execute(
            () -> {
              KubernetesScaleDownConfiguration config = new KubernetesScaleDownConfiguration();
              config.setStateConfigMapName("");
              assertEquals(
                  KubernetesApiAccessor.resolveStateConfigMapName(config, NAMESPACE),
                  "my-release-system-update-scale-down-state");
            });
  }
}

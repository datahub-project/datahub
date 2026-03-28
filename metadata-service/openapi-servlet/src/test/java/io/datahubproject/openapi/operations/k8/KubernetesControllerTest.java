package io.datahubproject.openapi.operations.k8;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.datahub.authorization.BatchAuthorizationRequest;
import com.datahub.authorization.BatchAuthorizationResult;
import com.datahub.test.authorization.ConstantAuthorizationResultMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.openapi.config.KubernetesClientFactory;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.openapi.operations.k8.models.ConfigMapUpdateRequest;
import io.datahubproject.openapi.operations.k8.models.CronJobTriggerRequest;
import io.datahubproject.openapi.operations.k8.models.DeploymentEnvUpdateRequest;
import io.datahubproject.openapi.operations.k8.models.DeploymentScaleRequest;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.api.model.batch.v1.CronJob;
import io.fabric8.kubernetes.api.model.batch.v1.CronJobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.*;
import io.fabric8.kubernetes.client.dsl.ContainerResource;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest(classes = KubernetesControllerTest.TestConfig.class)
@AutoConfigureWebMvc
@AutoConfigureMockMvc
@SuppressWarnings({"unchecked", "rawtypes"})
public class KubernetesControllerTest extends AbstractTestNGSpringContextTests {

  private static final String NAMESPACE = "datahub";
  private static final String BASE_PATH = "/openapi/operations/k8";

  @Autowired private KubernetesController kubernetesController;
  @Autowired private MockMvc mockMvc;
  @Autowired private KubernetesClient kubernetesClient;
  @Autowired private AuthorizerChain authorizerChain;
  @Autowired private ObjectMapper objectMapper;

  @Autowired
  @org.springframework.beans.factory.annotation.Qualifier("systemOperationContext")
  private OperationContext systemOperationContext;

  @BeforeMethod
  public void setupMocks() {
    // Setup authentication
    Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
    AuthenticationContext.setAuthentication(authentication);

    // Setup authorization
    doReturn(
            new BatchAuthorizationResult(
                mock(BatchAuthorizationRequest.class),
                new ConstantAuthorizationResultMap(AuthorizationResult.Type.ALLOW)))
        .when(authorizerChain)
        .authorizeBatch(any(BatchAuthorizationRequest.class));

    // Setup K8s client config
    Config config = mock(Config.class);
    when(config.getNamespace()).thenReturn(NAMESPACE);
    when(kubernetesClient.getConfiguration()).thenReturn(config);
  }

  @Test
  public void testControllerInit() {
    assertNotNull(kubernetesController);
  }

  // ==================== Status Tests ====================

  @Test
  public void testGetStatus() throws Exception {
    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/status").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.available").value(true))
        .andExpect(jsonPath("$.namespace").value(NAMESPACE));
  }

  // ==================== Deployment Tests ====================

  @Test
  public void testListDeployments() throws Exception {
    Deployment deployment = createTestDeployment("test-deployment");

    MixedOperation deploymentsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    DeploymentList deploymentList = new DeploymentList();
    deploymentList.setItems(List.of(deployment));

    when(kubernetesClient.apps()).thenReturn(mock(AppsAPIGroupDSL.class));
    when(kubernetesClient.apps().deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.list()).thenReturn(deploymentList);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/deployments")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$.elements[0].metadata.name").value("test-deployment"));
  }

  @Test
  public void testScaleDeploymentBadRequest() throws Exception {
    DeploymentScaleRequest request = DeploymentScaleRequest.builder().build();

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/deployments/test/scale")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.error").exists());
  }

  @Test
  public void testUpdateDeploymentEnvBadRequest() throws Exception {
    DeploymentEnvUpdateRequest request = DeploymentEnvUpdateRequest.builder().build();

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/deployments/test/env")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.error").exists());
  }

  // ==================== Pod Tests ====================

  @Test
  public void testListPods() throws Exception {
    Pod pod = createTestPod("test-pod");

    MixedOperation podsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    PodList podList = new PodList();
    podList.setItems(List.of(pod));

    when(kubernetesClient.pods()).thenReturn(podsOp);
    when(podsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.list()).thenReturn(podList);

    mockMvc
        .perform(MockMvcRequestBuilders.get(BASE_PATH + "/pods").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1));
  }

  @Test
  public void testDeletePodNotFound() throws Exception {
    MixedOperation podsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    PodResource podResource = mock(PodResource.class);

    when(kubernetesClient.pods()).thenReturn(podsOp);
    when(podsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("nonexistent")).thenReturn(podResource);
    when(podResource.get()).thenReturn(null);

    mockMvc
        .perform(
            MockMvcRequestBuilders.delete(BASE_PATH + "/pods/nonexistent")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  // ==================== ConfigMap Tests ====================

  @Test
  public void testListConfigMaps() throws Exception {
    ConfigMap configMap = createTestConfigMap("test-config");

    MixedOperation configMapsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    ConfigMapList configMapList = new ConfigMapList();
    configMapList.setItems(List.of(configMap));

    when(kubernetesClient.configMaps()).thenReturn(configMapsOp);
    when(configMapsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.list()).thenReturn(configMapList);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/configmaps")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1));
  }

  @Test
  public void testUpdateConfigMapBadRequest() throws Exception {
    ConfigMapUpdateRequest request = ConfigMapUpdateRequest.builder().build();

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/configmaps/test")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.error").exists());
  }

  // ==================== CronJob Tests ====================

  @Test
  public void testListCronJobs() throws Exception {
    CronJob cronJob = createTestCronJob("test-cronjob");

    var batchApi = mock(BatchAPIGroupDSL.class);
    var v1Api = mock(V1BatchAPIGroupDSL.class);
    MixedOperation cronJobsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    var cronJobList = new io.fabric8.kubernetes.api.model.batch.v1.CronJobList();
    cronJobList.setItems(List.of(cronJob));

    when(kubernetesClient.batch()).thenReturn(batchApi);
    when(batchApi.v1()).thenReturn(v1Api);
    when(v1Api.cronjobs()).thenReturn(cronJobsOp);
    when(cronJobsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.list()).thenReturn(cronJobList);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/cronjobs").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1));
  }

  @Test
  public void testTriggerCronJobBadRequest() throws Exception {
    CronJobTriggerRequest request = CronJobTriggerRequest.builder().build();

    mockMvc
        .perform(
            MockMvcRequestBuilders.post(BASE_PATH + "/cronjobs/test/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.error").exists());
  }

  // ==================== Job Tests ====================

  @Test
  public void testListJobs() throws Exception {
    Job job = createTestJob("test-job");

    var batchApi = mock(BatchAPIGroupDSL.class);
    var v1Api = mock(V1BatchAPIGroupDSL.class);
    MixedOperation jobsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    var jobList = new io.fabric8.kubernetes.api.model.batch.v1.JobList();
    jobList.setItems(List.of(job));

    when(kubernetesClient.batch()).thenReturn(batchApi);
    when(batchApi.v1()).thenReturn(v1Api);
    when(v1Api.jobs()).thenReturn(jobsOp);
    when(jobsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.list()).thenReturn(jobList);

    mockMvc
        .perform(MockMvcRequestBuilders.get(BASE_PATH + "/jobs").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1));
  }

  @Test
  public void testGetJob() throws Exception {
    Job job = createTestJob("my-job");

    var batchApi = mock(BatchAPIGroupDSL.class);
    var v1Api = mock(V1BatchAPIGroupDSL.class);
    MixedOperation jobsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    ScalableResource jobResource = mock(ScalableResource.class);

    when(kubernetesClient.batch()).thenReturn(batchApi);
    when(batchApi.v1()).thenReturn(v1Api);
    when(v1Api.jobs()).thenReturn(jobsOp);
    when(jobsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("my-job")).thenReturn(jobResource);
    when(jobResource.get()).thenReturn(job);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/jobs/my-job")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.metadata.name").value("my-job"));
  }

  @Test
  public void testGetJobNotFound() throws Exception {
    var batchApi = mock(BatchAPIGroupDSL.class);
    var v1Api = mock(V1BatchAPIGroupDSL.class);
    MixedOperation jobsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    ScalableResource jobResource = mock(ScalableResource.class);

    when(kubernetesClient.batch()).thenReturn(batchApi);
    when(batchApi.v1()).thenReturn(v1Api);
    when(v1Api.jobs()).thenReturn(jobsOp);
    when(jobsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("nonexistent")).thenReturn(jobResource);
    when(jobResource.get()).thenReturn(null);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/jobs/nonexistent")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testDeleteJob() throws Exception {
    Job job = createTestJob("to-delete");

    var batchApi = mock(BatchAPIGroupDSL.class);
    var v1Api = mock(V1BatchAPIGroupDSL.class);
    MixedOperation jobsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    ScalableResource jobResource = mock(ScalableResource.class);

    when(kubernetesClient.batch()).thenReturn(batchApi);
    when(batchApi.v1()).thenReturn(v1Api);
    when(v1Api.jobs()).thenReturn(jobsOp);
    when(jobsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("to-delete")).thenReturn(jobResource);
    when(jobResource.get()).thenReturn(job);

    mockMvc
        .perform(
            MockMvcRequestBuilders.delete(BASE_PATH + "/jobs/to-delete")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.message").value("Job deleted: to-delete"));
  }

  // ==================== Get Single Resource Tests ====================

  @Test
  public void testGetDeployment() throws Exception {
    Deployment deployment = createTestDeployment("my-deployment");

    MixedOperation deploymentsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    RollableScalableResource deploymentResource = mock(RollableScalableResource.class);

    when(kubernetesClient.apps()).thenReturn(mock(AppsAPIGroupDSL.class));
    when(kubernetesClient.apps().deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("my-deployment")).thenReturn(deploymentResource);
    when(deploymentResource.get()).thenReturn(deployment);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/deployments/my-deployment")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.metadata.name").value("my-deployment"));
  }

  @Test
  public void testGetDeploymentNotFound() throws Exception {
    MixedOperation deploymentsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    RollableScalableResource deploymentResource = mock(RollableScalableResource.class);

    when(kubernetesClient.apps()).thenReturn(mock(AppsAPIGroupDSL.class));
    when(kubernetesClient.apps().deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("nonexistent")).thenReturn(deploymentResource);
    when(deploymentResource.get()).thenReturn(null);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/deployments/nonexistent")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testGetPod() throws Exception {
    Pod pod = createTestPod("my-pod");

    MixedOperation podsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    PodResource podResource = mock(PodResource.class);

    when(kubernetesClient.pods()).thenReturn(podsOp);
    when(podsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("my-pod")).thenReturn(podResource);
    when(podResource.get()).thenReturn(pod);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/pods/my-pod")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.metadata.name").value("my-pod"));
  }

  @Test
  public void testGetConfigMap() throws Exception {
    ConfigMap configMap = createTestConfigMap("my-config");

    MixedOperation configMapsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    Resource configMapResource = mock(Resource.class);

    when(kubernetesClient.configMaps()).thenReturn(configMapsOp);
    when(configMapsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("my-config")).thenReturn(configMapResource);
    when(configMapResource.get()).thenReturn(configMap);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/configmaps/my-config")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.metadata.name").value("my-config"));
  }

  @Test
  public void testGetCronJob() throws Exception {
    CronJob cronJob = createTestCronJob("my-cronjob");

    var batchApi = mock(BatchAPIGroupDSL.class);
    var v1Api = mock(V1BatchAPIGroupDSL.class);
    MixedOperation cronJobsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    Resource cronJobResource = mock(Resource.class);

    when(kubernetesClient.batch()).thenReturn(batchApi);
    when(batchApi.v1()).thenReturn(v1Api);
    when(v1Api.cronjobs()).thenReturn(cronJobsOp);
    when(cronJobsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("my-cronjob")).thenReturn(cronJobResource);
    when(cronJobResource.get()).thenReturn(cronJob);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/cronjobs/my-cronjob")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.metadata.name").value("my-cronjob"));
  }

  // ==================== Mutation Success Tests ====================

  @Test
  public void testScaleDeploymentNegativeReplicas() throws Exception {
    DeploymentScaleRequest request = DeploymentScaleRequest.builder().replicas(-1).build();

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/deployments/test/scale")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.error").value("Replicas must be non-negative"));
  }

  @Test
  public void testScaleDeploymentNotFound() throws Exception {
    DeploymentScaleRequest request = DeploymentScaleRequest.builder().replicas(3).build();

    MixedOperation deploymentsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    RollableScalableResource deploymentResource = mock(RollableScalableResource.class);

    when(kubernetesClient.apps()).thenReturn(mock(AppsAPIGroupDSL.class));
    when(kubernetesClient.apps().deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("nonexistent")).thenReturn(deploymentResource);
    when(deploymentResource.get()).thenReturn(null);

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/deployments/nonexistent/scale")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testDeletePodSuccess() throws Exception {
    Pod pod = createTestPod("pod-to-delete");

    MixedOperation podsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    PodResource podResource = mock(PodResource.class);

    when(kubernetesClient.pods()).thenReturn(podsOp);
    when(podsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("pod-to-delete")).thenReturn(podResource);
    when(podResource.get()).thenReturn(pod);

    mockMvc
        .perform(
            MockMvcRequestBuilders.delete(BASE_PATH + "/pods/pod-to-delete")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.message").value("Pod deleted: pod-to-delete"));
  }

  @Test
  public void testTriggerCronJobNotFound() throws Exception {
    CronJobTriggerRequest request =
        CronJobTriggerRequest.builder().jobName("manual-job-123").build();

    var batchApi = mock(BatchAPIGroupDSL.class);
    var v1Api = mock(V1BatchAPIGroupDSL.class);
    MixedOperation cronJobsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    Resource cronJobResource = mock(Resource.class);

    when(kubernetesClient.batch()).thenReturn(batchApi);
    when(batchApi.v1()).thenReturn(v1Api);
    when(v1Api.cronjobs()).thenReturn(cronJobsOp);
    when(cronJobsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("nonexistent")).thenReturn(cronJobResource);
    when(cronJobResource.get()).thenReturn(null);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post(BASE_PATH + "/cronjobs/nonexistent/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testUpdateConfigMapNotFound() throws Exception {
    ConfigMapUpdateRequest request =
        ConfigMapUpdateRequest.builder().set(Map.of("key", "value")).build();

    MixedOperation configMapsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    Resource configMapResource = mock(Resource.class);

    when(kubernetesClient.configMaps()).thenReturn(configMapsOp);
    when(configMapsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("nonexistent")).thenReturn(configMapResource);
    when(configMapResource.get()).thenReturn(null);

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/configmaps/nonexistent")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testUpdateDeploymentEnvNotFound() throws Exception {
    DeploymentEnvUpdateRequest request =
        DeploymentEnvUpdateRequest.builder().set(Map.of("VAR", "value")).build();

    MixedOperation deploymentsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    RollableScalableResource deploymentResource = mock(RollableScalableResource.class);

    when(kubernetesClient.apps()).thenReturn(mock(AppsAPIGroupDSL.class));
    when(kubernetesClient.apps().deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("nonexistent")).thenReturn(deploymentResource);
    when(deploymentResource.get()).thenReturn(null);

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/deployments/nonexistent/env")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  // ==================== Pod Logs Tests ====================

  @Test
  public void testGetPodLogs() throws Exception {
    Pod pod = createTestPod("log-pod");

    MixedOperation podsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    PodResource podResource = mock(PodResource.class);
    ContainerResource containerResource = mock(ContainerResource.class);

    when(kubernetesClient.pods()).thenReturn(podsOp);
    when(podsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("log-pod")).thenReturn(podResource);
    when(podResource.get()).thenReturn(pod);
    when(podResource.inContainer("main")).thenReturn(containerResource);
    when(containerResource.getLog()).thenReturn("Log line 1\nLog line 2\n");

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/pods/log-pod/logs")
                .accept(MediaType.TEXT_PLAIN))
        .andExpect(status().isOk())
        .andExpect(content().string("Log line 1\nLog line 2\n"));
  }

  @Test
  public void testGetPodLogsNotFound() throws Exception {
    MixedOperation podsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    PodResource podResource = mock(PodResource.class);

    when(kubernetesClient.pods()).thenReturn(podsOp);
    when(podsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("nonexistent")).thenReturn(podResource);
    when(podResource.get()).thenReturn(null);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/pods/nonexistent/logs")
                .accept(MediaType.TEXT_PLAIN))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testGetPodLogsWithContainer() throws Exception {
    Pod pod = createTestPod("log-pod");

    MixedOperation podsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    PodResource podResource = mock(PodResource.class);
    ContainerResource containerResource = mock(ContainerResource.class);

    when(kubernetesClient.pods()).thenReturn(podsOp);
    when(podsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("log-pod")).thenReturn(podResource);
    when(podResource.get()).thenReturn(pod);
    when(podResource.inContainer("main")).thenReturn(containerResource);
    when(containerResource.getLog()).thenReturn("Container logs");

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/pods/log-pod/logs")
                .param("container", "main")
                .accept(MediaType.TEXT_PLAIN))
        .andExpect(status().isOk())
        .andExpect(content().string("Container logs"));
  }

  // ==================== Trigger CronJob Success Tests ====================

  @Test
  public void testTriggerCronJobSuccess() throws Exception {
    CronJob cronJob = createTestCronJob("my-cronjob");
    Job createdJob = createTestJob("manual-job-001");

    CronJobTriggerRequest request =
        CronJobTriggerRequest.builder().jobName("manual-job-001").build();

    var batchApi = mock(BatchAPIGroupDSL.class);
    var v1Api = mock(V1BatchAPIGroupDSL.class);
    MixedOperation cronJobsOp = mock(MixedOperation.class);
    MixedOperation jobsOp = mock(MixedOperation.class);
    NonNamespaceOperation cronJobNsOp = mock(NonNamespaceOperation.class);
    NonNamespaceOperation jobsNsOp = mock(NonNamespaceOperation.class);
    Resource cronJobResource = mock(Resource.class);

    when(kubernetesClient.batch()).thenReturn(batchApi);
    when(batchApi.v1()).thenReturn(v1Api);
    when(v1Api.cronjobs()).thenReturn(cronJobsOp);
    when(v1Api.jobs()).thenReturn(jobsOp);
    when(cronJobsOp.inNamespace(NAMESPACE)).thenReturn(cronJobNsOp);
    when(jobsOp.inNamespace(NAMESPACE)).thenReturn(jobsNsOp);
    when(cronJobNsOp.withName("my-cronjob")).thenReturn(cronJobResource);
    when(cronJobResource.get()).thenReturn(cronJob);
    when(jobsNsOp.create(any(Job.class))).thenReturn(createdJob);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post(BASE_PATH + "/cronjobs/my-cronjob/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.metadata.name").value("manual-job-001"));
  }

  // ==================== Scale Deployment Success Tests ====================

  @Test
  public void testScaleDeploymentSuccess() throws Exception {
    Deployment deployment = createTestDeployment("scalable");
    DeploymentScaleRequest request = DeploymentScaleRequest.builder().replicas(3).build();

    MixedOperation deploymentsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    RollableScalableResource deploymentResource = mock(RollableScalableResource.class);

    when(kubernetesClient.apps()).thenReturn(mock(AppsAPIGroupDSL.class));
    when(kubernetesClient.apps().deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("scalable")).thenReturn(deploymentResource);
    when(deploymentResource.get()).thenReturn(deployment);

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/deployments/scalable/scale")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    verify(deploymentResource).scale(3);
  }

  @Test
  public void testScaleDeploymentWithResources() throws Exception {
    Deployment deployment = createTestDeployment("scalable");

    ResourceRequirements resources =
        new ResourceRequirementsBuilder()
            .addToLimits("cpu", new Quantity("2"))
            .addToLimits("memory", new Quantity("4Gi"))
            .build();

    DeploymentScaleRequest request =
        DeploymentScaleRequest.builder().resources(resources).containerName("main").build();

    MixedOperation deploymentsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    RollableScalableResource deploymentResource = mock(RollableScalableResource.class);

    when(kubernetesClient.apps()).thenReturn(mock(AppsAPIGroupDSL.class));
    when(kubernetesClient.apps().deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("scalable")).thenReturn(deploymentResource);
    when(deploymentResource.get()).thenReturn(deployment);
    when(deploymentResource.edit(any(java.util.function.UnaryOperator.class)))
        .thenReturn(deployment);

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/deployments/scalable/scale")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    verify(deploymentResource).edit(any(java.util.function.UnaryOperator.class));
  }

  @Test
  public void testScaleDeploymentWithReplicasAndResources() throws Exception {
    Deployment deployment = createTestDeployment("scalable");

    ResourceRequirements resources =
        new ResourceRequirementsBuilder().addToRequests("cpu", new Quantity("500m")).build();

    DeploymentScaleRequest request =
        DeploymentScaleRequest.builder().replicas(5).resources(resources).build();

    MixedOperation deploymentsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    RollableScalableResource deploymentResource = mock(RollableScalableResource.class);

    when(kubernetesClient.apps()).thenReturn(mock(AppsAPIGroupDSL.class));
    when(kubernetesClient.apps().deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("scalable")).thenReturn(deploymentResource);
    when(deploymentResource.get()).thenReturn(deployment);
    when(deploymentResource.edit(any(java.util.function.UnaryOperator.class)))
        .thenReturn(deployment);

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/deployments/scalable/scale")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    verify(deploymentResource).scale(5);
    verify(deploymentResource).edit(any(java.util.function.UnaryOperator.class));
  }

  // ==================== Update ConfigMap Success Tests ====================

  @Test
  public void testUpdateConfigMapSuccess() throws Exception {
    ConfigMap configMap = createTestConfigMap("my-config");
    ConfigMapUpdateRequest request =
        ConfigMapUpdateRequest.builder().set(Map.of("new-key", "new-value")).build();

    MixedOperation configMapsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    Resource configMapResource = mock(Resource.class);

    when(kubernetesClient.configMaps()).thenReturn(configMapsOp);
    when(configMapsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("my-config")).thenReturn(configMapResource);
    when(configMapResource.get()).thenReturn(configMap);
    when(configMapResource.edit(any(java.util.function.UnaryOperator.class))).thenReturn(configMap);

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/configmaps/my-config")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    verify(configMapResource).edit(any(java.util.function.UnaryOperator.class));
  }

  @Test
  public void testUpdateConfigMapWithRemove() throws Exception {
    ConfigMap configMap = createTestConfigMap("my-config");
    ConfigMapUpdateRequest request =
        ConfigMapUpdateRequest.builder().remove(List.of("old-key")).build();

    MixedOperation configMapsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    Resource configMapResource = mock(Resource.class);

    when(kubernetesClient.configMaps()).thenReturn(configMapsOp);
    when(configMapsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("my-config")).thenReturn(configMapResource);
    when(configMapResource.get()).thenReturn(configMap);
    when(configMapResource.edit(any(java.util.function.UnaryOperator.class))).thenReturn(configMap);

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/configmaps/my-config")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
  }

  // ==================== Update Deployment Env Success Tests ====================

  @Test
  public void testUpdateDeploymentEnvSuccess() throws Exception {
    Deployment deployment = createTestDeployment("env-deployment");
    DeploymentEnvUpdateRequest request =
        DeploymentEnvUpdateRequest.builder().set(Map.of("NEW_VAR", "value")).build();

    MixedOperation deploymentsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    RollableScalableResource deploymentResource = mock(RollableScalableResource.class);

    when(kubernetesClient.apps()).thenReturn(mock(AppsAPIGroupDSL.class));
    when(kubernetesClient.apps().deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("env-deployment")).thenReturn(deploymentResource);
    when(deploymentResource.get()).thenReturn(deployment);
    when(deploymentResource.edit(any(java.util.function.UnaryOperator.class)))
        .thenReturn(deployment);

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/deployments/env-deployment/env")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
  }

  @Test
  public void testUpdateDeploymentEnvWithRemove() throws Exception {
    Deployment deployment = createTestDeployment("env-deployment");
    DeploymentEnvUpdateRequest request =
        DeploymentEnvUpdateRequest.builder().remove(List.of("OLD_VAR")).build();

    MixedOperation deploymentsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    RollableScalableResource deploymentResource = mock(RollableScalableResource.class);

    when(kubernetesClient.apps()).thenReturn(mock(AppsAPIGroupDSL.class));
    when(kubernetesClient.apps().deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("env-deployment")).thenReturn(deploymentResource);
    when(deploymentResource.get()).thenReturn(deployment);
    when(deploymentResource.edit(any(java.util.function.UnaryOperator.class)))
        .thenReturn(deployment);

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/deployments/env-deployment/env")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
  }

  // ==================== Trigger CronJob with Overrides Tests ====================

  @Test
  public void testTriggerCronJobWithOverrides() throws Exception {
    CronJob cronJob = createTestCronJob("my-cronjob");
    Job createdJob = createTestJob("override-job");

    CronJobTriggerRequest request =
        CronJobTriggerRequest.builder()
            .jobName("override-job")
            .command(List.of("/bin/sh", "-c"))
            .args(List.of("echo test"))
            .build();

    var batchApi = mock(BatchAPIGroupDSL.class);
    var v1Api = mock(V1BatchAPIGroupDSL.class);
    MixedOperation cronJobsOp = mock(MixedOperation.class);
    MixedOperation jobsOp = mock(MixedOperation.class);
    NonNamespaceOperation cronJobNsOp = mock(NonNamespaceOperation.class);
    NonNamespaceOperation jobsNsOp = mock(NonNamespaceOperation.class);
    Resource cronJobResource = mock(Resource.class);

    when(kubernetesClient.batch()).thenReturn(batchApi);
    when(batchApi.v1()).thenReturn(v1Api);
    when(v1Api.cronjobs()).thenReturn(cronJobsOp);
    when(v1Api.jobs()).thenReturn(jobsOp);
    when(cronJobsOp.inNamespace(NAMESPACE)).thenReturn(cronJobNsOp);
    when(jobsOp.inNamespace(NAMESPACE)).thenReturn(jobsNsOp);
    when(cronJobNsOp.withName("my-cronjob")).thenReturn(cronJobResource);
    when(cronJobResource.get()).thenReturn(cronJob);
    when(jobsNsOp.create(any(Job.class))).thenReturn(createdJob);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post(BASE_PATH + "/cronjobs/my-cronjob/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated());
  }

  // ==================== Delete Job Not Found Test ====================

  @Test
  public void testDeleteJobNotFound() throws Exception {
    var batchApi = mock(BatchAPIGroupDSL.class);
    var v1Api = mock(V1BatchAPIGroupDSL.class);
    MixedOperation jobsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    ScalableResource jobResource = mock(ScalableResource.class);

    when(kubernetesClient.batch()).thenReturn(batchApi);
    when(batchApi.v1()).thenReturn(v1Api);
    when(v1Api.jobs()).thenReturn(jobsOp);
    when(jobsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("nonexistent")).thenReturn(jobResource);
    when(jobResource.get()).thenReturn(null);

    mockMvc
        .perform(
            MockMvcRequestBuilders.delete(BASE_PATH + "/jobs/nonexistent")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  // ==================== Get CronJob Not Found Test ====================

  @Test
  public void testGetCronJobNotFound() throws Exception {
    var batchApi = mock(BatchAPIGroupDSL.class);
    var v1Api = mock(V1BatchAPIGroupDSL.class);
    MixedOperation cronJobsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    Resource cronJobResource = mock(Resource.class);

    when(kubernetesClient.batch()).thenReturn(batchApi);
    when(batchApi.v1()).thenReturn(v1Api);
    when(v1Api.cronjobs()).thenReturn(cronJobsOp);
    when(cronJobsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("nonexistent")).thenReturn(cronJobResource);
    when(cronJobResource.get()).thenReturn(null);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/cronjobs/nonexistent")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  // ==================== Get ConfigMap Not Found Test ====================

  @Test
  public void testGetConfigMapNotFound() throws Exception {
    MixedOperation configMapsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    Resource configMapResource = mock(Resource.class);

    when(kubernetesClient.configMaps()).thenReturn(configMapsOp);
    when(configMapsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("nonexistent")).thenReturn(configMapResource);
    when(configMapResource.get()).thenReturn(null);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/configmaps/nonexistent")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  // ==================== Get Pod Not Found Test ====================

  @Test
  public void testGetPodNotFound() throws Exception {
    MixedOperation podsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    PodResource podResource = mock(PodResource.class);

    when(kubernetesClient.pods()).thenReturn(podsOp);
    when(podsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("nonexistent")).thenReturn(podResource);
    when(podResource.get()).thenReturn(null);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/pods/nonexistent")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  // ==================== Pagination Tests ====================

  @Test
  public void testListDeploymentsWithPagination() throws Exception {
    Deployment d1 = createTestDeployment("deploy-1");
    Deployment d2 = createTestDeployment("deploy-2");
    Deployment d3 = createTestDeployment("deploy-3");

    MixedOperation deploymentsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    DeploymentList deploymentList = new DeploymentList();
    deploymentList.setItems(List.of(d1, d2, d3));

    when(kubernetesClient.apps()).thenReturn(mock(AppsAPIGroupDSL.class));
    when(kubernetesClient.apps().deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.list()).thenReturn(deploymentList);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/deployments")
                .param("start", "1")
                .param("count", "1")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(3))
        .andExpect(jsonPath("$.start").value(1))
        .andExpect(jsonPath("$.count").value(1))
        .andExpect(jsonPath("$.elements[0].metadata.name").value("deploy-2"));
  }

  // ==================== K8s Unavailable Tests ====================

  @Test
  public void testK8sUnavailableReturnsServiceUnavailable() throws Exception {
    KubernetesController nullClientController =
        new KubernetesController(
            systemOperationContext,
            authorizerChain,
            null,
            KubernetesClientFactory.getObjectMapper());

    jakarta.servlet.http.HttpServletRequest mockRequest =
        mock(jakarta.servlet.http.HttpServletRequest.class);
    when(mockRequest.getRemoteAddr()).thenReturn("127.0.0.1");
    when(mockRequest.getMethod()).thenReturn("GET");
    when(mockRequest.getRequestURI()).thenReturn("/openapi/operations/k8/status");

    var response = nullClientController.getStatus(mockRequest);
    assertEquals(response.getStatusCodeValue(), 200);
    assertTrue(response.getBody().toString().contains("available=false"));
  }

  @Test
  public void testListDeploymentsK8sUnavailable() throws Exception {
    KubernetesController nullClientController =
        new KubernetesController(
            systemOperationContext,
            authorizerChain,
            null,
            KubernetesClientFactory.getObjectMapper());

    jakarta.servlet.http.HttpServletRequest mockRequest =
        mock(jakarta.servlet.http.HttpServletRequest.class);
    when(mockRequest.getRemoteAddr()).thenReturn("127.0.0.1");
    when(mockRequest.getMethod()).thenReturn("GET");
    when(mockRequest.getRequestURI()).thenReturn("/openapi/operations/k8/deployments");

    var response = nullClientController.listDeployments(mockRequest, 0, 10);
    assertEquals(response.getStatusCodeValue(), 503);
  }

  // ==================== Empty Containers Tests ====================

  @Test
  public void testScaleDeploymentEmptyContainers() throws Exception {
    Deployment deployment =
        new DeploymentBuilder()
            .withNewMetadata()
            .withName("empty-containers")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .withNewTemplate()
            .withNewSpec()
            .withContainers()
            .endSpec()
            .endTemplate()
            .endSpec()
            .build();

    ResourceRequirements resources =
        new ResourceRequirementsBuilder().addToLimits("cpu", new Quantity("1")).build();

    DeploymentScaleRequest request = DeploymentScaleRequest.builder().resources(resources).build();

    MixedOperation deploymentsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    RollableScalableResource deploymentResource = mock(RollableScalableResource.class);

    when(kubernetesClient.apps()).thenReturn(mock(AppsAPIGroupDSL.class));
    when(kubernetesClient.apps().deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("empty-containers")).thenReturn(deploymentResource);
    when(deploymentResource.get()).thenReturn(deployment);

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/deployments/empty-containers/scale")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testUpdateDeploymentEnvEmptyContainers() throws Exception {
    Deployment deployment =
        new DeploymentBuilder()
            .withNewMetadata()
            .withName("empty-containers")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .withNewTemplate()
            .withNewSpec()
            .withContainers()
            .endSpec()
            .endTemplate()
            .endSpec()
            .build();

    DeploymentEnvUpdateRequest request =
        DeploymentEnvUpdateRequest.builder().set(Map.of("VAR", "value")).build();

    MixedOperation deploymentsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    RollableScalableResource deploymentResource = mock(RollableScalableResource.class);

    when(kubernetesClient.apps()).thenReturn(mock(AppsAPIGroupDSL.class));
    when(kubernetesClient.apps().deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("empty-containers")).thenReturn(deploymentResource);
    when(deploymentResource.get()).thenReturn(deployment);

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/deployments/empty-containers/env")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  // ==================== Pod Logs Tests ====================

  @Test
  public void testGetPodLogsSuccess() throws Exception {
    Pod pod = createTestPod("log-pod");
    MixedOperation podsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    PodResource podResource = mock(PodResource.class);
    ContainerResource containerResource = mock(ContainerResource.class);

    when(kubernetesClient.pods()).thenReturn(podsOp);
    when(podsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("log-pod")).thenReturn(podResource);
    when(podResource.get()).thenReturn(pod);
    when(podResource.inContainer("main")).thenReturn(containerResource);
    when(containerResource.getLog()).thenReturn("Test log content");

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/pods/log-pod/logs")
                .accept(MediaType.TEXT_PLAIN))
        .andExpect(status().isOk())
        .andExpect(content().string("Test log content"));
  }

  @Test
  public void testGetPodLogsTruncation() throws Exception {
    Pod pod = createTestPod("log-pod");
    MixedOperation podsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    PodResource podResource = mock(PodResource.class);
    ContainerResource containerResource = mock(ContainerResource.class);

    // Create logs larger than the minimum limit (1024 bytes)
    String largeLogs = "x".repeat(5000);

    when(kubernetesClient.pods()).thenReturn(podsOp);
    when(podsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("log-pod")).thenReturn(podResource);
    when(podResource.get()).thenReturn(pod);
    when(podResource.inContainer("main")).thenReturn(containerResource);
    when(containerResource.getLog()).thenReturn(largeLogs);

    // Use limitBytes parameter with value higher than min (1024) but lower than log size
    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/pods/log-pod/logs")
                .param("limitBytes", "2000")
                .accept(MediaType.TEXT_PLAIN))
        .andExpect(status().isOk())
        .andExpect(content().string(org.hamcrest.Matchers.containsString("[truncated...]")));
  }

  @Test
  public void testGetPodLogsWithSinceSeconds() throws Exception {
    Pod pod = createTestPod("log-pod");
    MixedOperation podsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    PodResource podResource = mock(PodResource.class);
    ContainerResource containerResource = mock(ContainerResource.class, RETURNS_DEEP_STUBS);

    when(kubernetesClient.pods()).thenReturn(podsOp);
    when(podsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("log-pod")).thenReturn(podResource);
    when(podResource.get()).thenReturn(pod);
    when(podResource.inContainer("main")).thenReturn(containerResource);
    when(containerResource.sinceSeconds(300).getLog()).thenReturn("Recent logs");

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/pods/log-pod/logs")
                .param("sinceSeconds", "300")
                .accept(MediaType.TEXT_PLAIN))
        .andExpect(status().isOk())
        .andExpect(content().string("Recent logs"));
  }

  @Test
  public void testGetPodLogsEmptyContainers() throws Exception {
    Pod pod =
        new PodBuilder()
            .withNewMetadata()
            .withName("empty-pod")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .withContainers()
            .endSpec()
            .build();

    MixedOperation podsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    PodResource podResource = mock(PodResource.class);

    when(kubernetesClient.pods()).thenReturn(podsOp);
    when(podsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("empty-pod")).thenReturn(podResource);
    when(podResource.get()).thenReturn(pod);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/pods/empty-pod/logs")
                .accept(MediaType.TEXT_PLAIN))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testGetPodLogsException() throws Exception {
    Pod pod = createTestPod("log-pod");
    MixedOperation podsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    PodResource podResource = mock(PodResource.class);
    ContainerResource containerResource = mock(ContainerResource.class);

    when(kubernetesClient.pods()).thenReturn(podsOp);
    when(podsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("log-pod")).thenReturn(podResource);
    when(podResource.get()).thenReturn(pod);
    when(podResource.inContainer("main")).thenReturn(containerResource);
    when(containerResource.getLog()).thenThrow(new RuntimeException("Connection refused"));

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/pods/log-pod/logs")
                .accept(MediaType.TEXT_PLAIN))
        .andExpect(status().isInternalServerError())
        .andExpect(content().string(org.hamcrest.Matchers.containsString("Connection refused")));
  }

  @Test
  public void testGetPodLogsNullLogs() throws Exception {
    Pod pod = createTestPod("log-pod");
    MixedOperation podsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    PodResource podResource = mock(PodResource.class);
    ContainerResource containerResource = mock(ContainerResource.class);

    when(kubernetesClient.pods()).thenReturn(podsOp);
    when(podsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("log-pod")).thenReturn(podResource);
    when(podResource.get()).thenReturn(pod);
    when(podResource.inContainer("main")).thenReturn(containerResource);
    when(containerResource.getLog()).thenReturn(null);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/pods/log-pod/logs")
                .accept(MediaType.TEXT_PLAIN))
        .andExpect(status().isOk())
        .andExpect(content().string(""));
  }

  // ==================== ConfigMap Update Edge Cases ====================

  @Test
  public void testUpdateConfigMapWithNullData() throws Exception {
    ConfigMap configMap =
        new ConfigMapBuilder()
            .withNewMetadata()
            .withName("null-data-config")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .build();

    ConfigMapUpdateRequest request =
        ConfigMapUpdateRequest.builder().set(Map.of("new-key", "new-value")).build();

    MixedOperation configMapsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    Resource configMapResource = mock(Resource.class);

    when(kubernetesClient.configMaps()).thenReturn(configMapsOp);
    when(configMapsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("null-data-config")).thenReturn(configMapResource);
    when(configMapResource.get()).thenReturn(configMap);
    when(configMapResource.edit(any(java.util.function.UnaryOperator.class))).thenReturn(configMap);

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/configmaps/null-data-config")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    verify(configMapResource).edit(any(java.util.function.UnaryOperator.class));
  }

  // ==================== Deployment Env Update Edge Cases ====================

  @Test
  public void testUpdateDeploymentEnvWithNullExistingEnv() throws Exception {
    Deployment deployment =
        new DeploymentBuilder()
            .withNewMetadata()
            .withName("null-env-deployment")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .withNewTemplate()
            .withNewSpec()
            .addNewContainer()
            .withName("main")
            .withImage("nginx:latest")
            .endContainer()
            .endSpec()
            .endTemplate()
            .endSpec()
            .build();

    DeploymentEnvUpdateRequest request =
        DeploymentEnvUpdateRequest.builder().set(Map.of("NEW_VAR", "value")).build();

    MixedOperation deploymentsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    RollableScalableResource deploymentResource = mock(RollableScalableResource.class);

    when(kubernetesClient.apps()).thenReturn(mock(AppsAPIGroupDSL.class));
    when(kubernetesClient.apps().deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("null-env-deployment")).thenReturn(deploymentResource);
    when(deploymentResource.get()).thenReturn(deployment);
    when(deploymentResource.edit(any(java.util.function.UnaryOperator.class)))
        .thenReturn(deployment);

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/deployments/null-env-deployment/env")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());
  }

  @Test
  public void testUpdateDeploymentEnvRemoveAndSet() throws Exception {
    Deployment deployment = createTestDeployment("env-deployment");
    deployment
        .getSpec()
        .getTemplate()
        .getSpec()
        .getContainers()
        .get(0)
        .setEnv(List.of(new EnvVar("OLD_VAR", "old", null), new EnvVar("KEEP_VAR", "keep", null)));

    DeploymentEnvUpdateRequest request =
        DeploymentEnvUpdateRequest.builder()
            .set(Map.of("NEW_VAR", "new"))
            .remove(List.of("OLD_VAR"))
            .build();

    MixedOperation deploymentsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    RollableScalableResource deploymentResource = mock(RollableScalableResource.class);

    when(kubernetesClient.apps()).thenReturn(mock(AppsAPIGroupDSL.class));
    when(kubernetesClient.apps().deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("env-deployment")).thenReturn(deploymentResource);
    when(deploymentResource.get()).thenReturn(deployment);
    when(deploymentResource.edit(any(java.util.function.UnaryOperator.class)))
        .thenReturn(deployment);

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/deployments/env-deployment/env")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    verify(deploymentResource).edit(any(java.util.function.UnaryOperator.class));
  }

  // ==================== Container Resolution Tests ====================

  @Test
  public void testScaleDeploymentWithSpecificContainer() throws Exception {
    Deployment deployment =
        new DeploymentBuilder()
            .withNewMetadata()
            .withName("multi-container")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .withNewTemplate()
            .withNewSpec()
            .addNewContainer()
            .withName("main")
            .withImage("nginx:latest")
            .endContainer()
            .addNewContainer()
            .withName("sidecar")
            .withImage("busybox:latest")
            .endContainer()
            .endSpec()
            .endTemplate()
            .endSpec()
            .build();

    ResourceRequirements resources =
        new ResourceRequirementsBuilder().addToLimits("cpu", new Quantity("2")).build();

    DeploymentScaleRequest request =
        DeploymentScaleRequest.builder().resources(resources).containerName("sidecar").build();

    MixedOperation deploymentsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    RollableScalableResource deploymentResource = mock(RollableScalableResource.class);

    when(kubernetesClient.apps()).thenReturn(mock(AppsAPIGroupDSL.class));
    when(kubernetesClient.apps().deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("multi-container")).thenReturn(deploymentResource);
    when(deploymentResource.get()).thenReturn(deployment);
    when(deploymentResource.edit(any(java.util.function.UnaryOperator.class)))
        .thenReturn(deployment);

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/deployments/multi-container/scale")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    verify(deploymentResource).edit(any(java.util.function.UnaryOperator.class));
  }

  @Test
  public void testScaleDeploymentContainerNotFound() throws Exception {
    Deployment deployment = createTestDeployment("my-deployment");

    ResourceRequirements resources =
        new ResourceRequirementsBuilder().addToLimits("cpu", new Quantity("1")).build();

    DeploymentScaleRequest request =
        DeploymentScaleRequest.builder()
            .resources(resources)
            .containerName("nonexistent-container")
            .build();

    MixedOperation deploymentsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    RollableScalableResource deploymentResource = mock(RollableScalableResource.class);

    when(kubernetesClient.apps()).thenReturn(mock(AppsAPIGroupDSL.class));
    when(kubernetesClient.apps().deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("my-deployment")).thenReturn(deploymentResource);
    when(deploymentResource.get()).thenReturn(deployment);

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/deployments/my-deployment/scale")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isNotFound());
  }

  @Test
  public void testMultiContainerDefaultsToFirstContainer() throws Exception {
    // When no containerName is specified, the controller defaults to the first container
    Deployment deployment =
        new DeploymentBuilder()
            .withNewMetadata()
            .withName("multi-container")
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .withNewTemplate()
            .withNewSpec()
            .addNewContainer()
            .withName("main")
            .withImage("nginx:latest")
            .endContainer()
            .addNewContainer()
            .withName("sidecar")
            .withImage("busybox:latest")
            .endContainer()
            .endSpec()
            .endTemplate()
            .endSpec()
            .build();

    ResourceRequirements resources =
        new ResourceRequirementsBuilder().addToLimits("cpu", new Quantity("1")).build();

    DeploymentScaleRequest request = DeploymentScaleRequest.builder().resources(resources).build();

    MixedOperation deploymentsOp = mock(MixedOperation.class);
    NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
    RollableScalableResource deploymentResource = mock(RollableScalableResource.class);

    when(kubernetesClient.apps()).thenReturn(mock(AppsAPIGroupDSL.class));
    when(kubernetesClient.apps().deployments()).thenReturn(deploymentsOp);
    when(deploymentsOp.inNamespace(NAMESPACE)).thenReturn(nsOp);
    when(nsOp.withName("multi-container")).thenReturn(deploymentResource);
    when(deploymentResource.get()).thenReturn(deployment);
    when(deploymentResource.edit(any(java.util.function.UnaryOperator.class)))
        .thenReturn(deployment);

    mockMvc
        .perform(
            MockMvcRequestBuilders.patch(BASE_PATH + "/deployments/multi-container/scale")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk());

    verify(deploymentResource).edit(any(java.util.function.UnaryOperator.class));
  }

  // ==================== Helper Methods ====================

  private Deployment createTestDeployment(String name) {
    return new DeploymentBuilder()
        .withNewMetadata()
        .withName(name)
        .withNamespace(NAMESPACE)
        .endMetadata()
        .withNewSpec()
        .withReplicas(1)
        .withNewTemplate()
        .withNewSpec()
        .addNewContainer()
        .withName("main")
        .withImage("test:latest")
        .endContainer()
        .endSpec()
        .endTemplate()
        .endSpec()
        .build();
  }

  private Pod createTestPod(String name) {
    return new PodBuilder()
        .withNewMetadata()
        .withName(name)
        .withNamespace(NAMESPACE)
        .endMetadata()
        .withNewSpec()
        .addNewContainer()
        .withName("main")
        .withImage("test:latest")
        .endContainer()
        .endSpec()
        .build();
  }

  private ConfigMap createTestConfigMap(String name) {
    return new ConfigMapBuilder()
        .withNewMetadata()
        .withName(name)
        .withNamespace(NAMESPACE)
        .endMetadata()
        .withData(Map.of("key", "value"))
        .build();
  }

  private CronJob createTestCronJob(String name) {
    return new CronJobBuilder()
        .withNewMetadata()
        .withName(name)
        .withNamespace(NAMESPACE)
        .endMetadata()
        .withNewSpec()
        .withSchedule("0 * * * *")
        .withNewJobTemplate()
        .withNewSpec()
        .withNewTemplate()
        .withNewSpec()
        .addNewContainer()
        .withName("main")
        .withImage("test:latest")
        .endContainer()
        .withRestartPolicy("Never")
        .endSpec()
        .endTemplate()
        .endSpec()
        .endJobTemplate()
        .endSpec()
        .build();
  }

  private Job createTestJob(String name) {
    return new JobBuilder()
        .withNewMetadata()
        .withName(name)
        .withNamespace(NAMESPACE)
        .endMetadata()
        .withNewSpec()
        .withNewTemplate()
        .withNewSpec()
        .addNewContainer()
        .withName("main")
        .withImage("test:latest")
        .endContainer()
        .withRestartPolicy("Never")
        .endSpec()
        .endTemplate()
        .endSpec()
        .build();
  }

  // ==================== Test Configuration ====================

  @SpringBootConfiguration
  @Import({TestK8sConfig.class, TracingInterceptor.class})
  @ComponentScan(basePackages = {"io.datahubproject.openapi.operations.k8"})
  static class TestConfig {}

  @TestConfiguration
  public static class TestK8sConfig {

    @Bean
    @Primary
    public KubernetesClient kubernetesClient() {
      return mock(KubernetesClient.class);
    }

    @Bean
    public ObjectMapper objectMapper() {
      return new ObjectMapper();
    }

    @Bean
    @Primary
    public SystemTelemetryContext systemTelemetryContext() {
      return mock(SystemTelemetryContext.class);
    }

    @Bean(name = "systemOperationContext")
    public OperationContext systemOperationContext(
        @Qualifier("objectMapper") ObjectMapper objectMapper,
        SystemTelemetryContext systemTelemetryContext) {
      return TestOperationContexts.systemContextTraceNoSearchAuthorization(
          () -> ObjectMapperContext.builder().objectMapper(objectMapper).build(),
          () -> systemTelemetryContext);
    }

    @Bean
    @Primary
    public AuthorizerChain authorizerChain() {
      AuthorizerChain authorizerChain = mock(AuthorizerChain.class);
      Authentication authentication = mock(Authentication.class);
      when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
      doReturn(
              new BatchAuthorizationResult(
                  mock(BatchAuthorizationRequest.class),
                  new ConstantAuthorizationResultMap(AuthorizationResult.Type.ALLOW)))
          .when(authorizerChain)
          .authorizeBatch(any(BatchAuthorizationRequest.class));
      AuthenticationContext.setAuthentication(authentication);
      return authorizerChain;
    }

    @Bean(name = "kubernetesObjectMapper")
    public ObjectMapper kubernetesObjectMapper() {
      return KubernetesClientFactory.getObjectMapper();
    }
  }
}

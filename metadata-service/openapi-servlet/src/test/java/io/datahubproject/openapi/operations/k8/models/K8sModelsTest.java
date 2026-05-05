package io.datahubproject.openapi.operations.k8.models;

import static org.testng.Assert.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for Kubernetes operations DTOs. */
public class K8sModelsTest {

  private ObjectMapper objectMapper;

  @BeforeMethod
  public void setup() {
    objectMapper = new ObjectMapper();
  }

  // ==================== K8sStatusResponse Tests ====================

  @Test
  public void testK8sStatusResponseBuilder() {
    K8sStatusResponse response =
        K8sStatusResponse.builder().available(true).namespace("datahub").build();

    assertTrue(response.isAvailable());
    assertEquals(response.getNamespace(), "datahub");
    assertNull(response.getReason());
  }

  @Test
  public void testK8sStatusResponseWithReason() {
    K8sStatusResponse response =
        K8sStatusResponse.builder()
            .available(false)
            .reason("Not running in Kubernetes environment")
            .build();

    assertFalse(response.isAvailable());
    assertNull(response.getNamespace());
    assertEquals(response.getReason(), "Not running in Kubernetes environment");
  }

  @Test
  public void testK8sStatusResponseSerialization() throws JsonProcessingException {
    K8sStatusResponse response =
        K8sStatusResponse.builder().available(true).namespace("datahub").build();

    String json = objectMapper.writeValueAsString(response);

    assertTrue(json.contains("\"available\":true"));
    assertTrue(json.contains("\"namespace\":\"datahub\""));
    // Null fields should not be included
    assertFalse(json.contains("reason"));
  }

  // ==================== DeploymentScaleRequest Tests ====================

  @Test
  public void testDeploymentScaleRequestWithReplicas() {
    DeploymentScaleRequest request = DeploymentScaleRequest.builder().replicas(3).build();

    assertEquals(request.getReplicas(), Integer.valueOf(3));
    assertNull(request.getResources());
    assertNull(request.getContainerName());
  }

  @Test
  public void testDeploymentScaleRequestWithResources() {
    ResourceRequirements resources =
        new ResourceRequirementsBuilder()
            .addToLimits("cpu", new Quantity("2"))
            .addToLimits("memory", new Quantity("4Gi"))
            .addToRequests("cpu", new Quantity("1"))
            .addToRequests("memory", new Quantity("2Gi"))
            .build();

    DeploymentScaleRequest request =
        DeploymentScaleRequest.builder().containerName("main").resources(resources).build();

    assertNull(request.getReplicas());
    assertEquals(request.getContainerName(), "main");
    assertNotNull(request.getResources());
  }

  @Test
  public void testDeploymentScaleRequestDeserialization() throws JsonProcessingException {
    String json = "{\"replicas\":5,\"containerName\":\"web\"}";

    DeploymentScaleRequest request = objectMapper.readValue(json, DeploymentScaleRequest.class);

    assertEquals(request.getReplicas(), Integer.valueOf(5));
    assertEquals(request.getContainerName(), "web");
  }

  // ==================== DeploymentEnvUpdateRequest Tests ====================

  @Test
  public void testDeploymentEnvUpdateRequestWithSet() {
    DeploymentEnvUpdateRequest request =
        DeploymentEnvUpdateRequest.builder()
            .containerName("main")
            .set(Map.of("VAR1", "value1", "VAR2", "value2"))
            .build();

    assertEquals(request.getContainerName(), "main");
    assertEquals(request.getSet().size(), 2);
    assertEquals(request.getSet().get("VAR1"), "value1");
    assertNull(request.getRemove());
  }

  @Test
  public void testDeploymentEnvUpdateRequestWithRemove() {
    DeploymentEnvUpdateRequest request =
        DeploymentEnvUpdateRequest.builder().remove(List.of("OLD_VAR", "DEPRECATED_VAR")).build();

    assertNull(request.getContainerName());
    assertNull(request.getSet());
    assertEquals(request.getRemove().size(), 2);
    assertTrue(request.getRemove().contains("OLD_VAR"));
  }

  @Test
  public void testDeploymentEnvUpdateRequestSerialization() throws JsonProcessingException {
    DeploymentEnvUpdateRequest request =
        DeploymentEnvUpdateRequest.builder()
            .set(Map.of("KEY", "value"))
            .remove(List.of("OLD"))
            .build();

    String json = objectMapper.writeValueAsString(request);

    assertTrue(json.contains("\"set\""));
    assertTrue(json.contains("\"KEY\""));
    assertTrue(json.contains("\"remove\""));
    assertTrue(json.contains("\"OLD\""));
  }

  // ==================== ConfigMapUpdateRequest Tests ====================

  @Test
  public void testConfigMapUpdateRequestWithSet() {
    ConfigMapUpdateRequest request =
        ConfigMapUpdateRequest.builder().set(Map.of("config.yaml", "key: value")).build();

    assertEquals(request.getSet().get("config.yaml"), "key: value");
    assertNull(request.getRemove());
  }

  @Test
  public void testConfigMapUpdateRequestWithRemove() {
    ConfigMapUpdateRequest request =
        ConfigMapUpdateRequest.builder().remove(List.of("old-config.yaml")).build();

    assertNull(request.getSet());
    assertEquals(request.getRemove().size(), 1);
    assertTrue(request.getRemove().contains("old-config.yaml"));
  }

  @Test
  public void testConfigMapUpdateRequestDeserialization() throws JsonProcessingException {
    String json = "{\"set\":{\"app.properties\":\"debug=true\"},\"remove\":[\"deprecated.cfg\"]}";

    ConfigMapUpdateRequest request = objectMapper.readValue(json, ConfigMapUpdateRequest.class);

    assertEquals(request.getSet().get("app.properties"), "debug=true");
    assertTrue(request.getRemove().contains("deprecated.cfg"));
  }

  // ==================== CronJobTriggerRequest Tests ====================

  @Test
  public void testCronJobTriggerRequestMinimal() {
    CronJobTriggerRequest request =
        CronJobTriggerRequest.builder().jobName("my-manual-job").build();

    assertEquals(request.getJobName(), "my-manual-job");
    assertNull(request.getContainerName());
    assertNull(request.getCommand());
    assertNull(request.getArgs());
    assertNull(request.getResources());
  }

  @Test
  public void testCronJobTriggerRequestWithOverrides() {
    ResourceRequirements resources =
        new ResourceRequirementsBuilder()
            .addToLimits("cpu", new Quantity("4"))
            .addToLimits("memory", new Quantity("8Gi"))
            .build();

    CronJobTriggerRequest request =
        CronJobTriggerRequest.builder()
            .jobName("upgrade-job")
            .containerName("datahub-upgrade")
            .command(List.of("/bin/sh", "-c"))
            .args(List.of("datahub migrate --force"))
            .resources(resources)
            .build();

    assertEquals(request.getJobName(), "upgrade-job");
    assertEquals(request.getContainerName(), "datahub-upgrade");
    assertEquals(request.getCommand(), List.of("/bin/sh", "-c"));
    assertEquals(request.getArgs(), List.of("datahub migrate --force"));
    assertNotNull(request.getResources());
  }

  @Test
  public void testCronJobTriggerRequestDeserialization() throws JsonProcessingException {
    String json =
        "{\"jobName\":\"test-job\",\"containerName\":\"main\",\"command\":[\"/bin/bash\"],\"args\":[\"-c\",\"echo hello\"]}";

    CronJobTriggerRequest request = objectMapper.readValue(json, CronJobTriggerRequest.class);

    assertEquals(request.getJobName(), "test-job");
    assertEquals(request.getContainerName(), "main");
    assertEquals(request.getCommand(), List.of("/bin/bash"));
    assertEquals(request.getArgs(), List.of("-c", "echo hello"));
  }

  @Test
  public void testCronJobTriggerRequestSerialization() throws JsonProcessingException {
    CronJobTriggerRequest request =
        CronJobTriggerRequest.builder()
            .jobName("my-job")
            .command(List.of("echo"))
            .args(List.of("test"))
            .build();

    String json = objectMapper.writeValueAsString(request);

    assertTrue(json.contains("\"jobName\":\"my-job\""));
    assertTrue(json.contains("\"command\":[\"echo\"]"));
    assertTrue(json.contains("\"args\":[\"test\"]"));
    // Null fields should not be included due to @JsonInclude
    assertFalse(json.contains("containerName"));
    assertFalse(json.contains("resources"));
  }
}

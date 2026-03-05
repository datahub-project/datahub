package com.linkedin.metadata.config.kubernetes;

import static org.testng.Assert.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ManagedFieldsEntry;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReference;
import java.util.List;
import org.testng.annotations.Test;

/** Unit tests for KubernetesJacksonConfig. */
public class KubernetesJacksonConfigTest {

  @Test
  public void testKubernetesObjectMapperCreation() {
    ObjectMapper mapper = KubernetesJacksonConfig.getObjectMapper();
    assertNotNull(mapper, "kubernetesObjectMapper should not be null");
  }

  @Test
  public void testSingletonInstance() {
    ObjectMapper mapper1 = KubernetesJacksonConfig.getObjectMapper();
    ObjectMapper mapper2 = KubernetesJacksonConfig.getObjectMapper();
    assertSame(mapper1, mapper2, "Should return the same singleton instance");
  }

  @Test
  public void testInstanceMethodReturnsSameMapper() {
    KubernetesJacksonConfig config = new KubernetesJacksonConfig();
    assertSame(
        config.getObjectMapperInstance(),
        KubernetesJacksonConfig.getObjectMapper(),
        "Instance getObjectMapperInstance() should return same mapper as static getObjectMapper()");
  }

  @Test
  public void testManagedFieldsAreStripped() throws JsonProcessingException {
    ObjectMapper k8sMapper = KubernetesJacksonConfig.getObjectMapper();
    ObjectMapper defaultMapper = new ObjectMapper();

    ObjectMeta meta = new ObjectMeta();
    meta.setName("test-resource");
    meta.setNamespace("test-namespace");
    ManagedFieldsEntry managedField = new ManagedFieldsEntry();
    managedField.setManager("test-manager");
    managedField.setOperation("Apply");
    meta.setManagedFields(List.of(managedField));

    String defaultJson = defaultMapper.writeValueAsString(meta);
    assertTrue(
        defaultJson.contains("managedFields"),
        "Default mapper should include managedFields: " + defaultJson);

    String k8sJson = k8sMapper.writeValueAsString(meta);
    assertFalse(
        k8sJson.contains("managedFields"), "K8s mapper should strip managedFields: " + k8sJson);

    assertTrue(k8sJson.contains("test-resource"), "name should still be present");
    assertTrue(k8sJson.contains("test-namespace"), "namespace should still be present");
  }

  @Test
  public void testOwnerReferencesAreStripped() throws JsonProcessingException {
    ObjectMapper k8sMapper = KubernetesJacksonConfig.getObjectMapper();
    ObjectMapper defaultMapper = new ObjectMapper();

    ObjectMeta meta = new ObjectMeta();
    meta.setName("test-resource");
    OwnerReference owner = new OwnerReference();
    owner.setName("parent-resource");
    owner.setKind("Deployment");
    owner.setApiVersion("apps/v1");
    owner.setUid("test-uid");
    meta.setOwnerReferences(List.of(owner));

    String defaultJson = defaultMapper.writeValueAsString(meta);
    assertTrue(
        defaultJson.contains("ownerReferences"),
        "Default mapper should include ownerReferences: " + defaultJson);

    String k8sJson = k8sMapper.writeValueAsString(meta);
    assertFalse(
        k8sJson.contains("ownerReferences"), "K8s mapper should strip ownerReferences: " + k8sJson);
  }

  @Test
  public void testOtherFieldsPreserved() throws JsonProcessingException {
    ObjectMapper k8sMapper = KubernetesJacksonConfig.getObjectMapper();

    ObjectMeta meta = new ObjectMeta();
    meta.setName("my-deployment");
    meta.setNamespace("production");
    meta.setUid("abc-123");
    meta.setResourceVersion("12345");
    meta.setLabels(java.util.Map.of("app", "myapp", "env", "prod"));
    meta.setAnnotations(java.util.Map.of("description", "My deployment"));

    String json = k8sMapper.writeValueAsString(meta);

    assertTrue(json.contains("my-deployment"), "name should be present");
    assertTrue(json.contains("production"), "namespace should be present");
    assertTrue(json.contains("abc-123"), "uid should be present");
    assertTrue(json.contains("12345"), "resourceVersion should be present");
    assertTrue(json.contains("myapp"), "labels should be present");
    assertTrue(json.contains("My deployment"), "annotations should be present");
  }
}

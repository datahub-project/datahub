package com.linkedin.metadata.utils;

import static com.linkedin.metadata.Constants.DEFAULT_RUN_ID;
import static org.testng.Assert.*;

import com.linkedin.data.template.StringMap;
import com.linkedin.mxe.SystemMetadata;
import org.testng.annotations.Test;

public class SystemMetadataUtilsTest {

  @Test
  public void testCreateDefaultSystemMetadata() {
    SystemMetadata metadata = SystemMetadataUtils.createDefaultSystemMetadata();

    assertNotNull(metadata);
    assertEquals(metadata.getRunId(), DEFAULT_RUN_ID);
    assertTrue(metadata.hasLastObserved());
    assertTrue(metadata.getLastObserved() > 0);
  }

  @Test
  public void testCreateDefaultSystemMetadataWithRunId() {
    String customRunId = "custom-run-id";
    SystemMetadata metadata = SystemMetadataUtils.createDefaultSystemMetadata(customRunId);

    assertNotNull(metadata);
    assertEquals(metadata.getRunId(), customRunId);
    assertTrue(metadata.hasLastObserved());
    assertTrue(metadata.getLastObserved() > 0);
  }

  @Test
  public void testGenerateSystemMetadataIfEmpty() {
    // Test with null input
    SystemMetadata nullMetadata = SystemMetadataUtils.generateSystemMetadataIfEmpty(null);
    assertNotNull(nullMetadata);
    assertEquals(nullMetadata.getRunId(), DEFAULT_RUN_ID);
    assertTrue(nullMetadata.hasLastObserved());

    // Test with existing metadata
    SystemMetadata existingMetadata =
        new SystemMetadata().setRunId("existing-run").setLastObserved(1234567890L);
    SystemMetadata result = SystemMetadataUtils.generateSystemMetadataIfEmpty(existingMetadata);

    assertEquals(result.getRunId(), "existing-run");
    assertEquals(result.getLastObserved(), 1234567890L);
  }

  @Test
  public void testParseSystemMetadata() {
    // Test null input
    SystemMetadata nullResult = SystemMetadataUtils.parseSystemMetadata(null);
    assertNotNull(nullResult);
    assertEquals(nullResult.getRunId(), DEFAULT_RUN_ID);

    // Test empty string input
    SystemMetadata emptyResult = SystemMetadataUtils.parseSystemMetadata("");
    assertNotNull(emptyResult);
    assertEquals(emptyResult.getRunId(), DEFAULT_RUN_ID);

    // Test valid JSON input
    String validJson = "{\"runId\":\"test-run\",\"lastObserved\":1234567890}";
    SystemMetadata jsonResult = SystemMetadataUtils.parseSystemMetadata(validJson);
    assertNotNull(jsonResult);
    assertEquals(jsonResult.getRunId(), "test-run");
    assertEquals(jsonResult.getLastObserved(), 1234567890L);
  }

  @Test
  public void testIsNoOp() {
    // Test null metadata
    assertFalse(SystemMetadataUtils.isNoOp(null));

    // Test metadata without properties
    SystemMetadata emptyMetadata = new SystemMetadata();
    assertFalse(SystemMetadataUtils.isNoOp(emptyMetadata));

    // Test metadata with isNoOp=true
    SystemMetadata noOpMetadata = new SystemMetadata();
    StringMap properties = new StringMap();
    properties.put("isNoOp", "true");
    noOpMetadata.setProperties(properties);
    assertTrue(SystemMetadataUtils.isNoOp(noOpMetadata));

    // Test metadata with isNoOp=false
    properties.put("isNoOp", "false");
    assertFalse(SystemMetadataUtils.isNoOp(noOpMetadata));
  }

  @Test
  public void testSetNoOp() {
    // Test with null metadata
    assertNull(SystemMetadataUtils.setNoOp(null, true));

    // Test setting noOp to true
    SystemMetadata metadata = new SystemMetadata();
    SystemMetadata result = SystemMetadataUtils.setNoOp(metadata, true);
    assertNotNull(result);
    assertTrue(result.hasProperties());
    assertNotNull(result.getProperties());
    assertEquals(result.getProperties().get("isNoOp"), "true");

    // Test setting noOp to false
    result = SystemMetadataUtils.setNoOp(metadata, false);
    assertNotNull(result);
    assertTrue(result.hasProperties());
    assertNotNull(result.getProperties());
    assertEquals(result.getProperties().get("isNoOp"), "false");

    // Test with existing properties
    StringMap existingProps = new StringMap();
    existingProps.put("otherKey", "value");
    metadata.setProperties(existingProps);
    result = SystemMetadataUtils.setNoOp(metadata, true);
    assertNotNull(result);
    assertEquals(result.getProperties().get("otherKey"), "value");
    assertEquals(result.getProperties().get("isNoOp"), "true");
  }

  @Test
  public void testGenerateSystemMetadataIfEmpty_NullInput() {
    SystemMetadata result = SystemMetadataUtils.generateSystemMetadataIfEmpty(null);

    assertNotNull(result);
    assertEquals(DEFAULT_RUN_ID, result.getRunId());
    assertNotNull(result.getLastObserved());
    assertTrue(result.getLastObserved() > 0);
  }

  @Test
  public void testGenerateSystemMetadataIfEmpty_NoRunId() {
    SystemMetadata input = new SystemMetadata().setLastObserved(1234567890L);

    SystemMetadata result = SystemMetadataUtils.generateSystemMetadataIfEmpty(input);

    assertNotNull(result);
    assertEquals(DEFAULT_RUN_ID, result.getRunId());
    assertEquals(1234567890L, result.getLastObserved().longValue());
  }

  @Test
  public void testGenerateSystemMetadataIfEmpty_NoLastObserved() {
    SystemMetadata input = new SystemMetadata().setRunId("custom-run-id");

    SystemMetadata result = SystemMetadataUtils.generateSystemMetadataIfEmpty(input);

    assertNotNull(result);
    assertEquals("custom-run-id", result.getRunId());
    assertNotNull(result.getLastObserved());
    assertTrue(result.getLastObserved() > 0);
  }

  @Test
  public void testGenerateSystemMetadataIfEmpty_ZeroLastObserved() {
    SystemMetadata input = new SystemMetadata().setRunId("custom-run-id").setLastObserved(0L);

    SystemMetadata result = SystemMetadataUtils.generateSystemMetadataIfEmpty(input);

    assertNotNull(result);
    assertEquals("custom-run-id", result.getRunId());
    assertNotNull(result.getLastObserved());
    assertTrue(result.getLastObserved() > 0);
  }

  @Test
  public void testGenerateSystemMetadataIfEmpty_AllFieldsPopulated() {
    SystemMetadata input =
        new SystemMetadata().setRunId("custom-run-id").setLastObserved(1234567890L);

    SystemMetadata result = SystemMetadataUtils.generateSystemMetadataIfEmpty(input);

    assertNotNull(result);
    assertEquals("custom-run-id", result.getRunId());
    assertEquals(1234567890L, result.getLastObserved().longValue());
  }
}

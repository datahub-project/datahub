package com.linkedin.metadata.kafka.config;

import org.mockito.Mockito;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CDCProcessorConditionTest {

  private CDCProcessorCondition condition;
  private ConditionContext mockContext;
  private Environment mockEnvironment;
  private AnnotatedTypeMetadata mockMetadata;

  @BeforeMethod
  public void setup() {
    condition = new CDCProcessorCondition();
    mockContext = Mockito.mock(ConditionContext.class);
    mockEnvironment = Mockito.mock(Environment.class);
    mockMetadata = Mockito.mock(AnnotatedTypeMetadata.class);

    Mockito.when(mockContext.getEnvironment()).thenReturn(mockEnvironment);
  }

  @Test
  public void testCondition_MatchesWhenBothPropertiesTrue() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("mclProcessing.cdcSource.enabled", "false"))
        .thenReturn("true");
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");

    // Execute
    boolean result = condition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertTrue(
        result,
        "CDC condition should match when both mclProcessing.cdcSource.enabled and MCE_CONSUMER_ENABLED are true");
  }

  @Test
  public void testCondition_NoMatchWhenCdcSourceDisabled() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("mclProcessing.cdcSource.enabled", "false"))
        .thenReturn("false");
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");

    // Execute
    boolean result = condition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertFalse(
        result, "CDC condition should not match when mclProcessing.cdcSource.enabled is false");
  }

  @Test
  public void testCondition_NoMatchWhenMceConsumerDisabled() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("mclProcessing.cdcSource.enabled", "false"))
        .thenReturn("true");
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("false");

    // Execute
    boolean result = condition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertFalse(result, "CDC condition should not match when MCE_CONSUMER_ENABLED is false");
  }

  @Test
  public void testCondition_NoMatchWhenMceConsumerNull() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("mclProcessing.cdcSource.enabled", "false"))
        .thenReturn("true");
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn(null);

    // Execute
    boolean result = condition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertFalse(result, "CDC condition should not match when MCE_CONSUMER_ENABLED is null");
  }

  @Test
  public void testCondition_NoMatchWhenBothPropertiesDisabled() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("mclProcessing.cdcSource.enabled", "false"))
        .thenReturn("false");
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("false");

    // Execute
    boolean result = condition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertFalse(result, "CDC condition should not match when both properties are disabled");
  }

  @Test
  public void testCondition_DefaultCdcSourceValueIsFalse() {
    // Setup - not setting the cdcSource property explicitly should use default "false"
    Mockito.when(mockEnvironment.getProperty("mclProcessing.cdcSource.enabled", "false"))
        .thenReturn("false");
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");

    // Execute
    boolean result = condition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertFalse(
        result, "CDC condition should not match with default cdcSource value (false)");
  }

  @Test
  public void testCondition_CaseSensitivePropertyValues() {
    // Test cdcSource case sensitivity
    Mockito.when(mockEnvironment.getProperty("mclProcessing.cdcSource.enabled", "false"))
        .thenReturn("TRUE");
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");

    boolean result = condition.matches(mockContext, mockMetadata);
    Assert.assertFalse(result, "CDC condition should not match with 'TRUE' (case sensitive)");

    // Test MCE_CONSUMER_ENABLED case sensitivity
    Mockito.when(mockEnvironment.getProperty("mclProcessing.cdcSource.enabled", "false"))
        .thenReturn("true");
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("TRUE");

    result = condition.matches(mockContext, mockMetadata);
    Assert.assertFalse(
        result,
        "CDC condition should not match with 'TRUE' for MCE_CONSUMER_ENABLED (case sensitive)");
  }

  @Test
  public void testCondition_NonBooleanPropertyValues() {
    // Test non-boolean values for cdcSource
    Mockito.when(mockEnvironment.getProperty("mclProcessing.cdcSource.enabled", "false"))
        .thenReturn("1");
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");

    boolean result = condition.matches(mockContext, mockMetadata);
    Assert.assertFalse(
        result, "CDC condition should not match with '1' (only 'true' string matches)");

    // Test with 'yes'
    Mockito.when(mockEnvironment.getProperty("mclProcessing.cdcSource.enabled", "false"))
        .thenReturn("yes");

    result = condition.matches(mockContext, mockMetadata);
    Assert.assertFalse(
        result, "CDC condition should not match with 'yes' (only 'true' string matches)");

    // Test with empty string
    Mockito.when(mockEnvironment.getProperty("mclProcessing.cdcSource.enabled", "false"))
        .thenReturn("");

    result = condition.matches(mockContext, mockMetadata);
    Assert.assertFalse(result, "CDC condition should not match with empty string");
  }

  @Test
  public void testCondition_WhitespaceHandling() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("mclProcessing.cdcSource.enabled", "false"))
        .thenReturn(" true ");
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");

    // Execute
    boolean result = condition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertFalse(
        result,
        "CDC condition should not match with whitespace around 'true' (exact match required)");
  }

  @Test
  public void testCondition_ConsistentResults() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("mclProcessing.cdcSource.enabled", "false"))
        .thenReturn("true");
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");

    // Execute multiple times
    boolean result1 = condition.matches(mockContext, mockMetadata);
    boolean result2 = condition.matches(mockContext, mockMetadata);
    boolean result3 = condition.matches(mockContext, mockMetadata);

    // Verify consistency
    Assert.assertTrue(result1, "First call should match");
    Assert.assertTrue(result2, "Second call should match");
    Assert.assertTrue(result3, "Third call should match");
    Assert.assertEquals(result1, result2, "Results should be consistent across calls");
    Assert.assertEquals(result2, result3, "Results should be consistent across calls");
  }
}

package com.linkedin.metadata.kafka.config;

import org.mockito.Mockito;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MetadataChangeEventsProcessorConditionTest {

  private MetadataChangeEventsProcessorCondition condition;
  private ConditionContext mockContext;
  private Environment mockEnvironment;
  private AnnotatedTypeMetadata mockMetadata;

  @BeforeMethod
  public void setup() {
    condition = new MetadataChangeEventsProcessorCondition();
    mockContext = Mockito.mock(ConditionContext.class);
    mockEnvironment = Mockito.mock(Environment.class);
    mockMetadata = Mockito.mock(AnnotatedTypeMetadata.class);

    Mockito.when(mockContext.getEnvironment()).thenReturn(mockEnvironment);
  }

  @Test
  public void testCondition_MatchesWhenMceConsumerEnabled() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");

    // Execute
    boolean result = condition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertTrue(result, "Condition should match when MCE_CONSUMER_ENABLED is true");
  }

  @Test
  public void testCondition_NoMatchWhenMceConsumerDisabled() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("false");

    // Execute
    boolean result = condition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertFalse(result, "Condition should not match when MCE_CONSUMER_ENABLED is false");
  }

  @Test
  public void testCondition_NoMatchWhenMceConsumerNotSet() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn(null);

    // Execute
    boolean result = condition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertFalse(result, "Condition should not match when MCE_CONSUMER_ENABLED is not set");
  }

  @Test
  public void testCondition_NoMatchWhenMceConsumerEmptyString() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("");

    // Execute
    boolean result = condition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertFalse(result, "Condition should not match when MCE_CONSUMER_ENABLED is empty");
  }

  @Test
  public void testCondition_CaseSensitiveTrue() {
    // Setup - "TRUE" (uppercase) should not match
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("TRUE");

    // Execute
    boolean result = condition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertFalse(
        result, "Condition should not match when MCE_CONSUMER_ENABLED is 'TRUE' (case-sensitive)");
  }

  @Test
  public void testCondition_IgnoresBatchMode() {
    // Setup - MCE consumer should be enabled regardless of batch mode setting
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");
    Mockito.when(
            mockEnvironment.getProperty("metadataChangeProposal.consumer.batch.enabled", "false"))
        .thenReturn("true");

    // Execute
    boolean result = condition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertTrue(
        result, "MCE condition should match regardless of batch mode (no batch MCE consumer)");
  }

  @Test
  public void testCondition_IndependentOfMcpConsumer() {
    // Setup - MCE consumer should be enabled regardless of MCP_CONSUMER_ENABLED
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");
    Mockito.when(mockEnvironment.getProperty("MCP_CONSUMER_ENABLED")).thenReturn("false");

    // Execute
    boolean result = condition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertTrue(
        result, "MCE condition should match regardless of MCP_CONSUMER_ENABLED setting");
  }
}

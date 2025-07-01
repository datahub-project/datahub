package com.linkedin.metadata.kafka.config;

import com.linkedin.metadata.kafka.config.batch.BatchMetadataChangeProposalProcessorCondition;
import org.mockito.Mockito;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MetadataChangeProposalProcessorConditionsTest {

  private MetadataChangeProposalProcessorCondition regularCondition;
  private BatchMetadataChangeProposalProcessorCondition batchCondition;
  private ConditionContext mockContext;
  private Environment mockEnvironment;
  private AnnotatedTypeMetadata mockMetadata;

  @BeforeMethod
  public void setup() {
    regularCondition = new MetadataChangeProposalProcessorCondition();
    batchCondition = new BatchMetadataChangeProposalProcessorCondition();
    mockContext = Mockito.mock(ConditionContext.class);
    mockEnvironment = Mockito.mock(Environment.class);
    mockMetadata = Mockito.mock(AnnotatedTypeMetadata.class);

    Mockito.when(mockContext.getEnvironment()).thenReturn(mockEnvironment);
  }

  // Tests for MetadataChangeProposalProcessorCondition

  @Test
  public void testRegularCondition_MatchesWhenMceEnabledAndBatchDisabled() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");
    Mockito.when(mockEnvironment.getProperty("MCP_CONSUMER_ENABLED")).thenReturn(null);
    Mockito.when(
            mockEnvironment.getProperty("metadataChangeProposal.consumer.batch.enabled", "false"))
        .thenReturn("false");

    // Execute
    boolean result = regularCondition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertTrue(
        result,
        "Regular condition should match when MCE_CONSUMER_ENABLED is true and batch is disabled");
  }

  @Test
  public void testRegularCondition_MatchesWhenMcpEnabledAndBatchDisabled() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn(null);
    Mockito.when(mockEnvironment.getProperty("MCP_CONSUMER_ENABLED")).thenReturn("true");
    Mockito.when(
            mockEnvironment.getProperty("metadataChangeProposal.consumer.batch.enabled", "false"))
        .thenReturn("false");

    // Execute
    boolean result = regularCondition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertTrue(
        result,
        "Regular condition should match when MCP_CONSUMER_ENABLED is true and batch is disabled");
  }

  @Test
  public void testRegularCondition_NoMatchWhenBatchEnabled() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");
    Mockito.when(mockEnvironment.getProperty("MCP_CONSUMER_ENABLED")).thenReturn("true");
    Mockito.when(
            mockEnvironment.getProperty("metadataChangeProposal.consumer.batch.enabled", "false"))
        .thenReturn("true");

    // Execute
    boolean result = regularCondition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertFalse(
        result, "Regular condition should not match when batch processing is enabled");
  }

  @Test
  public void testRegularCondition_NoMatchWhenConsumersDisabled() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("false");
    Mockito.when(mockEnvironment.getProperty("MCP_CONSUMER_ENABLED")).thenReturn("false");
    Mockito.when(
            mockEnvironment.getProperty("metadataChangeProposal.consumer.batch.enabled", "false"))
        .thenReturn("false");

    // Execute
    boolean result = regularCondition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertFalse(
        result, "Regular condition should not match when both MCE and MCP consumers are disabled");
  }

  // Tests for BatchMetadataChangeProposalProcessorCondition

  @Test
  public void testBatchCondition_MatchesWhenMceEnabledAndBatchEnabled() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");
    Mockito.when(mockEnvironment.getProperty("MCP_CONSUMER_ENABLED")).thenReturn(null);
    Mockito.when(
            mockEnvironment.getProperty("metadataChangeProposal.consumer.batch.enabled", "false"))
        .thenReturn("true");

    // Execute
    boolean result = batchCondition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertTrue(
        result,
        "Batch condition should match when MCE_CONSUMER_ENABLED is true and batch is enabled");
  }

  @Test
  public void testBatchCondition_MatchesWhenMcpEnabledAndBatchEnabled() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn(null);
    Mockito.when(mockEnvironment.getProperty("MCP_CONSUMER_ENABLED")).thenReturn("true");
    Mockito.when(
            mockEnvironment.getProperty("metadataChangeProposal.consumer.batch.enabled", "false"))
        .thenReturn("true");

    // Execute
    boolean result = batchCondition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertTrue(
        result,
        "Batch condition should match when MCP_CONSUMER_ENABLED is true and batch is enabled");
  }

  @Test
  public void testBatchCondition_MatchesWhenBothEnabledAndBatchEnabled() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");
    Mockito.when(mockEnvironment.getProperty("MCP_CONSUMER_ENABLED")).thenReturn("true");
    Mockito.when(
            mockEnvironment.getProperty("metadataChangeProposal.consumer.batch.enabled", "false"))
        .thenReturn("true");

    // Execute
    boolean result = batchCondition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertTrue(
        result,
        "Batch condition should match when both MCE and MCP are enabled and batch is enabled");
  }

  @Test
  public void testBatchCondition_NoMatchWhenBatchDisabled() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");
    Mockito.when(mockEnvironment.getProperty("MCP_CONSUMER_ENABLED")).thenReturn("true");
    Mockito.when(
            mockEnvironment.getProperty("metadataChangeProposal.consumer.batch.enabled", "false"))
        .thenReturn("false");

    // Execute
    boolean result = batchCondition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertFalse(
        result, "Batch condition should not match when batch processing is disabled");
  }

  @Test
  public void testBatchCondition_NoMatchWhenConsumersDisabled() {
    // Setup
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("false");
    Mockito.when(mockEnvironment.getProperty("MCP_CONSUMER_ENABLED")).thenReturn("false");
    Mockito.when(
            mockEnvironment.getProperty("metadataChangeProposal.consumer.batch.enabled", "false"))
        .thenReturn("true");

    // Execute
    boolean result = batchCondition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertFalse(
        result, "Batch condition should not match when both MCE and MCP consumers are disabled");
  }

  @Test
  public void testBatchCondition_DefaultBatchValueIsFalse() {
    // Setup - not setting the batch property explicitly should use default "false"
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");
    Mockito.when(mockEnvironment.getProperty("MCP_CONSUMER_ENABLED")).thenReturn(null);
    Mockito.when(
            mockEnvironment.getProperty("metadataChangeProposal.consumer.batch.enabled", "false"))
        .thenReturn("false");

    // Execute
    boolean result = batchCondition.matches(mockContext, mockMetadata);

    // Verify
    Assert.assertFalse(result, "Batch condition should not match with default batch value (false)");
  }

  @Test
  public void testBothConditionsAreMutuallyExclusive() {
    // These tests verify that in any given environment configuration,
    // only one (or neither) of the conditions can match, but never both

    // Test case 1: MCE enabled, batch disabled
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("true");
    Mockito.when(mockEnvironment.getProperty("MCP_CONSUMER_ENABLED")).thenReturn(null);
    Mockito.when(
            mockEnvironment.getProperty("metadataChangeProposal.consumer.batch.enabled", "false"))
        .thenReturn("false");

    boolean regularMatches = regularCondition.matches(mockContext, mockMetadata);
    boolean batchMatches = batchCondition.matches(mockContext, mockMetadata);

    Assert.assertTrue(regularMatches, "Regular condition should match");
    Assert.assertFalse(batchMatches, "Batch condition should not match");
    Assert.assertNotEquals(regularMatches, batchMatches, "Conditions should be mutually exclusive");

    // Test case 2: MCE enabled, batch enabled
    Mockito.when(
            mockEnvironment.getProperty("metadataChangeProposal.consumer.batch.enabled", "false"))
        .thenReturn("true");

    regularMatches = regularCondition.matches(mockContext, mockMetadata);
    batchMatches = batchCondition.matches(mockContext, mockMetadata);

    Assert.assertFalse(regularMatches, "Regular condition should not match");
    Assert.assertTrue(batchMatches, "Batch condition should match");
    Assert.assertNotEquals(regularMatches, batchMatches, "Conditions should be mutually exclusive");

    // Test case 3: Both consumers disabled, batch setting irrelevant
    Mockito.when(mockEnvironment.getProperty("MCE_CONSUMER_ENABLED")).thenReturn("false");
    Mockito.when(mockEnvironment.getProperty("MCP_CONSUMER_ENABLED")).thenReturn("false");

    regularMatches = regularCondition.matches(mockContext, mockMetadata);
    batchMatches = batchCondition.matches(mockContext, mockMetadata);

    Assert.assertFalse(regularMatches, "Regular condition should not match");
    Assert.assertFalse(batchMatches, "Batch condition should not match");
  }
}

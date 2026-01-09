package com.linkedin.metadata.entity.validation;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.AspectSizeValidationConfig;
import com.linkedin.metadata.config.AspectSizeValidationConfig.AspectCheckpointConfig;
import com.linkedin.metadata.config.OversizedAspectRemediation;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AspectSizeValidatorTest {

  private Urn urn;
  private static final String URN_STRING =
      "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)";
  private static final String ASPECT_NAME = "status";

  private OperationContext mockOpContext;
  private List<AspectDeletionRequest> pendingDeletions;

  @BeforeMethod
  public void setup() throws Exception {
    MockitoAnnotations.openMocks(this);
    urn = Urn.createFromString(URN_STRING);

    // Create a real mutable list for pending deletions
    pendingDeletions = new ArrayList<>();

    // Mock OperationContext to use the mutable list
    mockOpContext = mock(OperationContext.class);
    doAnswer(
            invocation -> {
              pendingDeletions.add(invocation.getArgument(0));
              return null;
            })
        .when(mockOpContext)
        .addPendingDeletion(any());
    when(mockOpContext.getPendingDeletions())
        .thenAnswer(invocation -> new ArrayList<>(pendingDeletions));
    doAnswer(
            invocation -> {
              pendingDeletions.clear();
              return null;
            })
        .when(mockOpContext)
        .clearPendingDeletions();
  }

  @AfterMethod
  public void cleanup() {
    pendingDeletions.clear();
  }

  @Test
  public void testValidationDisabledWithNullConfig() {
    String metadata = generateLargeMetadata(20000000); // 20MB

    // Should not throw with null config
    AspectSizeValidator.validatePrePatchSize(metadata, urn, ASPECT_NAME, null, mockOpContext, null);

    // Should not add deletion requests
    assertEquals(mockOpContext.getPendingDeletions().size(), 0);
  }

  @Test
  public void testValidationDisabledWithNullPrePatch() {
    AspectSizeValidationConfig config = new AspectSizeValidationConfig();
    config.setPrePatch(null);

    String metadata = generateLargeMetadata(20000000); // 20MB

    // Should not throw
    AspectSizeValidator.validatePrePatchSize(
        metadata, urn, ASPECT_NAME, config, mockOpContext, null);

    // Should not add deletion requests
    assertEquals(mockOpContext.getPendingDeletions().size(), 0);
  }

  @Test
  public void testValidationDisabledWhenNotEnabled() {
    AspectSizeValidationConfig config = new AspectSizeValidationConfig();
    AspectCheckpointConfig prePatchConfig = new AspectCheckpointConfig();
    prePatchConfig.setEnabled(false);
    config.setPrePatch(prePatchConfig);

    String metadata = generateLargeMetadata(20000000); // 20MB

    // Should not throw
    AspectSizeValidator.validatePrePatchSize(
        metadata, urn, ASPECT_NAME, config, mockOpContext, null);

    // Should not add deletion requests
    assertEquals(mockOpContext.getPendingDeletions().size(), 0);
  }

  @Test
  public void testValidationWithNullMetadata() {
    AspectSizeValidationConfig config =
        createEnabledConfig(15728640L, OversizedAspectRemediation.DELETE);

    // Should not throw with null metadata
    AspectSizeValidator.validatePrePatchSize(null, urn, ASPECT_NAME, config);

    // Should not add deletion requests
    assertEquals(mockOpContext.getPendingDeletions().size(), 0);
  }

  @Test
  public void testValidationPassesForSmallAspect() {
    AspectSizeValidationConfig config =
        createEnabledConfig(15728640L, OversizedAspectRemediation.DELETE);

    String metadata = generateLargeMetadata(1000); // 1KB

    // Should not throw
    AspectSizeValidator.validatePrePatchSize(
        metadata, urn, ASPECT_NAME, config, mockOpContext, null);

    // Should not add deletion requests
    assertEquals(mockOpContext.getPendingDeletions().size(), 0);
  }

  @Test
  public void testValidationFailsWithIgnoreRemediation() {
    AspectSizeValidationConfig config =
        createEnabledConfig(15728640L, OversizedAspectRemediation.IGNORE);

    String metadata = generateLargeMetadata(20000000); // 20MB

    try {
      AspectSizeValidator.validatePrePatchSize(
          metadata, urn, ASPECT_NAME, config, mockOpContext, null);
      fail("Expected AspectSizeExceededException");
    } catch (AspectSizeExceededException exception) {
      assertEquals(exception.getValidationPoint(), "PRE_DB_PATCH");
      assertEquals(exception.getActualSize(), 20000000L);
      assertEquals(exception.getThreshold(), 15728640L);
      assertEquals(exception.getUrn(), URN_STRING);
      assertEquals(exception.getAspectName(), ASPECT_NAME);

      // IGNORE remediation should NOT add deletion request
      List<AspectDeletionRequest> deletions =
          mockOpContext.getPendingDeletions().stream()
              .filter(obj -> obj instanceof AspectDeletionRequest)
              .map(obj -> (AspectDeletionRequest) obj)
              .collect(java.util.stream.Collectors.toList());
      assertEquals(deletions.size(), 0);
    }
  }

  @Test
  public void testValidationFailsWithDeleteRemediation() {
    AspectSizeValidationConfig config =
        createEnabledConfig(15728640L, OversizedAspectRemediation.DELETE);

    String metadata = generateLargeMetadata(20000000); // 20MB

    try {
      AspectSizeValidator.validatePrePatchSize(
          metadata, urn, ASPECT_NAME, config, mockOpContext, null);
      fail("Expected AspectSizeExceededException");
    } catch (AspectSizeExceededException exception) {
      assertEquals(exception.getValidationPoint(), "PRE_DB_PATCH");
      assertEquals(exception.getActualSize(), 20000000L);
      assertEquals(exception.getThreshold(), 15728640L);
      assertEquals(exception.getUrn(), URN_STRING);
      assertEquals(exception.getAspectName(), ASPECT_NAME);

      // DELETE remediation should add deletion request to ThreadLocal
      List<AspectDeletionRequest> deletions =
          mockOpContext.getPendingDeletions().stream()
              .filter(obj -> obj instanceof AspectDeletionRequest)
              .map(obj -> (AspectDeletionRequest) obj)
              .collect(java.util.stream.Collectors.toList());
      assertEquals(deletions.size(), 1);
      AspectDeletionRequest deletion = deletions.get(0);
      assertEquals(deletion.getUrn(), urn);
      assertEquals(deletion.getAspectName(), ASPECT_NAME);
      assertEquals(deletion.getValidationPoint(), "PRE_DB_PATCH");
      assertEquals(deletion.getAspectSize(), 20000000L);
      assertEquals(deletion.getThreshold(), 15728640L);
    }
  }

  @Test
  public void testValidationAtExactThreshold() {
    long threshold = 15728640L;
    AspectSizeValidationConfig config =
        createEnabledConfig(threshold, OversizedAspectRemediation.DELETE);

    String metadata = generateLargeMetadata((int) threshold);

    // Should not throw - exact threshold is allowed
    AspectSizeValidator.validatePrePatchSize(
        metadata, urn, ASPECT_NAME, config, mockOpContext, null);

    // Should not add deletion requests
    assertEquals(mockOpContext.getPendingDeletions().size(), 0);
  }

  @Test
  public void testValidationOneByteOverThreshold() {
    long threshold = 15728640L;
    AspectSizeValidationConfig config =
        createEnabledConfig(threshold, OversizedAspectRemediation.DELETE);

    String metadata = generateLargeMetadata((int) (threshold + 1));

    try {
      AspectSizeValidator.validatePrePatchSize(
          metadata, urn, ASPECT_NAME, config, mockOpContext, null);
      fail("Expected AspectSizeExceededException");
    } catch (AspectSizeExceededException exception) {
      assertEquals(exception.getActualSize(), threshold + 1);
      assertEquals(exception.getThreshold(), threshold);

      // Verify deletion request was added
      List<AspectDeletionRequest> deletions =
          mockOpContext.getPendingDeletions().stream()
              .filter(obj -> obj instanceof AspectDeletionRequest)
              .map(obj -> (AspectDeletionRequest) obj)
              .collect(java.util.stream.Collectors.toList());
      assertEquals(deletions.size(), 1);
    }
  }

  @Test
  public void testMultipleValidationFailuresAccumulateDeletions() {
    AspectSizeValidationConfig config =
        createEnabledConfig(1000L, OversizedAspectRemediation.DELETE);

    String metadata1 = generateLargeMetadata(2000);
    String metadata2 = generateLargeMetadata(3000);

    // First validation should add deletion request
    try {
      AspectSizeValidator.validatePrePatchSize(
          metadata1, urn, "aspect1", config, mockOpContext, null);
      fail("Expected AspectSizeExceededException");
    } catch (AspectSizeExceededException e) {
      // Expected
    }

    // Second validation should add another deletion request
    try {
      AspectSizeValidator.validatePrePatchSize(
          metadata2, urn, "aspect2", config, mockOpContext, null);
      fail("Expected AspectSizeExceededException");
    } catch (AspectSizeExceededException e) {
      // Expected
    }

    // Both deletion requests should be in ThreadLocal
    List<AspectDeletionRequest> deletions =
        mockOpContext.getPendingDeletions().stream()
            .filter(obj -> obj instanceof AspectDeletionRequest)
            .map(obj -> (AspectDeletionRequest) obj)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(deletions.size(), 2);
    assertEquals(deletions.get(0).getAspectName(), "aspect1");
    assertEquals(deletions.get(1).getAspectName(), "aspect2");
  }

  @Test
  public void testValidationSkippedForRemediationDeletion() {
    AspectSizeValidationConfig config =
        createEnabledConfig(1000L, OversizedAspectRemediation.DELETE);

    String metadata = generateLargeMetadata(20000000); // 20MB - way over threshold

    // Create OperationContext with isRemediationDeletion flag
    io.datahubproject.metadata.context.ValidationContext validationContext =
        io.datahubproject.metadata.context.ValidationContext.builder()
            .alternateValidation(false)
            .isRemediationDeletion(true)
            .build();
    when(mockOpContext.getValidationContext()).thenReturn(validationContext);

    // Should not throw even though aspect is oversized
    AspectSizeValidator.validatePrePatchSize(
        metadata, urn, ASPECT_NAME, config, mockOpContext, null);

    // Should not add deletion requests
    assertEquals(mockOpContext.getPendingDeletions().size(), 0);
  }

  @Test
  public void testValidationSkippedWithFalseRemediationFlag() {
    AspectSizeValidationConfig config =
        createEnabledConfig(1000L, OversizedAspectRemediation.DELETE);

    String metadata = generateLargeMetadata(20000); // Over threshold

    // Create OperationContext with isRemediationDeletion = false
    io.datahubproject.metadata.context.ValidationContext validationContext =
        io.datahubproject.metadata.context.ValidationContext.builder()
            .alternateValidation(false)
            .isRemediationDeletion(false)
            .build();
    when(mockOpContext.getValidationContext()).thenReturn(validationContext);

    // Should throw because flag is false
    try {
      AspectSizeValidator.validatePrePatchSize(
          metadata, urn, ASPECT_NAME, config, mockOpContext, null);
      fail("Expected AspectSizeExceededException");
    } catch (AspectSizeExceededException e) {
      assertEquals(e.getActualSize(), 20000L);
    }
  }

  @Test
  public void testValidationWithWarningThreshold() {
    AspectSizeValidationConfig config = new AspectSizeValidationConfig();
    AspectCheckpointConfig prePatchConfig = new AspectCheckpointConfig();
    prePatchConfig.setEnabled(true);
    prePatchConfig.setWarnSizeBytes(1000L); // Warn at 1KB
    prePatchConfig.setMaxSizeBytes(10000L); // Block at 10KB
    prePatchConfig.setOversizedRemediation(OversizedAspectRemediation.IGNORE);
    config.setPrePatch(prePatchConfig);

    String metadata = generateLargeMetadata(5000); // 5KB - above warn, below max

    // Should NOT throw - only logs warning
    AspectSizeValidator.validatePrePatchSize(
        metadata, urn, ASPECT_NAME, config, mockOpContext, null);

    // Should not add deletion requests
    assertEquals(mockOpContext.getPendingDeletions().size(), 0);
  }

  @Test
  public void testValidationWithNullWarningThreshold() {
    AspectSizeValidationConfig config = new AspectSizeValidationConfig();
    AspectCheckpointConfig prePatchConfig = new AspectCheckpointConfig();
    prePatchConfig.setEnabled(true);
    prePatchConfig.setWarnSizeBytes(null); // No warning threshold
    prePatchConfig.setMaxSizeBytes(10000L);
    prePatchConfig.setOversizedRemediation(OversizedAspectRemediation.IGNORE);
    config.setPrePatch(prePatchConfig);

    String metadata = generateLargeMetadata(5000); // 5KB

    // Should pass - no warning threshold set
    AspectSizeValidator.validatePrePatchSize(
        metadata, urn, ASPECT_NAME, config, mockOpContext, null);

    // Should not add deletion requests
    assertEquals(mockOpContext.getPendingDeletions().size(), 0);
  }

  @Test
  public void testValidationWithEmptyContext() {
    AspectSizeValidationConfig config =
        createEnabledConfig(1000L, OversizedAspectRemediation.DELETE);

    String metadata = generateLargeMetadata(20000); // Over threshold

    // Empty ValidationContext (or null) should not skip validation
    io.datahubproject.metadata.context.ValidationContext validationContext =
        io.datahubproject.metadata.context.ValidationContext.builder()
            .alternateValidation(false)
            .isRemediationDeletion(false)
            .build();
    when(mockOpContext.getValidationContext()).thenReturn(validationContext);

    try {
      AspectSizeValidator.validatePrePatchSize(
          metadata, urn, ASPECT_NAME, config, mockOpContext, null);
      fail("Expected AspectSizeExceededException");
    } catch (AspectSizeExceededException e) {
      // Expected - validation should run normally
      assertEquals(e.getActualSize(), 20000L);
    }
  }

  private AspectSizeValidationConfig createEnabledConfig(
      long maxSizeBytes, OversizedAspectRemediation remediation) {
    AspectSizeValidationConfig config = new AspectSizeValidationConfig();
    AspectCheckpointConfig prePatchConfig = new AspectCheckpointConfig();
    prePatchConfig.setEnabled(true);
    prePatchConfig.setMaxSizeBytes(maxSizeBytes);
    prePatchConfig.setOversizedRemediation(remediation);
    config.setPrePatch(prePatchConfig);
    return config;
  }

  private String generateLargeMetadata(int size) {
    StringBuilder sb = new StringBuilder(size);
    for (int i = 0; i < size; i++) {
      sb.append('x');
    }
    return sb.toString();
  }
}

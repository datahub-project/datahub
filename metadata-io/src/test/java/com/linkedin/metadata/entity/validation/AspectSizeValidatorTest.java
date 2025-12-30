package com.linkedin.metadata.entity.validation;

import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.AspectSizeValidationConfig;
import com.linkedin.metadata.config.AspectSizeValidationConfig.AspectCheckpointConfig;
import com.linkedin.metadata.config.OversizedAspectRemediation;
import java.util.List;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AspectSizeValidatorTest {

  private Urn urn;
  private static final String URN_STRING =
      "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)";
  private static final String ASPECT_NAME = "status";

  @BeforeMethod
  public void setup() throws Exception {
    urn = Urn.createFromString(URN_STRING);
    AspectValidationContext.clearPendingDeletions();
  }

  @AfterMethod
  public void cleanup() {
    AspectValidationContext.clearPendingDeletions();
  }

  @Test
  public void testValidationDisabledWithNullConfig() {
    String metadata = generateLargeMetadata(20000000); // 20MB

    // Should not throw with null config
    AspectSizeValidator.validatePrePatchSize(metadata, urn, ASPECT_NAME, null);

    // Should not add deletion requests
    assertEquals(AspectValidationContext.getPendingDeletions().size(), 0);
  }

  @Test
  public void testValidationDisabledWithNullPrePatch() {
    AspectSizeValidationConfig config = new AspectSizeValidationConfig();
    config.setPrePatch(null);

    String metadata = generateLargeMetadata(20000000); // 20MB

    // Should not throw
    AspectSizeValidator.validatePrePatchSize(metadata, urn, ASPECT_NAME, config);

    // Should not add deletion requests
    assertEquals(AspectValidationContext.getPendingDeletions().size(), 0);
  }

  @Test
  public void testValidationDisabledWhenNotEnabled() {
    AspectSizeValidationConfig config = new AspectSizeValidationConfig();
    AspectCheckpointConfig prePatchConfig = new AspectCheckpointConfig();
    prePatchConfig.setEnabled(false);
    config.setPrePatch(prePatchConfig);

    String metadata = generateLargeMetadata(20000000); // 20MB

    // Should not throw
    AspectSizeValidator.validatePrePatchSize(metadata, urn, ASPECT_NAME, config);

    // Should not add deletion requests
    assertEquals(AspectValidationContext.getPendingDeletions().size(), 0);
  }

  @Test
  public void testValidationWithNullMetadata() {
    AspectSizeValidationConfig config =
        createEnabledConfig(15728640L, OversizedAspectRemediation.DELETE);

    // Should not throw with null metadata
    AspectSizeValidator.validatePrePatchSize(null, urn, ASPECT_NAME, config);

    // Should not add deletion requests
    assertEquals(AspectValidationContext.getPendingDeletions().size(), 0);
  }

  @Test
  public void testValidationPassesForSmallAspect() {
    AspectSizeValidationConfig config =
        createEnabledConfig(15728640L, OversizedAspectRemediation.DELETE);

    String metadata = generateLargeMetadata(1000); // 1KB

    // Should not throw
    AspectSizeValidator.validatePrePatchSize(metadata, urn, ASPECT_NAME, config);

    // Should not add deletion requests
    assertEquals(AspectValidationContext.getPendingDeletions().size(), 0);
  }

  @Test
  public void testValidationFailsWithIgnoreRemediation() {
    AspectSizeValidationConfig config =
        createEnabledConfig(15728640L, OversizedAspectRemediation.IGNORE);

    String metadata = generateLargeMetadata(20000000); // 20MB

    try {
      AspectSizeValidator.validatePrePatchSize(metadata, urn, ASPECT_NAME, config);
      fail("Expected AspectSizeExceededException");
    } catch (AspectSizeExceededException exception) {
      assertEquals(exception.getValidationPoint(), ValidationPoint.PRE_DB_PATCH);
      assertEquals(exception.getActualSize(), 20000000L);
      assertEquals(exception.getThreshold(), 15728640L);
      assertEquals(exception.getUrn(), URN_STRING);
      assertEquals(exception.getAspectName(), ASPECT_NAME);

      // IGNORE remediation should NOT add deletion request
      List<AspectDeletionRequest> deletions = AspectValidationContext.getPendingDeletions();
      assertEquals(deletions.size(), 0);
    }
  }

  @Test
  public void testValidationFailsWithDeleteRemediation() {
    AspectSizeValidationConfig config =
        createEnabledConfig(15728640L, OversizedAspectRemediation.DELETE);

    String metadata = generateLargeMetadata(20000000); // 20MB

    try {
      AspectSizeValidator.validatePrePatchSize(metadata, urn, ASPECT_NAME, config);
      fail("Expected AspectSizeExceededException");
    } catch (AspectSizeExceededException exception) {
      assertEquals(exception.getValidationPoint(), ValidationPoint.PRE_DB_PATCH);
      assertEquals(exception.getActualSize(), 20000000L);
      assertEquals(exception.getThreshold(), 15728640L);
      assertEquals(exception.getUrn(), URN_STRING);
      assertEquals(exception.getAspectName(), ASPECT_NAME);

      // DELETE remediation should add deletion request to ThreadLocal
      List<AspectDeletionRequest> deletions = AspectValidationContext.getPendingDeletions();
      assertEquals(deletions.size(), 1);
      AspectDeletionRequest deletion = deletions.get(0);
      assertEquals(deletion.getUrn(), urn);
      assertEquals(deletion.getAspectName(), ASPECT_NAME);
      assertEquals(deletion.getValidationPoint(), ValidationPoint.PRE_DB_PATCH);
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
    AspectSizeValidator.validatePrePatchSize(metadata, urn, ASPECT_NAME, config);

    // Should not add deletion requests
    assertEquals(AspectValidationContext.getPendingDeletions().size(), 0);
  }

  @Test
  public void testValidationOneByteOverThreshold() {
    long threshold = 15728640L;
    AspectSizeValidationConfig config =
        createEnabledConfig(threshold, OversizedAspectRemediation.DELETE);

    String metadata = generateLargeMetadata((int) (threshold + 1));

    try {
      AspectSizeValidator.validatePrePatchSize(metadata, urn, ASPECT_NAME, config);
      fail("Expected AspectSizeExceededException");
    } catch (AspectSizeExceededException exception) {
      assertEquals(exception.getActualSize(), threshold + 1);
      assertEquals(exception.getThreshold(), threshold);

      // Verify deletion request was added
      List<AspectDeletionRequest> deletions = AspectValidationContext.getPendingDeletions();
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
      AspectSizeValidator.validatePrePatchSize(metadata1, urn, "aspect1", config);
      fail("Expected AspectSizeExceededException");
    } catch (AspectSizeExceededException e) {
      // Expected
    }

    // Second validation should add another deletion request
    try {
      AspectSizeValidator.validatePrePatchSize(metadata2, urn, "aspect2", config);
      fail("Expected AspectSizeExceededException");
    } catch (AspectSizeExceededException e) {
      // Expected
    }

    // Both deletion requests should be in ThreadLocal
    List<AspectDeletionRequest> deletions = AspectValidationContext.getPendingDeletions();
    assertEquals(deletions.size(), 2);
    assertEquals(deletions.get(0).getAspectName(), "aspect1");
    assertEquals(deletions.get(1).getAspectName(), "aspect2");
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

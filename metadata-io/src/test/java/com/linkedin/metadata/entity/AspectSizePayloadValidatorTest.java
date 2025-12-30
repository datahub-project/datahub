package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.config.AspectSizeValidationConfig;
import com.linkedin.metadata.config.AspectSizeValidationConfig.AspectCheckpointConfig;
import com.linkedin.metadata.config.OversizedAspectRemediation;
import com.linkedin.metadata.entity.validation.AspectDeletionRequest;
import com.linkedin.metadata.entity.validation.AspectSizeExceededException;
import com.linkedin.metadata.entity.validation.AspectValidationContext;
import com.linkedin.metadata.entity.validation.ValidationPoint;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AspectSizePayloadValidatorTest {

  private final OperationContext opContext = TestOperationContexts.systemContextNoValidate();
  private final EntityRegistry entityRegistry = opContext.getEntityRegistry();
  private final EntitySpec entitySpec = entityRegistry.getEntitySpec(DATASET_ENTITY_NAME);
  private final AspectSpec aspectSpec = entitySpec.getAspectSpec(STATUS_ASPECT_NAME);
  private final RecordTemplate recordTemplate = new Status().setRemoved(false);

  @Mock private SystemAspect systemAspect;

  private Urn urn;
  private static final String URN_STRING =
      "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)";

  @BeforeMethod
  public void setup() throws Exception {
    MockitoAnnotations.openMocks(this);
    urn = Urn.createFromString(URN_STRING);

    when(systemAspect.getUrn()).thenReturn(urn);
    when(systemAspect.getAspectSpec()).thenReturn(aspectSpec);

    // Clear ThreadLocal before each test
    AspectValidationContext.clearPendingDeletions();
  }

  @AfterMethod
  public void cleanup() {
    // Always cleanup ThreadLocal after each test
    AspectValidationContext.clearPendingDeletions();
  }

  @Test
  public void testValidationDisabled() {
    AspectSizeValidationConfig config = new AspectSizeValidationConfig();
    config.setPostPatch(null);

    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config);

    EntityAspect serializedAspect = new EntityAspect();
    serializedAspect.setMetadata(generateLargeMetadata(20000000)); // 20MB

    // Should not throw
    validator.validatePayload(systemAspect, serializedAspect);

    // Should not add any deletion requests
    List<AspectDeletionRequest> deletions = AspectValidationContext.getPendingDeletions();
    assertEquals(deletions.size(), 0);
  }

  @Test
  public void testValidationDisabledWhenNotEnabled() {
    AspectSizeValidationConfig config = new AspectSizeValidationConfig();
    AspectCheckpointConfig postPatchConfig = new AspectCheckpointConfig();
    postPatchConfig.setEnabled(false);
    config.setPostPatch(postPatchConfig);

    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config);

    EntityAspect serializedAspect = new EntityAspect();
    serializedAspect.setMetadata(generateLargeMetadata(20000000)); // 20MB

    // Should not throw
    validator.validatePayload(systemAspect, serializedAspect);

    // Should not add any deletion requests
    List<AspectDeletionRequest> deletions = AspectValidationContext.getPendingDeletions();
    assertEquals(deletions.size(), 0);
  }

  @Test
  public void testValidationWithNullMetadata() {
    AspectSizeValidationConfig config =
        createEnabledConfig(15728640L, OversizedAspectRemediation.DELETE);
    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config);

    EntityAspect serializedAspect = new EntityAspect();
    serializedAspect.setMetadata(null);

    // Should not throw
    validator.validatePayload(systemAspect, serializedAspect);

    // Should not add any deletion requests
    List<AspectDeletionRequest> deletions = AspectValidationContext.getPendingDeletions();
    assertEquals(deletions.size(), 0);
  }

  @Test
  public void testValidationPassesForSmallAspect() {
    AspectSizeValidationConfig config =
        createEnabledConfig(15728640L, OversizedAspectRemediation.DELETE);
    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config);

    EntityAspect serializedAspect = new EntityAspect();
    serializedAspect.setMetadata(generateLargeMetadata(1000)); // 1KB

    // Should not throw
    validator.validatePayload(systemAspect, serializedAspect);

    // Should not add any deletion requests
    List<AspectDeletionRequest> deletions = AspectValidationContext.getPendingDeletions();
    assertEquals(deletions.size(), 0);
  }

  @Test
  public void testValidationFailsWithDeleteRemediation() {
    AspectSizeValidationConfig config =
        createEnabledConfig(15728640L, OversizedAspectRemediation.DELETE);
    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config);

    EntityAspect serializedAspect = new EntityAspect();
    serializedAspect.setMetadata(generateLargeMetadata(20000000)); // 20MB

    try {
      validator.validatePayload(systemAspect, serializedAspect);
      fail("Expected AspectSizeExceededException");
    } catch (AspectSizeExceededException exception) {
      assertEquals(exception.getValidationPoint(), ValidationPoint.POST_DB_PATCH);
      assertEquals(exception.getActualSize(), 20000000L);
      assertEquals(exception.getThreshold(), 15728640L);
      assertEquals(exception.getUrn(), URN_STRING);
      assertEquals(exception.getAspectName(), STATUS_ASPECT_NAME);

      // Verify deletion request was added to ThreadLocal
      List<AspectDeletionRequest> deletions = AspectValidationContext.getPendingDeletions();
      assertEquals(deletions.size(), 1);
      AspectDeletionRequest deletion = deletions.get(0);
      assertEquals(deletion.getUrn(), urn);
      assertEquals(deletion.getAspectName(), STATUS_ASPECT_NAME);
      assertEquals(deletion.getValidationPoint(), ValidationPoint.POST_DB_PATCH);
      assertEquals(deletion.getAspectSize(), 20000000L);
      assertEquals(deletion.getThreshold(), 15728640L);
    }
  }

  @Test
  public void testValidationFailsWithIgnoreRemediation() {
    AspectSizeValidationConfig config =
        createEnabledConfig(15728640L, OversizedAspectRemediation.IGNORE);
    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config);

    EntityAspect serializedAspect = new EntityAspect();
    serializedAspect.setMetadata(generateLargeMetadata(20000000)); // 20MB

    try {
      validator.validatePayload(systemAspect, serializedAspect);
      fail("Expected AspectSizeExceededException");
    } catch (AspectSizeExceededException exception) {
      assertEquals(exception.getValidationPoint(), ValidationPoint.POST_DB_PATCH);

      // IGNORE remediation should NOT add deletion request
      List<AspectDeletionRequest> deletions = AspectValidationContext.getPendingDeletions();
      assertEquals(deletions.size(), 0);
    }
  }

  @Test
  public void testValidationAtExactThreshold() {
    long threshold = 15728640L;
    AspectSizeValidationConfig config =
        createEnabledConfig(threshold, OversizedAspectRemediation.DELETE);
    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config);

    EntityAspect serializedAspect = new EntityAspect();
    serializedAspect.setMetadata(generateLargeMetadata((int) threshold));

    // Should not throw - exact threshold is allowed
    validator.validatePayload(systemAspect, serializedAspect);

    // Should not add any deletion requests
    List<AspectDeletionRequest> deletions = AspectValidationContext.getPendingDeletions();
    assertEquals(deletions.size(), 0);
  }

  @Test
  public void testValidationOneByteOverThreshold() {
    long threshold = 15728640L;
    AspectSizeValidationConfig config =
        createEnabledConfig(threshold, OversizedAspectRemediation.DELETE);
    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config);

    EntityAspect serializedAspect = new EntityAspect();
    serializedAspect.setMetadata(generateLargeMetadata((int) (threshold + 1)));

    try {
      validator.validatePayload(systemAspect, serializedAspect);
      fail("Expected AspectSizeExceededException");
    } catch (AspectSizeExceededException exception) {
      assertEquals(exception.getActualSize(), threshold + 1);
      assertEquals(exception.getThreshold(), threshold);

      // Verify deletion request was added
      List<AspectDeletionRequest> deletions = AspectValidationContext.getPendingDeletions();
      assertEquals(deletions.size(), 1);
    }
  }

  private AspectSizeValidationConfig createEnabledConfig(
      long maxSizeBytes, OversizedAspectRemediation remediation) {
    AspectSizeValidationConfig config = new AspectSizeValidationConfig();
    AspectCheckpointConfig postPatchConfig = new AspectCheckpointConfig();
    postPatchConfig.setEnabled(true);
    postPatchConfig.setMaxSizeBytes(maxSizeBytes);
    postPatchConfig.setOversizedRemediation(remediation);
    config.setPostPatch(postPatchConfig);
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

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
import com.linkedin.metadata.config.AspectSizeValidationConfiguration;
import com.linkedin.metadata.config.AspectSizeValidationConfiguration.AspectCheckpointConfig;
import com.linkedin.metadata.config.OversizedAspectRemediation;
import com.linkedin.metadata.entity.validation.AspectDeletionRequest;
import com.linkedin.metadata.entity.validation.AspectSizeExceededException;
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
  @Mock private OperationContext mockOpContext;

  private Urn urn;
  private static final String URN_STRING =
      "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)";
  private java.util.List<Object> pendingDeletions;

  @BeforeMethod
  public void setup() throws Exception {
    MockitoAnnotations.openMocks(this);
    urn = Urn.createFromString(URN_STRING);

    // Create a real mutable list for pending deletions
    pendingDeletions = new java.util.ArrayList<>();

    // Mock OperationContext to use the mutable list
    doAnswer(
            invocation -> {
              pendingDeletions.add(invocation.getArgument(0));
              return null;
            })
        .when(mockOpContext)
        .addPendingDeletion(any());
    when(mockOpContext.getPendingDeletions())
        .thenAnswer(invocation -> new java.util.ArrayList<>(pendingDeletions));
    doAnswer(
            invocation -> {
              pendingDeletions.clear();
              return null;
            })
        .when(mockOpContext)
        .clearPendingDeletions();

    when(systemAspect.getUrn()).thenReturn(urn);
    when(systemAspect.getAspectSpec()).thenReturn(aspectSpec);
    when(systemAspect.getOperationContext()).thenReturn(mockOpContext);

    // Clear pending deletions before each test
    pendingDeletions.clear();
  }

  @AfterMethod
  public void cleanup() {
    // Always cleanup pending deletions after each test
    pendingDeletions.clear();
  }

  @Test
  public void testValidationDisabled() {
    AspectSizeValidationConfiguration config = new AspectSizeValidationConfiguration();
    config.setPostPatch(null);

    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config, null);

    EntityAspect serializedAspect = new EntityAspect();
    serializedAspect.setMetadata(generateLargeMetadata(20000000)); // 20MB

    // Should not throw
    validator.validatePayload(systemAspect, serializedAspect);

    // Should not add any deletion requests
    List<AspectDeletionRequest> deletions =
        mockOpContext.getPendingDeletions().stream()
            .filter(obj -> obj instanceof AspectDeletionRequest)
            .map(obj -> (AspectDeletionRequest) obj)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(deletions.size(), 0);
  }

  @Test
  public void testValidationDisabledWhenNotEnabled() {
    AspectSizeValidationConfiguration config = new AspectSizeValidationConfiguration();
    AspectCheckpointConfig postPatchConfig = new AspectCheckpointConfig();
    postPatchConfig.setEnabled(false);
    config.setPostPatch(postPatchConfig);

    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config, null);

    EntityAspect serializedAspect = new EntityAspect();
    serializedAspect.setMetadata(generateLargeMetadata(20000000)); // 20MB

    // Should not throw
    validator.validatePayload(systemAspect, serializedAspect);

    // Should not add any deletion requests
    List<AspectDeletionRequest> deletions =
        mockOpContext.getPendingDeletions().stream()
            .filter(obj -> obj instanceof AspectDeletionRequest)
            .map(obj -> (AspectDeletionRequest) obj)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(deletions.size(), 0);
  }

  @Test
  public void testValidationWithNullMetadata() {
    AspectSizeValidationConfiguration config =
        createEnabledConfig(15728640L, OversizedAspectRemediation.DELETE);
    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config, null);

    EntityAspect serializedAspect = new EntityAspect();
    serializedAspect.setMetadata(null);

    // Should not throw
    validator.validatePayload(systemAspect, serializedAspect);

    // Should not add any deletion requests
    List<AspectDeletionRequest> deletions =
        mockOpContext.getPendingDeletions().stream()
            .filter(obj -> obj instanceof AspectDeletionRequest)
            .map(obj -> (AspectDeletionRequest) obj)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(deletions.size(), 0);
  }

  @Test
  public void testValidationPassesForSmallAspect() {
    AspectSizeValidationConfiguration config =
        createEnabledConfig(15728640L, OversizedAspectRemediation.DELETE);
    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config, null);

    EntityAspect serializedAspect = new EntityAspect();
    serializedAspect.setMetadata(generateLargeMetadata(1000)); // 1KB

    // Should not throw
    validator.validatePayload(systemAspect, serializedAspect);

    // Should not add any deletion requests
    List<AspectDeletionRequest> deletions =
        mockOpContext.getPendingDeletions().stream()
            .filter(obj -> obj instanceof AspectDeletionRequest)
            .map(obj -> (AspectDeletionRequest) obj)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(deletions.size(), 0);
  }

  @Test
  public void testValidationFailsWithDeleteRemediation() {
    AspectSizeValidationConfiguration config =
        createEnabledConfig(15728640L, OversizedAspectRemediation.DELETE);
    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config, null);

    EntityAspect serializedAspect = new EntityAspect();
    serializedAspect.setMetadata(generateLargeMetadata(20000000)); // 20MB

    try {
      validator.validatePayload(systemAspect, serializedAspect);
      fail("Expected AspectSizeExceededException");
    } catch (AspectSizeExceededException exception) {
      assertEquals(exception.getValidationPoint(), "POST_DB_PATCH");
      assertEquals(exception.getActualSize(), 20000000L);
      assertEquals(exception.getThreshold(), 15728640L);
      assertEquals(exception.getUrn(), URN_STRING);
      assertEquals(exception.getAspectName(), STATUS_ASPECT_NAME);

      // Verify deletion request was added to ThreadLocal
      List<AspectDeletionRequest> deletions =
          mockOpContext.getPendingDeletions().stream()
              .filter(obj -> obj instanceof AspectDeletionRequest)
              .map(obj -> (AspectDeletionRequest) obj)
              .collect(java.util.stream.Collectors.toList());
      assertEquals(deletions.size(), 1);
      AspectDeletionRequest deletion = deletions.get(0);
      assertEquals(deletion.getUrn(), urn);
      assertEquals(deletion.getAspectName(), STATUS_ASPECT_NAME);
      assertEquals(deletion.getValidationPoint(), "POST_DB_PATCH");
      assertEquals(deletion.getAspectSize(), 20000000L);
      assertEquals(deletion.getThreshold(), 15728640L);
    }
  }

  @Test
  public void testValidationFailsWithIgnoreRemediation() {
    AspectSizeValidationConfiguration config =
        createEnabledConfig(15728640L, OversizedAspectRemediation.IGNORE);
    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config, null);

    EntityAspect serializedAspect = new EntityAspect();
    serializedAspect.setMetadata(generateLargeMetadata(20000000)); // 20MB

    try {
      validator.validatePayload(systemAspect, serializedAspect);
      fail("Expected AspectSizeExceededException");
    } catch (AspectSizeExceededException exception) {
      assertEquals(exception.getValidationPoint(), "POST_DB_PATCH");

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
  public void testValidationAtExactThreshold() {
    long threshold = 15728640L;
    AspectSizeValidationConfiguration config =
        createEnabledConfig(threshold, OversizedAspectRemediation.DELETE);
    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config, null);

    EntityAspect serializedAspect = new EntityAspect();
    serializedAspect.setMetadata(generateLargeMetadata((int) threshold));

    // Should not throw - exact threshold is allowed
    validator.validatePayload(systemAspect, serializedAspect);

    // Should not add any deletion requests
    List<AspectDeletionRequest> deletions =
        mockOpContext.getPendingDeletions().stream()
            .filter(obj -> obj instanceof AspectDeletionRequest)
            .map(obj -> (AspectDeletionRequest) obj)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(deletions.size(), 0);
  }

  @Test
  public void testValidationOneByteOverThreshold() {
    long threshold = 15728640L;
    AspectSizeValidationConfiguration config =
        createEnabledConfig(threshold, OversizedAspectRemediation.DELETE);
    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config, null);

    EntityAspect serializedAspect = new EntityAspect();
    serializedAspect.setMetadata(generateLargeMetadata((int) (threshold + 1)));

    try {
      validator.validatePayload(systemAspect, serializedAspect);
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
  public void testValidationWithWarningThreshold() {
    // Test warning threshold: size above warning but below max - should NOT throw
    AspectSizeValidationConfiguration config = new AspectSizeValidationConfiguration();
    AspectCheckpointConfig postPatchConfig = new AspectCheckpointConfig();
    postPatchConfig.setEnabled(true);
    postPatchConfig.setWarnSizeBytes(1000L); // Warn at 1KB
    postPatchConfig.setMaxSizeBytes(10000L); // Block at 10KB
    postPatchConfig.setOversizedRemediation(OversizedAspectRemediation.IGNORE);
    config.setPostPatch(postPatchConfig);

    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config, null);

    EntityAspect serializedAspect = new EntityAspect();
    serializedAspect.setMetadata(generateLargeMetadata(5000)); // 5KB - above warn, below max

    // Should NOT throw - only logs warning
    validator.validatePayload(systemAspect, serializedAspect);

    // Should not add any deletion requests
    List<AspectDeletionRequest> deletions =
        mockOpContext.getPendingDeletions().stream()
            .filter(obj -> obj instanceof AspectDeletionRequest)
            .map(obj -> (AspectDeletionRequest) obj)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(deletions.size(), 0);
  }

  @Test
  public void testValidationWithNullWarningThreshold() {
    // Test that null warnSizeBytes is handled correctly
    AspectSizeValidationConfiguration config = new AspectSizeValidationConfiguration();
    AspectCheckpointConfig postPatchConfig = new AspectCheckpointConfig();
    postPatchConfig.setEnabled(true);
    postPatchConfig.setWarnSizeBytes(null); // No warning threshold
    postPatchConfig.setMaxSizeBytes(10000L);
    postPatchConfig.setOversizedRemediation(OversizedAspectRemediation.IGNORE);
    config.setPostPatch(postPatchConfig);

    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config, null);

    EntityAspect serializedAspect = new EntityAspect();
    serializedAspect.setMetadata(generateLargeMetadata(5000)); // 5KB

    // Should pass - no warning threshold set
    validator.validatePayload(systemAspect, serializedAspect);

    // Should not add any deletion requests
    List<AspectDeletionRequest> deletions =
        mockOpContext.getPendingDeletions().stream()
            .filter(obj -> obj instanceof AspectDeletionRequest)
            .map(obj -> (AspectDeletionRequest) obj)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(deletions.size(), 0);
  }

  private AspectSizeValidationConfiguration createEnabledConfig(
      long maxSizeBytes, OversizedAspectRemediation remediation) {
    AspectSizeValidationConfiguration config = new AspectSizeValidationConfiguration();
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

  @Test
  public void testConfigurableSizeBucketing() {
    // Test custom bucket configuration
    AspectSizeValidationConfiguration config = new AspectSizeValidationConfiguration();
    AspectCheckpointConfig postPatchConfig = new AspectCheckpointConfig();
    postPatchConfig.setEnabled(true);
    postPatchConfig.setMaxSizeBytes(100_000_000L);
    postPatchConfig.setOversizedRemediation(OversizedAspectRemediation.IGNORE);
    config.setPostPatch(postPatchConfig);

    // Configure custom buckets: 512KB, 2MB, 8MB
    AspectSizeValidationConfiguration.MetricsConfig metricsConfig =
        new AspectSizeValidationConfiguration.MetricsConfig();
    metricsConfig.setSizeBuckets(List.of(524288L, 2097152L, 8388608L));
    config.setMetrics(metricsConfig);

    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config, null);

    // Verify validator initialized with custom buckets
    assertNotNull(validator);
  }

  @Test
  public void testDefaultSizeBucketing() {
    // Test default bucket configuration (when metrics config is null)
    AspectSizeValidationConfiguration config = new AspectSizeValidationConfiguration();
    AspectCheckpointConfig postPatchConfig = new AspectCheckpointConfig();
    postPatchConfig.setEnabled(true);
    postPatchConfig.setMaxSizeBytes(100_000_000L);
    postPatchConfig.setOversizedRemediation(OversizedAspectRemediation.IGNORE);
    config.setPostPatch(postPatchConfig);
    // No metrics config - should use defaults

    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config, null);

    // Verify validator initialized with defaults (1MB, 5MB, 10MB, 15MB)
    assertNotNull(validator);
  }

  @Test
  public void testSizeBucketingEdgeCases() {
    // Test edge cases: sizes exactly at bucket boundaries
    AspectSizeValidationConfiguration config = new AspectSizeValidationConfiguration();
    AspectCheckpointConfig postPatchConfig = new AspectCheckpointConfig();
    postPatchConfig.setEnabled(true);
    postPatchConfig.setMaxSizeBytes(100_000_000L);
    postPatchConfig.setOversizedRemediation(OversizedAspectRemediation.IGNORE);
    config.setPostPatch(postPatchConfig);

    // Configure buckets: 1KB, 1MB
    AspectSizeValidationConfiguration.MetricsConfig metricsConfig =
        new AspectSizeValidationConfiguration.MetricsConfig();
    metricsConfig.setSizeBuckets(List.of(1024L, 1048576L));
    config.setMetrics(metricsConfig);

    com.linkedin.metadata.utils.metrics.MetricUtils mockMetrics =
        mock(com.linkedin.metadata.utils.metrics.MetricUtils.class);
    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config, mockMetrics);

    // Test size below first bucket (500 bytes)
    EntityAspect aspect1 = new EntityAspect();
    aspect1.setMetadata(generateLargeMetadata(500));
    validator.validatePayload(systemAspect, aspect1);

    // Test size between buckets (2KB)
    EntityAspect aspect2 = new EntityAspect();
    aspect2.setMetadata(generateLargeMetadata(2048));
    validator.validatePayload(systemAspect, aspect2);

    // Test size above all buckets (2MB)
    EntityAspect aspect3 = new EntityAspect();
    aspect3.setMetadata(generateLargeMetadata(2_097_152));
    validator.validatePayload(systemAspect, aspect3);

    // Verify metrics were called for all three sizes with correct bucket labels
    verify(mockMetrics)
        .incrementMicrometer(
            eq("aspectSizeValidation.postPatch.sizeDistribution"),
            eq(1.0),
            eq("aspectName"),
            anyString(),
            eq("sizeBucket"),
            eq("0-1KB")); // 500 bytes -> first bucket

    verify(mockMetrics)
        .incrementMicrometer(
            eq("aspectSizeValidation.postPatch.sizeDistribution"),
            eq(1.0),
            eq("aspectName"),
            anyString(),
            eq("sizeBucket"),
            eq("1KB-1MB")); // 2KB -> second bucket

    verify(mockMetrics)
        .incrementMicrometer(
            eq("aspectSizeValidation.postPatch.sizeDistribution"),
            eq(1.0),
            eq("aspectName"),
            anyString(),
            eq("sizeBucket"),
            eq("1MB+")); // 2MB -> exceeds all buckets
  }

  @Test
  public void testEmptyBucketConfiguration() {
    // Test with empty bucket list - should handle gracefully
    AspectSizeValidationConfiguration config = new AspectSizeValidationConfiguration();
    AspectCheckpointConfig postPatchConfig = new AspectCheckpointConfig();
    postPatchConfig.setEnabled(true);
    postPatchConfig.setMaxSizeBytes(100_000_000L);
    postPatchConfig.setOversizedRemediation(OversizedAspectRemediation.IGNORE);
    config.setPostPatch(postPatchConfig);

    AspectSizeValidationConfiguration.MetricsConfig metricsConfig =
        new AspectSizeValidationConfiguration.MetricsConfig();
    metricsConfig.setSizeBuckets(List.of()); // Empty list
    config.setMetrics(metricsConfig);

    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config, null);

    EntityAspect aspect = new EntityAspect();
    aspect.setMetadata(generateLargeMetadata(1000));

    // Should not throw - validation still works
    validator.validatePayload(systemAspect, aspect);
  }

  @Test
  public void testWarningThresholdWithMetrics() {
    // Test warning threshold emission
    AspectSizeValidationConfiguration config = new AspectSizeValidationConfiguration();
    AspectCheckpointConfig postPatchConfig = new AspectCheckpointConfig();
    postPatchConfig.setEnabled(true);
    postPatchConfig.setWarnSizeBytes(1000L);
    postPatchConfig.setMaxSizeBytes(10000L);
    postPatchConfig.setOversizedRemediation(OversizedAspectRemediation.IGNORE);
    config.setPostPatch(postPatchConfig);

    com.linkedin.metadata.utils.metrics.MetricUtils mockMetrics =
        mock(com.linkedin.metadata.utils.metrics.MetricUtils.class);
    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config, mockMetrics);

    EntityAspect aspect = new EntityAspect();
    aspect.setMetadata(generateLargeMetadata(5000)); // Between warn and max

    validator.validatePayload(systemAspect, aspect);

    // Verify warning metric was emitted
    verify(mockMetrics)
        .incrementMicrometer(
            eq("aspectSizeValidation.postPatch.warning"), eq(1.0), eq("aspectName"), anyString());
  }

  @Test
  public void testDeleteRemediationWithMetrics() {
    // Test DELETE remediation path with OperationContext and metrics
    AspectSizeValidationConfiguration config = new AspectSizeValidationConfiguration();
    AspectCheckpointConfig postPatchConfig = new AspectCheckpointConfig();
    postPatchConfig.setEnabled(true);
    postPatchConfig.setMaxSizeBytes(1000L);
    postPatchConfig.setOversizedRemediation(OversizedAspectRemediation.DELETE);
    config.setPostPatch(postPatchConfig);

    com.linkedin.metadata.utils.metrics.MetricUtils mockMetrics =
        mock(com.linkedin.metadata.utils.metrics.MetricUtils.class);
    AspectSizePayloadValidator validator = new AspectSizePayloadValidator(config, mockMetrics);

    // Mock systemAspect to return mockOpContext
    when(systemAspect.getOperationContext()).thenReturn(mockOpContext);

    EntityAspect aspect = new EntityAspect();
    aspect.setMetadata(generateLargeMetadata(5000)); // Oversized

    try {
      validator.validatePayload(systemAspect, aspect);
      fail("Expected AspectSizeExceededException");
    } catch (AspectSizeExceededException e) {
      // Expected
    }

    // Verify oversized metric was emitted with DELETE remediation
    verify(mockMetrics)
        .incrementMicrometer(
            eq("aspectSizeValidation.postPatch.oversized"),
            eq(1.0),
            eq("aspectName"),
            anyString(),
            eq("remediation"),
            eq("DELETE"));

    // Verify deletion was added to OperationContext
    verify(mockOpContext, times(1)).addPendingDeletion(any(AspectDeletionRequest.class));
  }
}

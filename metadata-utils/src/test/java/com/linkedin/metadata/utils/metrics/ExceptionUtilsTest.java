package com.linkedin.metadata.utils.metrics;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.aspect.plugins.validation.ValidationSubType;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ExceptionUtilsTest {

  @Mock private MetricUtils mockMetricUtils;

  @Mock private EntitySpec mockEntitySpec;

  @Mock private AspectSpec mockAspectSpec;

  private OperationContext testOperationContext;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    testOperationContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void testCollectMetrics_WithEmptyExceptionCollection() {
    // Given
    ValidationExceptionCollection emptyExceptions = ValidationExceptionCollection.newCollection();

    // When
    ValidationExceptionCollection result =
        ExceptionUtils.collectMetrics(mockMetricUtils, emptyExceptions);

    // Then
    assertNotNull(result);
    assertEquals(result, emptyExceptions);
    verifyNoInteractions(mockMetricUtils);
  }

  @Test
  public void testCollectMetrics_WithNullMetricUtils() throws Exception {
    // Given
    ValidationExceptionCollection exceptions = createExceptionCollection();

    // When
    ValidationExceptionCollection result = ExceptionUtils.collectMetrics(null, exceptions);

    // Then
    assertNotNull(result);
    assertEquals(result, exceptions);
    // No NPE should be thrown
  }

  @Test
  public void testCollectMetrics_WithSingleValidationException() throws Exception {
    // Given
    Urn entityUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleDataset,PROD)");
    BatchItem batchItem = createMockBatchItem(entityUrn, "datasetProperties", ChangeType.UPSERT);

    AspectValidationException exception =
        AspectValidationException.forItem(batchItem, "Test validation error");

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    exceptions.addException(exception);

    // When
    ValidationExceptionCollection result =
        ExceptionUtils.collectMetrics(mockMetricUtils, exceptions);

    // Then
    assertNotNull(result);
    assertEquals(result, exceptions);

    // Verify metric increments
    verify(mockMetricUtils).increment("metadata.validation.exception.validation", 1);
    verify(mockMetricUtils).increment("metadata.validation.exception.validation.upsert", 1);
    verify(mockMetricUtils).increment("metadata.validation.exception.validation.dataset", 1);
    verify(mockMetricUtils)
        .increment("metadata.validation.exception.validation.datasetProperties", 1);
  }

  @Test
  public void testCollectMetrics_WithMultipleExceptions() throws Exception {
    // Given
    Urn datasetUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleDataset,PROD)");
    Urn chartUrn = Urn.createFromString("urn:li:chart:(looker,dashboard.1)");

    BatchItem datasetItem1 =
        createMockBatchItem(datasetUrn, "datasetProperties", ChangeType.CREATE);
    BatchItem chartItem = createMockBatchItem(chartUrn, "chartInfo", ChangeType.DELETE);
    BatchItem datasetItem2 = createMockBatchItem(datasetUrn, "schemaMetadata", ChangeType.UPSERT);

    AspectValidationException exception1 =
        AspectValidationException.forPrecondition(datasetItem1, "Precondition failed");

    AspectValidationException exception2 =
        AspectValidationException.forAuth(chartItem, "Authorization failed");

    AspectValidationException exception3 =
        AspectValidationException.forItem(datasetItem2, "Validation failed");

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    exceptions.addException(exception1);
    exceptions.addException(exception2);
    exceptions.addException(exception3);

    // When
    ValidationExceptionCollection result =
        ExceptionUtils.collectMetrics(mockMetricUtils, exceptions);

    // Then
    assertNotNull(result);
    assertEquals(result, exceptions);

    // Verify metrics for exception1 (PRECONDITION)
    verify(mockMetricUtils).increment("metadata.validation.exception.precondition", 3);
    verify(mockMetricUtils).increment("metadata.validation.exception.precondition.create", 1);
    verify(mockMetricUtils).increment("metadata.validation.exception.precondition.dataset", 1);
    verify(mockMetricUtils)
        .increment("metadata.validation.exception.precondition.datasetProperties", 1);

    // Verify metrics for exception2 (AUTHORIZATION)
    verify(mockMetricUtils).increment("metadata.validation.exception.authorization", 3);
    verify(mockMetricUtils).increment("metadata.validation.exception.authorization.delete", 1);
    verify(mockMetricUtils).increment("metadata.validation.exception.authorization.chart", 1);
    verify(mockMetricUtils).increment("metadata.validation.exception.authorization.chartInfo", 1);

    // Verify metrics for exception3 (VALIDATION)
    verify(mockMetricUtils).increment("metadata.validation.exception.validation", 3);
    verify(mockMetricUtils).increment("metadata.validation.exception.validation.upsert", 1);
    verify(mockMetricUtils).increment("metadata.validation.exception.validation.dataset", 1);
    verify(mockMetricUtils).increment("metadata.validation.exception.validation.schemaMetadata", 1);
  }

  @Test
  public void testCollectMetrics_WithAllChangeTypes() throws Exception {
    // Given
    Urn entityUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleDataset,PROD)");

    Arrays.asList(
            ChangeType.CREATE,
            ChangeType.UPSERT,
            ChangeType.DELETE,
            ChangeType.PATCH,
            ChangeType.RESTATE)
        .forEach(
            changeType -> {
              BatchItem batchItem = createMockBatchItem(entityUrn, "testAspect", changeType);
              AspectValidationException exception =
                  AspectValidationException.forItem(batchItem, "Test error for " + changeType);

              ValidationExceptionCollection exceptions =
                  ValidationExceptionCollection.newCollection();
              exceptions.addException(exception);

              // When
              ExceptionUtils.collectMetrics(mockMetricUtils, exceptions);

              // Then
              verify(mockMetricUtils)
                  .increment(
                      "metadata.validation.exception.validation."
                          + changeType.toString().toLowerCase(),
                      1);
            });
  }

  @Test
  public void testCollectMetrics_WithAllValidationSubTypes() throws Exception {
    // Given
    Urn entityUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleDataset,PROD)");

    // Create different batch items to ensure they map to different keys in the collection
    BatchItem batchItem1 = createMockBatchItem(entityUrn, "testAspect1", ChangeType.UPSERT);
    BatchItem batchItem2 = createMockBatchItem(entityUrn, "testAspect2", ChangeType.UPSERT);
    BatchItem batchItem3 = createMockBatchItem(entityUrn, "testAspect3", ChangeType.UPSERT);
    BatchItem batchItem4 = createMockBatchItem(entityUrn, "testAspect4", ChangeType.UPSERT);

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    // Add VALIDATION exception
    exceptions.addException(AspectValidationException.forItem(batchItem1, "Validation error"));

    // Add PRECONDITION exception
    exceptions.addException(
        AspectValidationException.forPrecondition(batchItem2, "Precondition error"));

    // Add FILTER exception
    exceptions.addException(AspectValidationException.forFilter(batchItem3, "Filter error"));

    // Add AUTHORIZATION exception
    exceptions.addException(AspectValidationException.forAuth(batchItem4, "Auth error"));

    // When
    ExceptionUtils.collectMetrics(mockMetricUtils, exceptions);

    // Then
    // The ValidationExceptionCollection extends HashMap, so exceptions.size() returns the number of
    // unique Pair<Urn,String> keys
    // Since all 4 exceptions have different aspects, there are 4 unique keys, so exceptions.size()
    // = 4
    verify(mockMetricUtils).increment("metadata.validation.exception.validation", 4);
    verify(mockMetricUtils).increment("metadata.validation.exception.precondition", 4);
    verify(mockMetricUtils).increment("metadata.validation.exception.filter", 4);
    verify(mockMetricUtils).increment("metadata.validation.exception.authorization", 4);

    // Each exception also increments its own changeType metric
    verify(mockMetricUtils).increment("metadata.validation.exception.validation.upsert", 1);
    verify(mockMetricUtils).increment("metadata.validation.exception.precondition.upsert", 1);
    verify(mockMetricUtils).increment("metadata.validation.exception.filter.upsert", 1);
    verify(mockMetricUtils).increment("metadata.validation.exception.authorization.upsert", 1);

    // Each exception also increments its own entityType metric
    verify(mockMetricUtils).increment("metadata.validation.exception.validation.dataset", 1);
    verify(mockMetricUtils).increment("metadata.validation.exception.precondition.dataset", 1);
    verify(mockMetricUtils).increment("metadata.validation.exception.filter.dataset", 1);
    verify(mockMetricUtils).increment("metadata.validation.exception.authorization.dataset", 1);

    // Each exception also increments its own aspectName metric
    verify(mockMetricUtils).increment("metadata.validation.exception.validation.testAspect1", 1);
    verify(mockMetricUtils).increment("metadata.validation.exception.precondition.testAspect2", 1);
    verify(mockMetricUtils).increment("metadata.validation.exception.filter.testAspect3", 1);
    verify(mockMetricUtils).increment("metadata.validation.exception.authorization.testAspect4", 1);
  }

  @Test
  public void testCollectMetrics_WithSpecialCharactersInAspectName() throws Exception {
    // Given
    Urn entityUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleDataset,PROD)");
    String aspectNameWithSpecialChars = "aspect_name-with.special$chars";
    BatchItem batchItem =
        createMockBatchItem(entityUrn, aspectNameWithSpecialChars, ChangeType.UPSERT);

    AspectValidationException exception =
        AspectValidationException.forItem(batchItem, "Test error");

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    exceptions.addException(exception);

    // When
    ValidationExceptionCollection result =
        ExceptionUtils.collectMetrics(mockMetricUtils, exceptions);

    // Then
    assertNotNull(result);
    verify(mockMetricUtils)
        .increment("metadata.validation.exception.validation." + aspectNameWithSpecialChars, 1);
  }

  @Test
  public void testCollectMetrics_VerifyExceptionCollectionUnmodified() throws Exception {
    // Given
    Urn entityUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleDataset,PROD)");
    BatchItem batchItem = createMockBatchItem(entityUrn, "datasetProperties", ChangeType.UPSERT);

    AspectValidationException exception =
        AspectValidationException.forItem(batchItem, "Test error");

    ValidationExceptionCollection originalExceptions =
        ValidationExceptionCollection.newCollection();
    originalExceptions.addException(exception);
    long originalSize = originalExceptions.streamAllExceptions().count();

    // When
    ValidationExceptionCollection result =
        ExceptionUtils.collectMetrics(mockMetricUtils, originalExceptions);

    // Then
    assertSame(result, originalExceptions);
    assertEquals(originalExceptions.streamAllExceptions().count(), originalSize);
    assertTrue(originalExceptions.hasFatalExceptions());
    assertEquals(originalExceptions.getSubTypes().size(), 1);
    assertTrue(originalExceptions.getSubTypes().contains(ValidationSubType.VALIDATION));
  }

  @Test
  public void testCollectMetrics_WithDifferentEntityTypes() throws Exception {
    // Given
    Urn datasetUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleDataset,PROD)");
    Urn chartUrn = Urn.createFromString("urn:li:chart:(looker,dashboard.1)");
    Urn dashboardUrn = Urn.createFromString("urn:li:dashboard:(looker,dashboards.1)");
    Urn dataFlowUrn = Urn.createFromString("urn:li:dataFlow:(airflow,dag_abc,PROD)");

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    // Add exceptions for different entity types
    exceptions.addException(
        AspectValidationException.forItem(
            createMockBatchItem(datasetUrn, "datasetProperties", ChangeType.UPSERT),
            "Dataset error"));
    exceptions.addException(
        AspectValidationException.forItem(
            createMockBatchItem(chartUrn, "chartInfo", ChangeType.UPSERT), "Chart error"));
    exceptions.addException(
        AspectValidationException.forItem(
            createMockBatchItem(dashboardUrn, "dashboardInfo", ChangeType.UPSERT),
            "Dashboard error"));
    exceptions.addException(
        AspectValidationException.forItem(
            createMockBatchItem(dataFlowUrn, "dataFlowInfo", ChangeType.UPSERT), "DataFlow error"));

    // When
    ExceptionUtils.collectMetrics(mockMetricUtils, exceptions);

    // Then
    verify(mockMetricUtils).increment("metadata.validation.exception.validation.dataset", 1);
    verify(mockMetricUtils).increment("metadata.validation.exception.validation.chart", 1);
    verify(mockMetricUtils).increment("metadata.validation.exception.validation.dashboard", 1);
    verify(mockMetricUtils).increment("metadata.validation.exception.validation.dataflow", 1);
  }

  // Helper method to create a mock BatchItem
  private BatchItem createMockBatchItem(Urn urn, String aspectName, ChangeType changeType) {
    BatchItem mockItem = mock(BatchItem.class);
    when(mockItem.getUrn()).thenReturn(urn);
    when(mockItem.getAspectName()).thenReturn(aspectName);
    when(mockItem.getChangeType()).thenReturn(changeType);
    when(mockItem.getEntitySpec()).thenReturn(mockEntitySpec);
    when(mockItem.getAspectSpec()).thenReturn(mockAspectSpec);
    return mockItem;
  }

  // Helper method to create a sample exception collection
  private ValidationExceptionCollection createExceptionCollection() throws Exception {
    Urn entityUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,SampleDataset,PROD)");
    BatchItem batchItem = createMockBatchItem(entityUrn, "datasetProperties", ChangeType.UPSERT);

    AspectValidationException exception =
        AspectValidationException.forItem(batchItem, "Sample error");

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    exceptions.addException(exception);
    return exceptions;
  }
}

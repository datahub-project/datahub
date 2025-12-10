package com.linkedin.metadata.aspect.validation;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.CustomAssertionInfo;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests that verify the UrnAnnotationValidator correctly validates URN fields in assertion aspects.
 * These tests use real assertion record templates with the actual AspectSpec from the entity
 * registry.
 *
 * <p>Note: All assertion aspects have exist=false (entity type validation only).
 */
public class AssertionUrnValidationAnnotationTest {

  private static final EntityRegistry ENTITY_REGISTRY =
      TestOperationContexts.defaultEntityRegistry();
  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(UrnAnnotationValidator.class.getName())
          .enabled(true)
          .supportedOperations(List.of("UPSERT"))
          .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
          .build();

  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test-assertion");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,testDataset,PROD)");
  private static final Urn TEST_DATAJOB_URN =
      UrnUtils.getUrn("urn:li:dataJob:(urn:li:dataFlow:(airflow,test_dag,PROD),test_task)");
  private static final Urn TEST_SCHEMA_FIELD_URN =
      UrnUtils.getUrn(
          "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,testDataset,PROD),fieldName)");
  private static final Urn INVALID_ENTITY_TYPE_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");

  @Mock private BatchItem mockBatchItem;
  @Mock private RetrieverContext mockRetrieverContext;
  @Mock private AspectRetriever mockAspectRetriever;

  private UrnAnnotationValidator validator;
  private EntitySpec assertionEntitySpec;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    validator = new UrnAnnotationValidator();
    validator.setConfig(TEST_PLUGIN_CONFIG);
    assertionEntitySpec = ENTITY_REGISTRY.getEntitySpec("assertion");

    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(ENTITY_REGISTRY);
    // Default: no entities exist (tests override as needed)
    when(mockAspectRetriever.entityExists(any())).thenReturn(Collections.emptyMap());
  }

  // ==================== DatasetAssertionInfo Tests ====================

  @Test
  public void testDatasetAssertionInfo_ValidDatasetUrn() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.DATASET);
    DatasetAssertionInfo datasetAssertion = new DatasetAssertionInfo();
    datasetAssertion.setDataset(TEST_DATASET_URN);
    datasetAssertion.setScope(DatasetAssertionScope.DATASET_ROWS);
    datasetAssertion.setOperator(AssertionStdOperator.EQUAL_TO);
    assertionInfo.setDatasetAssertion(datasetAssertion);

    setupMockBatchItem(assertionInfo, "assertionInfo");

    List<AspectValidationException> exceptions = runValidation();
    assertTrue(exceptions.isEmpty(), "No exceptions expected for valid dataset URN");
  }

  @Test
  public void testDatasetAssertionInfo_InvalidEntityType() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.DATASET);
    DatasetAssertionInfo datasetAssertion = new DatasetAssertionInfo();
    datasetAssertion.setDataset(INVALID_ENTITY_TYPE_URN); // corpuser instead of dataset
    datasetAssertion.setScope(DatasetAssertionScope.DATASET_ROWS);
    datasetAssertion.setOperator(AssertionStdOperator.EQUAL_TO);
    assertionInfo.setDatasetAssertion(datasetAssertion);

    setupMockBatchItem(assertionInfo, "assertionInfo");

    List<AspectValidationException> exceptions = runValidation();
    assertFalse(exceptions.isEmpty(), "Should fail for invalid entity type");
    assertTrue(
        exceptions.get(0).getMessage().contains("Invalid entity type"),
        "Error should indicate invalid entity type");
  }

  @Test
  public void testDatasetAssertionInfo_ValidSchemaFieldUrns() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.DATASET);
    DatasetAssertionInfo datasetAssertion = new DatasetAssertionInfo();
    datasetAssertion.setDataset(TEST_DATASET_URN);
    datasetAssertion.setScope(DatasetAssertionScope.DATASET_COLUMN);
    datasetAssertion.setOperator(AssertionStdOperator.EQUAL_TO);
    datasetAssertion.setFields(new UrnArray(TEST_SCHEMA_FIELD_URN));
    assertionInfo.setDatasetAssertion(datasetAssertion);

    setupMockBatchItem(assertionInfo, "assertionInfo");

    List<AspectValidationException> exceptions = runValidation();
    assertTrue(exceptions.isEmpty(), "No exceptions for valid schemaField URNs");
  }

  @Test
  public void testDatasetAssertionInfo_InvalidSchemaFieldEntityType() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.DATASET);
    DatasetAssertionInfo datasetAssertion = new DatasetAssertionInfo();
    datasetAssertion.setDataset(TEST_DATASET_URN);
    datasetAssertion.setScope(DatasetAssertionScope.DATASET_COLUMN);
    datasetAssertion.setOperator(AssertionStdOperator.EQUAL_TO);
    datasetAssertion.setFields(new UrnArray(TEST_DATASET_URN)); // dataset instead of schemaField
    assertionInfo.setDatasetAssertion(datasetAssertion);

    setupMockBatchItem(assertionInfo, "assertionInfo");

    List<AspectValidationException> exceptions = runValidation();
    assertFalse(exceptions.isEmpty(), "Should fail for invalid schemaField entity type");
    assertTrue(exceptions.get(0).getMessage().contains("Invalid entity type"));
  }

  // ==================== CustomAssertionInfo Tests ====================

  @Test
  public void testCustomAssertionInfo_ValidUrns() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.CUSTOM);
    CustomAssertionInfo customAssertion = new CustomAssertionInfo();
    customAssertion.setType("custom-type");
    customAssertion.setEntity(TEST_DATASET_URN);
    customAssertion.setField(TEST_SCHEMA_FIELD_URN);
    assertionInfo.setCustomAssertion(customAssertion);

    setupMockBatchItem(assertionInfo, "assertionInfo");

    List<AspectValidationException> exceptions = runValidation();
    assertTrue(exceptions.isEmpty(), "No exceptions for valid custom assertion URNs");
  }

  @Test
  public void testCustomAssertionInfo_InvalidEntityType_ForEntity() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.CUSTOM);
    CustomAssertionInfo customAssertion = new CustomAssertionInfo();
    customAssertion.setType("custom-type");
    customAssertion.setEntity(INVALID_ENTITY_TYPE_URN); // corpuser instead of dataset
    assertionInfo.setCustomAssertion(customAssertion);

    setupMockBatchItem(assertionInfo, "assertionInfo");

    List<AspectValidationException> exceptions = runValidation();
    assertFalse(exceptions.isEmpty(), "Should fail for invalid entity type on entity field");
    assertTrue(exceptions.get(0).getMessage().contains("Invalid entity type"));
  }

  @Test
  public void testCustomAssertionInfo_InvalidEntityType_ForField() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.CUSTOM);
    CustomAssertionInfo customAssertion = new CustomAssertionInfo();
    customAssertion.setType("custom-type");
    customAssertion.setEntity(TEST_DATASET_URN);
    customAssertion.setField(TEST_DATASET_URN); // dataset instead of schemaField
    assertionInfo.setCustomAssertion(customAssertion);

    setupMockBatchItem(assertionInfo, "assertionInfo");

    List<AspectValidationException> exceptions = runValidation();
    assertFalse(exceptions.isEmpty(), "Should fail for invalid entity type on field");
    assertTrue(exceptions.get(0).getMessage().contains("Invalid entity type"));
  }

  // ==================== FreshnessAssertionInfo Tests ====================

  @Test
  public void testFreshnessAssertionInfo_ValidDatasetUrn() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.FRESHNESS);
    FreshnessAssertionInfo freshnessAssertion = new FreshnessAssertionInfo();
    freshnessAssertion.setType(FreshnessAssertionType.DATASET_CHANGE);
    freshnessAssertion.setEntity(TEST_DATASET_URN);
    freshnessAssertion.setSchedule(
        new FreshnessAssertionSchedule().setType(FreshnessAssertionScheduleType.FIXED_INTERVAL));
    assertionInfo.setFreshnessAssertion(freshnessAssertion);

    setupMockBatchItem(assertionInfo, "assertionInfo");

    List<AspectValidationException> exceptions = runValidation();
    assertTrue(exceptions.isEmpty(), "No exceptions for valid dataset URN in freshness assertion");
  }

  @Test
  public void testFreshnessAssertionInfo_ValidDataJobUrn() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.FRESHNESS);
    FreshnessAssertionInfo freshnessAssertion = new FreshnessAssertionInfo();
    freshnessAssertion.setType(FreshnessAssertionType.DATA_JOB_RUN);
    freshnessAssertion.setEntity(TEST_DATAJOB_URN);
    freshnessAssertion.setSchedule(
        new FreshnessAssertionSchedule().setType(FreshnessAssertionScheduleType.FIXED_INTERVAL));
    assertionInfo.setFreshnessAssertion(freshnessAssertion);

    setupMockBatchItem(assertionInfo, "assertionInfo");

    List<AspectValidationException> exceptions = runValidation();
    assertTrue(exceptions.isEmpty(), "No exceptions for valid dataJob URN in freshness assertion");
  }

  @Test
  public void testFreshnessAssertionInfo_InvalidEntityType() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.FRESHNESS);
    FreshnessAssertionInfo freshnessAssertion = new FreshnessAssertionInfo();
    freshnessAssertion.setType(FreshnessAssertionType.DATASET_CHANGE);
    freshnessAssertion.setEntity(INVALID_ENTITY_TYPE_URN); // corpuser not allowed
    freshnessAssertion.setSchedule(
        new FreshnessAssertionSchedule().setType(FreshnessAssertionScheduleType.FIXED_INTERVAL));
    assertionInfo.setFreshnessAssertion(freshnessAssertion);

    setupMockBatchItem(assertionInfo, "assertionInfo");

    List<AspectValidationException> exceptions = runValidation();
    assertFalse(exceptions.isEmpty(), "Should fail for invalid entity type");
    assertTrue(exceptions.get(0).getMessage().contains("Invalid entity type"));
  }

  // ==================== AssertionRunEvent Tests (exist=false, entity type validation only)
  // ====================

  @Test
  public void testAssertionRunEvent_ValidUrns() {
    AssertionRunEvent runEvent = new AssertionRunEvent();
    runEvent.setTimestampMillis(System.currentTimeMillis());
    runEvent.setRunId("test-run-id");
    runEvent.setAsserteeUrn(TEST_DATASET_URN);
    runEvent.setAssertionUrn(TEST_ASSERTION_URN);
    runEvent.setStatus(AssertionRunStatus.COMPLETE);

    setupMockBatchItem(runEvent, "assertionRunEvent");

    List<AspectValidationException> exceptions = runValidation();
    assertTrue(exceptions.isEmpty(), "No exceptions for valid URN entity types");
  }

  @Test
  public void testAssertionRunEvent_InvalidAsserteeEntityType() {
    AssertionRunEvent runEvent = new AssertionRunEvent();
    runEvent.setTimestampMillis(System.currentTimeMillis());
    runEvent.setRunId("test-run-id");
    runEvent.setAsserteeUrn(INVALID_ENTITY_TYPE_URN); // corpuser not allowed
    runEvent.setAssertionUrn(TEST_ASSERTION_URN);
    runEvent.setStatus(AssertionRunStatus.COMPLETE);

    setupMockBatchItem(runEvent, "assertionRunEvent");

    List<AspectValidationException> exceptions = runValidation();
    assertFalse(exceptions.isEmpty(), "Should fail for invalid assertee entity type");
    assertTrue(exceptions.get(0).getMessage().contains("Invalid entity type"));
  }

  @Test
  public void testAssertionRunEvent_InvalidAssertionEntityType() {
    AssertionRunEvent runEvent = new AssertionRunEvent();
    runEvent.setTimestampMillis(System.currentTimeMillis());
    runEvent.setRunId("test-run-id");
    runEvent.setAsserteeUrn(TEST_DATASET_URN);
    runEvent.setAssertionUrn(TEST_DATASET_URN); // dataset instead of assertion
    runEvent.setStatus(AssertionRunStatus.COMPLETE);

    setupMockBatchItem(runEvent, "assertionRunEvent");

    List<AspectValidationException> exceptions = runValidation();
    assertFalse(exceptions.isEmpty(), "Should fail for invalid assertion entity type");
    assertTrue(exceptions.get(0).getMessage().contains("Invalid entity type"));
  }

  @Test
  public void testAssertionRunEvent_DataJobAsAssertee_InvalidEntityType() {
    AssertionRunEvent runEvent = new AssertionRunEvent();
    runEvent.setTimestampMillis(System.currentTimeMillis());
    runEvent.setRunId("test-run-id");
    runEvent.setAsserteeUrn(TEST_DATAJOB_URN); // dataJob not allowed, only dataset
    runEvent.setAssertionUrn(TEST_ASSERTION_URN);
    runEvent.setStatus(AssertionRunStatus.COMPLETE);

    setupMockBatchItem(runEvent, "assertionRunEvent");

    List<AspectValidationException> exceptions = runValidation();
    assertFalse(exceptions.isEmpty(), "DataJob should not be valid as assertee");
    assertTrue(exceptions.get(0).getMessage().contains("Invalid entity type"));
  }

  // ==================== Helper Methods ====================

  private void setupMockBatchItem(
      com.linkedin.data.template.RecordTemplate recordTemplate, String aspectName) {
    when(mockBatchItem.getAspectSpec()).thenReturn(assertionEntitySpec.getAspectSpec(aspectName));
    when(mockBatchItem.getRecordTemplate()).thenReturn(recordTemplate);
  }

  private void mockEntityExists(Map<Urn, Boolean> existenceMap) {
    when(mockAspectRetriever.entityExists(any()))
        .thenAnswer(
            invocation -> {
              Set<Urn> requestedUrns = invocation.getArgument(0);
              Map<Urn, Boolean> result = new HashMap<>();
              for (Urn urn : requestedUrns) {
                result.put(urn, existenceMap.getOrDefault(urn, false));
              }
              return result;
            });
  }

  private List<AspectValidationException> runValidation() {
    Stream<AspectValidationException> result =
        validator.validateProposedAspects(
            Collections.singletonList(mockBatchItem), mockRetrieverContext);
    return result.collect(Collectors.toList());
  }
}

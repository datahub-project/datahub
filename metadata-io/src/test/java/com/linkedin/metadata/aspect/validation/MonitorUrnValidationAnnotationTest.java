package com.linkedin.metadata.aspect.validation;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.anomaly.AnomalySource;
import com.linkedin.anomaly.AnomalySourceType;
import com.linkedin.anomaly.MonitorAnomalyEvent;
import com.linkedin.common.CronSchedule;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.key.MonitorKey;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests that verify the UrnAnnotationValidator correctly validates URN fields in monitor-related
 * aspects. These tests use real record templates with the actual AspectSpec from the entity
 * registry.
 *
 * <p>Monitor-related URN fields have exist=false (entity type validation only, no existence
 * checks).
 */
public class MonitorUrnValidationAnnotationTest {

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
  private static final Urn INVALID_ENTITY_TYPE_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");

  @Mock private BatchItem mockBatchItem;
  @Mock private RetrieverContext mockRetrieverContext;
  @Mock private AspectRetriever mockAspectRetriever;

  private UrnAnnotationValidator validator;
  private EntitySpec monitorEntitySpec;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    validator = new UrnAnnotationValidator();
    validator.setConfig(TEST_PLUGIN_CONFIG);
    monitorEntitySpec = ENTITY_REGISTRY.getEntitySpec("monitor");

    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(ENTITY_REGISTRY);
    // Default: no entities exist (tests override as needed)
    when(mockAspectRetriever.entityExists(any())).thenReturn(Collections.emptyMap());
  }

  // ==================== MonitorKey Tests ====================

  @Test
  public void testMonitorKey_ValidDatasetUrn() {
    MonitorKey monitorKey = new MonitorKey();
    monitorKey.setEntity(TEST_DATASET_URN);
    monitorKey.setId("test-monitor-id");

    setupMockBatchItem(monitorKey, "monitorKey", monitorEntitySpec);

    List<AspectValidationException> exceptions = runValidation();
    assertTrue(exceptions.isEmpty(), "No exceptions for valid dataset URN");
  }

  @Test
  public void testMonitorKey_InvalidEntityType() {
    MonitorKey monitorKey = new MonitorKey();
    monitorKey.setEntity(INVALID_ENTITY_TYPE_URN); // corpuser instead of dataset
    monitorKey.setId("test-monitor-id");

    setupMockBatchItem(monitorKey, "monitorKey", monitorEntitySpec);

    List<AspectValidationException> exceptions = runValidation();
    assertFalse(exceptions.isEmpty(), "Should fail for invalid entity type");
    assertTrue(exceptions.get(0).getMessage().contains("Invalid entity type"));
  }

  // ==================== MonitorInfo with AssertionEvaluationSpec Tests ====================

  @Test
  public void testMonitorInfo_ValidAssertionUrn() {
    MonitorInfo monitorInfo = createMonitorInfo(TEST_ASSERTION_URN);

    setupMockBatchItem(monitorInfo, "monitorInfo", monitorEntitySpec);

    List<AspectValidationException> exceptions = runValidation();
    assertTrue(exceptions.isEmpty(), "No exceptions for valid assertion URN");
  }

  @Test
  public void testMonitorInfo_InvalidAssertionEntityType() {
    MonitorInfo monitorInfo = createMonitorInfo(INVALID_ENTITY_TYPE_URN); // corpuser instead

    setupMockBatchItem(monitorInfo, "monitorInfo", monitorEntitySpec);

    List<AspectValidationException> exceptions = runValidation();
    assertFalse(exceptions.isEmpty(), "Should fail for invalid entity type");
    assertTrue(exceptions.get(0).getMessage().contains("Invalid entity type"));
  }

  // ==================== MonitorAnomalyEvent with AnomalySource Tests ====================

  @Test
  public void testMonitorAnomalyEvent_ValidSourceUrn() {
    MonitorAnomalyEvent event = createMonitorAnomalyEvent(TEST_ASSERTION_URN);

    setupMockBatchItem(event, "monitorAnomalyEvent", monitorEntitySpec);

    List<AspectValidationException> exceptions = runValidation();
    assertTrue(exceptions.isEmpty(), "No exceptions for valid source URN");
  }

  @Test
  public void testMonitorAnomalyEvent_InvalidSourceUrnEntityType() {
    MonitorAnomalyEvent event = createMonitorAnomalyEvent(TEST_DATASET_URN); // dataset instead

    setupMockBatchItem(event, "monitorAnomalyEvent", monitorEntitySpec);

    List<AspectValidationException> exceptions = runValidation();
    assertFalse(exceptions.isEmpty(), "Should fail for invalid entity type");
    assertTrue(exceptions.get(0).getMessage().contains("Invalid entity type"));
  }

  @Test
  public void testMonitorAnomalyEvent_NullSourceUrn_ShouldPass() {
    MonitorAnomalyEvent event = new MonitorAnomalyEvent();
    event.setTimestampMillis(System.currentTimeMillis());
    AnomalySource source = new AnomalySource();
    source.setType(AnomalySourceType.USER_FEEDBACK);
    // sourceUrn is optional, not set
    event.setSource(source);
    event.setCreated(new TimeStamp().setTime(System.currentTimeMillis()));
    event.setLastUpdated(new TimeStamp().setTime(System.currentTimeMillis()));

    setupMockBatchItem(event, "monitorAnomalyEvent", monitorEntitySpec);

    List<AspectValidationException> exceptions = runValidation();
    assertTrue(exceptions.isEmpty(), "Should pass when optional sourceUrn is null");
  }

  // ==================== Helper Methods ====================

  private MonitorInfo createMonitorInfo(Urn assertionUrn) {
    MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.ASSERTION);
    monitorInfo.setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE));

    AssertionEvaluationSpec evalSpec = new AssertionEvaluationSpec();
    evalSpec.setAssertion(assertionUrn);
    evalSpec.setSchedule(new CronSchedule().setCron("0 0 * * *").setTimezone("UTC"));

    AssertionMonitor assertionMonitor = new AssertionMonitor();
    assertionMonitor.setAssertions(new AssertionEvaluationSpecArray(evalSpec));
    monitorInfo.setAssertionMonitor(assertionMonitor);

    return monitorInfo;
  }

  private MonitorAnomalyEvent createMonitorAnomalyEvent(Urn sourceUrn) {
    MonitorAnomalyEvent event = new MonitorAnomalyEvent();
    event.setTimestampMillis(System.currentTimeMillis());
    AnomalySource source = new AnomalySource();
    source.setType(AnomalySourceType.INFERRED_ASSERTION_FAILURE);
    source.setSourceUrn(sourceUrn);
    event.setSource(source);
    event.setCreated(new TimeStamp().setTime(System.currentTimeMillis()));
    event.setLastUpdated(new TimeStamp().setTime(System.currentTimeMillis()));
    return event;
  }

  private void setupMockBatchItem(
      com.linkedin.data.template.RecordTemplate recordTemplate,
      String aspectName,
      EntitySpec entitySpec) {
    when(mockBatchItem.getAspectSpec()).thenReturn(entitySpec.getAspectSpec(aspectName));
    when(mockBatchItem.getRecordTemplate()).thenReturn(recordTemplate);
  }

  private List<AspectValidationException> runValidation() {
    Stream<AspectValidationException> result =
        validator.validateProposedAspects(
            Collections.singletonList(mockBatchItem), mockRetrieverContext);
    return result.collect(Collectors.toList());
  }
}

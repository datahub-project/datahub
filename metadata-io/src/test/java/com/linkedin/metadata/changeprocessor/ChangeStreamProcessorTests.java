package com.linkedin.metadata.changeprocessor;

import com.linkedin.data.template.RecordTemplate;
import org.mockito.InOrder;
import org.mockito.internal.verification.VerificationModeFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;


public class ChangeStreamProcessorTests {

  private final String _testEntityName = "dataset";
  private final String _testAspectName = "datasetProperties";
  private ChangeProcessor _mockChangeProcessor;
  private RecordTemplate _mockRecordTemplate;
  private ChangeStreamProcessor _changeStreamProcessor;

  @BeforeMethod
  public void setupTest() {
    _mockChangeProcessor = mock(ChangeProcessor.class);
    _changeStreamProcessor = new ChangeStreamProcessor();
    _mockRecordTemplate = mock(RecordTemplate.class);
  }

  @Test
  public void testProcessorIsNotCalledForUnregisteredEntityAspects() {
    when(_mockChangeProcessor.process(_testEntityName, _testAspectName, null, _mockRecordTemplate)).thenReturn(
        ProcessChangeResult.success(_mockRecordTemplate));

    _changeStreamProcessor.addBeforeChangeProcessor(_testEntityName, _testAspectName, _mockChangeProcessor);

    ProcessChangeResult processChangeResult =
        _changeStreamProcessor.processBeforeChange(_testEntityName, "anything", null, _mockRecordTemplate);

    verify(_mockChangeProcessor, times(0)).process(_testEntityName, _testAspectName, null, _mockRecordTemplate);
    assertEquals(ProcessChangeResult.success(_mockRecordTemplate), processChangeResult);
    verifyNoMoreInteractions(_mockChangeProcessor);
  }

  @Test
  public void testProcessorIsCalledForSpecificEntityOnAspect() {
    when(_mockChangeProcessor.process(_testEntityName, _testAspectName, null, _mockRecordTemplate)).thenReturn(
        ProcessChangeResult.success(_mockRecordTemplate));
    _changeStreamProcessor.addBeforeChangeProcessor(_testEntityName, _testAspectName, _mockChangeProcessor);

    _changeStreamProcessor.processBeforeChange(_testEntityName, _testAspectName, null, _mockRecordTemplate);

    verify(_mockChangeProcessor).process(_testEntityName, _testAspectName, null, _mockRecordTemplate);
    verifyNoMoreInteractions(_mockChangeProcessor);
  }

  @Test
  public void testProcessorIsCalledForAllAspectsOnEntity() {
    when(_mockChangeProcessor.process(eq(_testEntityName), anyString(), eq(null), eq(_mockRecordTemplate))).thenReturn(
        ProcessChangeResult.success(_mockRecordTemplate));

    _changeStreamProcessor.addBeforeChangeProcessor(_testEntityName, "*", _mockChangeProcessor);

    _changeStreamProcessor.processBeforeChange(_testEntityName, "anything", null, _mockRecordTemplate);

    verify(_mockChangeProcessor).process(_testEntityName, "anything", null, _mockRecordTemplate);
    verifyNoMoreInteractions(_mockChangeProcessor);
  }

  @Test
  public void testProcessorIsCalledForAllAspectsForAllEntities() {
    when(_mockChangeProcessor.process(eq(_testEntityName), anyString(), eq(null), eq(_mockRecordTemplate))).thenReturn(
        ProcessChangeResult.success(_mockRecordTemplate));

    _changeStreamProcessor.addBeforeChangeProcessor("*", "*", _mockChangeProcessor);

    _changeStreamProcessor.processBeforeChange(_testEntityName, "anything", null, _mockRecordTemplate);

    verify(_mockChangeProcessor).process(_testEntityName, "anything", null, _mockRecordTemplate);
    verifyNoMoreInteractions(_mockChangeProcessor);
  }

  @Test
  public void testProcessorsAreCalledInPriorityOrder() {
    when(_mockChangeProcessor.getPriority()).thenReturn(1);
    when(_mockChangeProcessor.process(eq(_testEntityName), anyString(), eq(null), eq(_mockRecordTemplate))).thenReturn(
        ProcessChangeResult.success(_mockRecordTemplate));

    ChangeProcessor secondProcessor = mock(ChangeProcessor.class);
    when(secondProcessor.getPriority()).thenReturn(10);
    when(secondProcessor.process(eq(_testEntityName), anyString(), eq(null), eq(_mockRecordTemplate))).thenReturn(
        ProcessChangeResult.success(_mockRecordTemplate));

    _changeStreamProcessor.addBeforeChangeProcessor("*", "*", secondProcessor);
    _changeStreamProcessor.addBeforeChangeProcessor("*", "*", _mockChangeProcessor);

    _changeStreamProcessor.processBeforeChange(_testEntityName, "anything", null, _mockRecordTemplate);

    InOrder inOrder = inOrder(_mockChangeProcessor, secondProcessor);
    inOrder.verify(_mockChangeProcessor).process(_testEntityName, "anything", null, _mockRecordTemplate);
    inOrder.verify(secondProcessor).process(_testEntityName, "anything", null, _mockRecordTemplate);
  }
}

package com.linkedin.metadata.changeprocessor;

import com.linkedin.data.template.RecordTemplate;
import org.mockito.InOrder;
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
        ChangeResult.success(_mockRecordTemplate));

    _changeStreamProcessor.registerPreProcessor(_testEntityName, _testAspectName, _mockChangeProcessor);

    ChangeResult changeResult =
        _changeStreamProcessor.preProcessChange(_testEntityName, "anything", null, _mockRecordTemplate);

    verify(_mockChangeProcessor, times(0)).process(_testEntityName, _testAspectName, null, _mockRecordTemplate);
    verify(_mockChangeProcessor, times(2)).getPriority();
    assertEquals(ChangeResult.success(_mockRecordTemplate), changeResult);
    verifyNoMoreInteractions(_mockChangeProcessor);
  }

  @Test
  public void testProcessorIsCalledForSpecificEntityOnAspect() {
    when(_mockChangeProcessor.process(_testEntityName, _testAspectName, null, _mockRecordTemplate)).thenReturn(
        ChangeResult.success(_mockRecordTemplate));
    _changeStreamProcessor.registerPreProcessor(_testEntityName, _testAspectName, _mockChangeProcessor);

    _changeStreamProcessor.preProcessChange(_testEntityName, _testAspectName, null, _mockRecordTemplate);

    verify(_mockChangeProcessor).process(_testEntityName, _testAspectName, null, _mockRecordTemplate);
    verify(_mockChangeProcessor, times(2)).getPriority();
    verifyNoMoreInteractions(_mockChangeProcessor);
  }

  @Test
  public void testProcessorIsCalledForAllAspectsOnEntity() {
    when(_mockChangeProcessor.process(eq(_testEntityName), anyString(), eq(null), eq(_mockRecordTemplate))).thenReturn(
        ChangeResult.success(_mockRecordTemplate));

    _changeStreamProcessor.registerPreProcessor(_testEntityName, "*", _mockChangeProcessor);

    _changeStreamProcessor.preProcessChange(_testEntityName, "anything", null, _mockRecordTemplate);

    verify(_mockChangeProcessor).process(_testEntityName, "anything", null, _mockRecordTemplate);
    verify(_mockChangeProcessor, times(2)).getPriority();
    verifyNoMoreInteractions(_mockChangeProcessor);
  }

  @Test
  public void testProcessorIsCalledForAllAspectsForAllEntities() {
    when(_mockChangeProcessor.process(eq(_testEntityName), anyString(), eq(null), eq(_mockRecordTemplate))).thenReturn(
        ChangeResult.success(_mockRecordTemplate));

    _changeStreamProcessor.registerPreProcessor("*", "*", _mockChangeProcessor);

    _changeStreamProcessor.preProcessChange(_testEntityName, "anything", null, _mockRecordTemplate);

    verify(_mockChangeProcessor).process(_testEntityName, "anything", null, _mockRecordTemplate);
    verify(_mockChangeProcessor, times(2)).getPriority();
    verifyNoMoreInteractions(_mockChangeProcessor);
  }

  @Test
  public void testProcessorsAreCalledInPriorityOrder() {
    when(_mockChangeProcessor.getPriority()).thenReturn(1);
    when(_mockChangeProcessor.process(eq(_testEntityName), anyString(), eq(null), eq(_mockRecordTemplate))).thenReturn(
        ChangeResult.success(_mockRecordTemplate));

    ChangeProcessor secondProcessor = mock(ChangeProcessor.class);
    when(secondProcessor.getPriority()).thenReturn(10);
    when(secondProcessor.process(eq(_testEntityName), anyString(), eq(null), eq(_mockRecordTemplate))).thenReturn(
        ChangeResult.success(_mockRecordTemplate));

    _changeStreamProcessor.registerPreProcessor("*", "*", secondProcessor);
    _changeStreamProcessor.registerPreProcessor("*", "*", _mockChangeProcessor);

    _changeStreamProcessor.preProcessChange(_testEntityName, "anything", null, _mockRecordTemplate);

    InOrder inOrder = inOrder(_mockChangeProcessor, secondProcessor);
    inOrder.verify(_mockChangeProcessor).process(_testEntityName, "anything", null, _mockRecordTemplate);
    inOrder.verify(secondProcessor).process(_testEntityName, "anything", null, _mockRecordTemplate);
  }
}

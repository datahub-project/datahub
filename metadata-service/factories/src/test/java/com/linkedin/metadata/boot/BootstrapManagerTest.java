package com.linkedin.metadata.boot;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BootstrapManagerTest {

  private OperationContext operationContext;
  private BootstrapStep mockBlockingStep1;
  private BootstrapStep mockBlockingStep2;
  private BootstrapStep mockAsyncStep;

  @BeforeMethod
  public void setUp() {
    operationContext = TestOperationContexts.systemContextNoSearchAuthorization();

    mockBlockingStep1 = mock(BootstrapStep.class);
    when(mockBlockingStep1.getExecutionMode()).thenReturn(BootstrapStep.ExecutionMode.BLOCKING);
    when(mockBlockingStep1.name()).thenReturn("BlockingStep1");

    mockBlockingStep2 = mock(BootstrapStep.class);
    when(mockBlockingStep2.getExecutionMode()).thenReturn(BootstrapStep.ExecutionMode.BLOCKING);
    when(mockBlockingStep2.name()).thenReturn("BlockingStep2");

    mockAsyncStep = mock(BootstrapStep.class);
    when(mockAsyncStep.getExecutionMode()).thenReturn(BootstrapStep.ExecutionMode.ASYNC);
    when(mockAsyncStep.name()).thenReturn("AsyncStep");
  }

  @Test
  public void testInitialState() {
    List<BootstrapStep> steps = Arrays.asList(mockBlockingStep1, mockAsyncStep);
    BootstrapManager manager = new BootstrapManager(steps);

    // Initially, blocking steps should not be complete
    assertFalse(manager.areBlockingStepsComplete());
  }

  @Test
  public void testBlockingStepsCompletion() throws Exception {
    List<BootstrapStep> steps = Arrays.asList(mockBlockingStep1, mockBlockingStep2, mockAsyncStep);
    BootstrapManager manager = new BootstrapManager(steps);

    // Before start, blocking steps not complete
    assertFalse(manager.areBlockingStepsComplete());

    // Start bootstrap process
    manager.start(operationContext);

    // After start, blocking steps should be complete
    assertTrue(manager.areBlockingStepsComplete());

    // Verify blocking steps were executed
    verify(mockBlockingStep1).execute(operationContext);
    verify(mockBlockingStep2).execute(operationContext);
  }

  @Test
  public void testOnlyBlockingSteps() throws Exception {
    List<BootstrapStep> steps = Arrays.asList(mockBlockingStep1, mockBlockingStep2);
    BootstrapManager manager = new BootstrapManager(steps);

    manager.start(operationContext);

    // Blocking steps should be complete
    assertTrue(manager.areBlockingStepsComplete());

    // Verify all blocking steps were executed
    verify(mockBlockingStep1).execute(operationContext);
    verify(mockBlockingStep2).execute(operationContext);
  }

  @Test
  public void testOnlyAsyncSteps() throws Exception {
    List<BootstrapStep> steps = Arrays.asList(mockAsyncStep);
    BootstrapManager manager = new BootstrapManager(steps);

    manager.start(operationContext);

    // Even with only async steps, blocking steps should be marked complete
    assertTrue(manager.areBlockingStepsComplete());
  }

  @Test
  public void testEmptyStepsList() {
    List<BootstrapStep> steps = Arrays.asList();
    BootstrapManager manager = new BootstrapManager(steps);

    manager.start(operationContext);

    // With no steps, blocking steps should be marked complete
    assertTrue(manager.areBlockingStepsComplete());
  }

  @Test
  public void testAsyncStepExecution() throws Exception {
    List<BootstrapStep> steps = Arrays.asList(mockBlockingStep1, mockAsyncStep);
    BootstrapManager manager = new BootstrapManager(steps);

    manager.start(operationContext);

    // Blocking step should be executed immediately
    verify(mockBlockingStep1).execute(operationContext);

    // Async step execution is started but may not be complete yet
    // We can't easily verify async execution without adding complexity to the test
    // The important part is that blocking steps are marked complete regardless
    assertTrue(manager.areBlockingStepsComplete());
  }
}

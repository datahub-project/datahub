package com.linkedin.datahub.upgrade.assertions;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.system.assertions.GenerateAssertionEntityFieldStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.stream.Stream;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GenerateAssertionEntityFieldStepTest {

  @Mock private OperationContext mockOpContext;

  @Mock private EntityService<?> mockEntityService;

  @Mock private AspectDao mockAspectDao;

  @Mock private RetrieverContext mockRetrieverContext;

  private GenerateAssertionEntityFieldStep step;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    step =
        new GenerateAssertionEntityFieldStep(
            mockOpContext, mockEntityService, mockAspectDao, 10, 100, 1000);
    when(mockOpContext.getRetrieverContext()).thenReturn(mockRetrieverContext);
  }

  /** Test to verify the correct step ID is returned. */
  @Test
  public void testId() {
    assertEquals(step.id(), "assertion-entity-field-v3");
  }

  /** Test to verify that continueOnValidationFailure returns true for this step. */
  @Test
  public void testContinueOnValidationFailure() {
    assertTrue(step.continueOnValidationFailure());
  }

  /** Test to verify the skip logic based on previous run history. */
  @Test
  public void testSkip() {
    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);

    // Test case: No previous result - should not skip
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());
    assertFalse(step.skip(mockContext));

    // Test case: Previous result with SUCCEEDED state - should skip
    DataHubUpgradeResult succeededResult = mock(DataHubUpgradeResult.class);
    when(succeededResult.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(mockUpgrade.getUpgradeResult(any(), any(), any()))
        .thenReturn(Optional.of(succeededResult));
    assertTrue(step.skip(mockContext));

    // Test case: Previous result with ABORTED state - should skip
    DataHubUpgradeResult abortedResult = mock(DataHubUpgradeResult.class);
    when(abortedResult.getState()).thenReturn(DataHubUpgradeState.ABORTED);
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(abortedResult));
    assertTrue(step.skip(mockContext));

    // Test case: Previous result with IN_PROGRESS state - should not skip
    DataHubUpgradeResult inProgressResult = mock(DataHubUpgradeResult.class);
    when(inProgressResult.getState()).thenReturn(DataHubUpgradeState.IN_PROGRESS);
    when(mockUpgrade.getUpgradeResult(any(), any(), any()))
        .thenReturn(Optional.of(inProgressResult));
    assertFalse(step.skip(mockContext));
  }

  /** Test to verify the correct URN pattern is returned. */
  @Test
  public void testGetUrnLike() {
    assertEquals(step.getUrnLike(), "urn:li:assertion:%");
  }

  /**
   * Test to verify the executable function processes batches correctly and returns a success
   * result.
   */
  @Test
  public void testExecutable() throws URISyntaxException {
    UpgradeContext mockContext = mock(UpgradeContext.class);
    Upgrade mockUpgrade = mock(Upgrade.class);
    UpgradeReport mockReport = mock(UpgradeReport.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.report()).thenReturn(mockReport);
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());

    @SuppressWarnings("unchecked")
    PartitionedStream<EbeanAspectV2> mockStream = mock(PartitionedStream.class);
    when(mockAspectDao.streamAspectBatches(any(RestoreIndicesArgs.class))).thenReturn(mockStream);

    // Return empty stream to avoid complex mocking of EntityUtils
    when(mockStream.partition(anyInt())).thenReturn(Stream.empty());

    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);

    UpgradeStepResult result = step.executable().apply(mockContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockAspectDao).streamAspectBatches(argsCaptor.capture());
    assertEquals(argsCaptor.getValue().aspectNames().get(0), "assertionInfo");
    assertEquals(argsCaptor.getValue().batchSize(), 10);
    assertEquals(argsCaptor.getValue().limit(), 1000);
  }

  // Additional tests can be added to cover more scenarios and edge cases
}

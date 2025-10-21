package com.linkedin.datahub.upgrade.assertions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.system.assertions.GenerateAssertionEntityFieldStep;
import com.linkedin.metadata.aspect.SystemAspect;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class GenerateAssertionEntityFieldStepTest {

  @Mock private OperationContext mockOpContext;

  @Mock private EntityService<?> mockEntityService;

  @Mock private AspectDao mockAspectDao;

  @Mock private RetrieverContext mockRetrieverContext;

  private GenerateAssertionEntityFieldStep step;

  @BeforeEach
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
    assertEquals("assertion-entity-field-v1", step.id());
  }

  /** Test to verify the skip logic based on previous run history. */
  @Test
  public void testSkip() {
    UpgradeContext mockContext = mock(UpgradeContext.class);

    // Test case: No previous result - should not skip
    when(mockContext.upgrade().getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());
    assertFalse(step.skip(mockContext));

    // Test case: Previous result with SUCCEEDED state - should skip
    DataHubUpgradeResult succeededResult = mock(DataHubUpgradeResult.class);
    when(succeededResult.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(mockContext.upgrade().getUpgradeResult(any(), any(), any()))
        .thenReturn(Optional.of(succeededResult));
    assertTrue(step.skip(mockContext));

    // Test case: Previous result with ABORTED state - should skip
    DataHubUpgradeResult abortedResult = mock(DataHubUpgradeResult.class);
    when(abortedResult.getState()).thenReturn(DataHubUpgradeState.ABORTED);
    when(mockContext.upgrade().getUpgradeResult(any(), any(), any()))
        .thenReturn(Optional.of(abortedResult));
    assertTrue(step.skip(mockContext));

    // Test case: Previous result with IN_PROGRESS state - should not skip
    DataHubUpgradeResult inProgressResult = mock(DataHubUpgradeResult.class);
    when(inProgressResult.getState()).thenReturn(DataHubUpgradeState.IN_PROGRESS);
    when(mockContext.upgrade().getUpgradeResult(any(), any(), any()))
        .thenReturn(Optional.of(inProgressResult));
    assertFalse(step.skip(mockContext));
  }

  /** Test to verify the correct URN pattern is returned. */
  @Test
  public void testGetUrnLike() {
    assertEquals("urn:li:assertion:%", step.getUrnLike());
  }

  /**
   * Test to verify the executable function processes batches correctly and returns a success
   * result.
   */
  @Test
  public void testExecutable() throws URISyntaxException {
    UpgradeContext mockContext = mock(UpgradeContext.class);

    EbeanAspectV2 mockAspect = mock(EbeanAspectV2.class);
    @SuppressWarnings("unchecked")
    PartitionedStream<EbeanAspectV2> mockStream = mock(PartitionedStream.class);
    when(mockAspectDao.streamAspectBatches(any(RestoreIndicesArgs.class))).thenReturn(mockStream);

    when(mockStream.partition(anyInt())).thenReturn(Stream.of(Stream.of(mockAspect)));

    SystemAspect mockSystemAspect = mock(SystemAspect.class);
    when(mockSystemAspect.getAspectName()).thenReturn("assertionInfo");
    when(mockSystemAspect.getUrn())
        .thenReturn(com.linkedin.common.urn.Urn.createFromString("urn:li:assertion:test"));

    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);

    UpgradeStepResult result = step.executable().apply(mockContext);
    assertEquals(DataHubUpgradeState.SUCCEEDED, result.result());

    verify(mockAspectDao).streamAspectBatches(argsCaptor.capture());
    assertEquals("assertionInfo", argsCaptor.getValue().aspectNames().get(0));
    assertEquals(10, argsCaptor.getValue().batchSize());
    assertEquals(1000, argsCaptor.getValue().limit());
  }

  // Additional tests can be added to cover more scenarios and edge cases
}

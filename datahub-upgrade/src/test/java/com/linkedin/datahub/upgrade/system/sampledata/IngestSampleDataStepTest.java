package com.linkedin.datahub.upgrade.system.sampledata;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IngestSampleDataStepTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private EntityService<?> mockEntityService;
  private UpgradeContext mockContext;

  @BeforeMethod
  public void setup() {
    mockEntityService = mock(EntityService.class);
    mockContext = mock(UpgradeContext.class);
    when(mockContext.opContext()).thenReturn(OP_CONTEXT);
  }

  @Test
  public void testId() {
    IngestSampleDataStep step =
        new IngestSampleDataStep(OP_CONTEXT, mockEntityService, true, false, 500);
    // ID includes a hash of the file content for versioning
    assertTrue(step.id().startsWith("IngestSampleData-"));
  }

  @Test
  public void testIsOptional() {
    IngestSampleDataStep step =
        new IngestSampleDataStep(OP_CONTEXT, mockEntityService, true, false, 500);
    // Free trial data ingestion is optional and should not block system startup
    assertTrue(step.isOptional());
  }

  @Test
  public void testSkipWhenDisabled() {
    IngestSampleDataStep step =
        new IngestSampleDataStep(OP_CONTEXT, mockEntityService, false, false, 500);
    assertTrue(step.skip(mockContext));
  }

  @Test
  public void testSkipWhenPreviouslyRun() {
    when(mockEntityService.exists(any(), any(), any(), anyBoolean())).thenReturn(true);

    IngestSampleDataStep step =
        new IngestSampleDataStep(OP_CONTEXT, mockEntityService, true, false, 500);
    assertTrue(step.skip(mockContext));
  }

  @Test
  public void testNoSkipWhenEnabledAndNotPreviouslyRun() {
    when(mockEntityService.exists(any(), any(), any(), anyBoolean())).thenReturn(false);

    IngestSampleDataStep step =
        new IngestSampleDataStep(OP_CONTEXT, mockEntityService, true, false, 500);
    assertFalse(step.skip(mockContext));
  }

  @Test
  public void testNoSkipWhenReprocessEnabled() {
    // Even if previously run, reprocess should not skip
    when(mockEntityService.exists(any(), any(), any(), anyBoolean())).thenReturn(true);

    IngestSampleDataStep step =
        new IngestSampleDataStep(OP_CONTEXT, mockEntityService, true, true, 500);
    assertFalse(step.skip(mockContext));
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testConstructorFailsWhenFileNotFound() {
    // When file doesn't exist, constructor should fail (can't compute hash)
    new IngestSampleDataStep(
        OP_CONTEXT, mockEntityService, true, false, 500, "nonexistent/path/sample_data.json");
  }

  @Test
  public void testExecutableSucceedsWithValidFile() {
    IngestSampleDataStep step =
        new IngestSampleDataStep(
            OP_CONTEXT, mockEntityService, true, false, 500, "sampledata/test_sample_data.json");

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    // Verify ingestProposal was called with async=false to ensure synchronous writes
    verify(mockEntityService, times(1)).ingestProposal(any(), any(AspectsBatch.class), eq(false));
  }

  @Test
  public void testUpgradeWrapperWithDisabled() {
    IngestSampleData upgrade =
        new IngestSampleData(OP_CONTEXT, mockEntityService, null, false, false, 500, 30, 30);

    assertEquals(upgrade.id(), "IngestSampleData");
    assertTrue(upgrade.steps().isEmpty());
  }

  @Test
  public void testUpgradeWrapperWithEnabled() {
    // When enabled but statisticsGenerator is null, should only have 1 step (data ingestion)
    IngestSampleData upgrade =
        new IngestSampleData(OP_CONTEXT, mockEntityService, null, true, false, 500, 30, 30);

    assertEquals(upgrade.id(), "IngestSampleData");
    assertEquals(upgrade.steps().size(), 1);
  }
}

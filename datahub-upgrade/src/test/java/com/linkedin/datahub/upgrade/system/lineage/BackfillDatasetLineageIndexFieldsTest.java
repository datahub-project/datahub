package com.linkedin.datahub.upgrade.system.lineage;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import org.testng.annotations.Test;

public class BackfillDatasetLineageIndexFieldsTest {

  @Test
  public void testBackfillJobInstantiation() {
    // Mock dependencies
    OperationContext opContext = mock(OperationContext.class);
    EntityService<?> entityService = mock(EntityService.class);
    SearchService searchService = mock(SearchService.class);

    // Test that the backfill job can be instantiated
    BackfillDatasetLineageIndexFields backfillJob =
        new BackfillDatasetLineageIndexFields(
            opContext,
            entityService,
            searchService,
            true, // enabled
            false, // reprocessEnabled
            100 // batchSize
            );

    // Verify the job has steps when enabled
    assertEquals(backfillJob.steps().size(), 1);
    assertEquals(backfillJob.id(), "BackfillDatasetLineageIndexFields");
  }

  @Test
  public void testBackfillJobDisabled() {
    // Mock dependencies
    OperationContext opContext = mock(OperationContext.class);
    EntityService<?> entityService = mock(EntityService.class);
    SearchService searchService = mock(SearchService.class);

    // Test that the backfill job has no steps when disabled
    BackfillDatasetLineageIndexFields backfillJob =
        new BackfillDatasetLineageIndexFields(
            opContext,
            entityService,
            searchService,
            false, // enabled
            false, // reprocessEnabled
            100 // batchSize
            );

    // Verify the job has no steps when disabled
    assertEquals(backfillJob.steps().size(), 0);
    assertEquals(backfillJob.id(), "BackfillDatasetLineageIndexFields");
  }

  @Test
  public void testBackfillStepInstantiation() {
    // Mock dependencies
    OperationContext opContext = mock(OperationContext.class);
    EntityService<?> entityService = mock(EntityService.class);
    SearchService searchService = mock(SearchService.class);

    // Test that the backfill step can be instantiated
    BackfillDatasetLineageIndexFieldsStep backfillStep =
        new BackfillDatasetLineageIndexFieldsStep(
            opContext,
            entityService,
            searchService,
            false, // reprocessEnabled
            100 // batchSize
            );

    // Verify the step properties
    assertEquals(backfillStep.id(), "BackfillDatasetLineageIndexFieldsStep_V1");
    assertTrue(backfillStep.isOptional());
  }

  @Test
  public void testBackfillStepSkipLogic() {
    // Mock dependencies
    OperationContext opContext = mock(OperationContext.class);
    EntityService<?> entityService = mock(EntityService.class);
    SearchService searchService = mock(SearchService.class);

    // Test with reprocessEnabled = true
    BackfillDatasetLineageIndexFieldsStep backfillStep =
        new BackfillDatasetLineageIndexFieldsStep(
            opContext,
            entityService,
            searchService,
            true, // reprocessEnabled
            100 // batchSize
            );

    // When reprocessEnabled is true, skip should return false
    assertFalse(backfillStep.skip(null));
  }
}

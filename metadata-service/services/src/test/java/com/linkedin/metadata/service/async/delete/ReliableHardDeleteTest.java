package com.linkedin.metadata.service.async.delete;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.ConditionalDeleteOutcome;
import com.linkedin.metadata.entity.DeleteCeiling;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackResult;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.run.DeleteReferencesResponse;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** The orchestration only: capture, today's checks, references, then the bounded deleteUrn. */
public class ReliableHardDeleteTest {
  private static final Urn URN = UrnUtils.getUrn("urn:li:tag:reliable");
  private static final DeleteCeiling CEILING =
      new DeleteCeiling(Map.of("tagKey", 1L, "tagProperties", 3L));

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private EntityService<?> entityService;
  private DeleteEntityService deleteEntityService;
  private ReliableHardDelete reliableHardDelete;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setup() {
    entityService = mock(EntityService.class);
    deleteEntityService = mock(DeleteEntityService.class);
    reliableHardDelete = new ReliableHardDelete(entityService, deleteEntityService, true);
    when(entityService.captureDeleteCeiling(any(), eq(URN))).thenReturn(Optional.of(CEILING));
    when(deleteEntityService.deleteReferencesToOrFail(any(), eq(URN)))
        .thenReturn(new DeleteReferencesResponse().setTotal(2));
  }

  @Test
  public void anAbsentEntityIsAlreadyDeletedAndNothingRuns() {
    when(entityService.captureDeleteCeiling(any(), eq(URN))).thenReturn(Optional.empty());

    assertEquals(
        reliableHardDelete.delete(opContext, URN).outcome(),
        ConditionalDeleteOutcome.ALREADY_DELETED);
    verifyNoInteractions(deleteEntityService);
    verify(entityService, never()).deleteUrn(any(), any(), any(DeleteCeiling.class));
  }

  @Test
  public void referencesGoFirstThenTheEntityBoundedByTheCapture() {
    when(entityService.deleteUrn(any(), eq(URN), eq(CEILING)))
        .thenReturn(new RollbackRunResult(List.of(), 7, List.of(deleted("tagKey", true))));

    final DeleteEntityReport report = reliableHardDelete.delete(opContext, URN);

    final InOrder order = inOrder(entityService, deleteEntityService);
    order.verify(entityService).validateHardDelete(any(), eq(URN));
    order.verify(deleteEntityService).deleteReferencesToOrFail(any(), eq(URN));
    order.verify(entityService).deleteUrn(any(), eq(URN), eq(CEILING));
    assertEquals(report.outcome(), ConditionalDeleteOutcome.DELETED);
    assertEquals(report.rowsDeleted(), 7L);
    assertEquals(report.referencesRemoved(), 2);
  }

  /** Data written since the capture keeps the key: only captured aspects were deleted. */
  @Test
  public void anEntityWrittenSinceTheCaptureStays() {
    when(entityService.deleteUrn(any(), eq(URN), eq(CEILING)))
        .thenReturn(new RollbackRunResult(List.of(), 0, List.of(deleted("tagProperties", false))));

    final DeleteEntityReport report = reliableHardDelete.delete(opContext, URN);

    assertEquals(report.outcome(), ConditionalDeleteOutcome.PARTIAL);
    assertEquals(report.rowsDeleted(), 1L);
  }

  /** A referrer that could not be cleaned (e.g. written since it was read) keeps the entity. */
  @Test
  public void aFailedReferenceCleanupLeavesTheEntity() {
    when(deleteEntityService.deleteReferencesToOrFail(any(), eq(URN)))
        .thenThrow(new IllegalStateException("referrer changed since it was read"));

    expectThrows(IllegalStateException.class, () -> reliableHardDelete.delete(opContext, URN));
    verify(entityService, never()).deleteUrn(any(), any(), any(DeleteCeiling.class));
  }

  /** A delete today's checks reject changes nothing: references are not touched. */
  @Test
  public void aRejectedDeleteRemovesNoReferences() {
    doThrow(new IllegalArgumentException("soft-delete the structured property first"))
        .when(entityService)
        .validateHardDelete(any(), eq(URN));

    expectThrows(IllegalArgumentException.class, () -> reliableHardDelete.delete(opContext, URN));
    verifyNoInteractions(deleteEntityService);
    verify(entityService, never()).captureDeleteCeiling(any(), any());
  }

  private static RollbackResult deleted(final String aspectName, final boolean keyAspect) {
    return new RollbackResult(
        URN, "tag", aspectName, null, null, null, null, ChangeType.DELETE, keyAspect, 0);
  }
}

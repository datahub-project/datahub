package com.linkedin.metadata.service.async.delete;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.ConditionalDeleteOutcome;
import com.linkedin.metadata.entity.DeleteCeiling;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackResult;
import com.linkedin.metadata.entity.RollbackRunResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** The orchestration only: capture, then today's deleteUrn bounded by the capture. */
public class ReliableHardDeleteTest {
  private static final Urn URN = UrnUtils.getUrn("urn:li:tag:reliable");
  private static final DeleteCeiling CEILING =
      new DeleteCeiling(Map.of("tagKey", 1L, "tagProperties", 3L), 1L);

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private EntityService<?> entityService;
  private ReliableHardDelete reliableHardDelete;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setup() {
    entityService = mock(EntityService.class);
    reliableHardDelete = new ReliableHardDelete(entityService, true);
    when(entityService.captureDeleteCeiling(any(), eq(URN))).thenReturn(Optional.of(CEILING));
  }

  @Test
  public void anAbsentEntityIsAlreadyDeletedAndNothingRuns() {
    when(entityService.captureDeleteCeiling(any(), eq(URN))).thenReturn(Optional.empty());

    assertEquals(
        reliableHardDelete.delete(opContext, URN).outcome(),
        ConditionalDeleteOutcome.ALREADY_DELETED);
    verify(entityService, never()).deleteUrn(any(), any(), any(DeleteCeiling.class));
  }

  @Test
  public void anUnchangedEntityIsDeletedBoundedByTheCapture() {
    when(entityService.deleteUrn(any(), eq(URN), eq(CEILING)))
        .thenReturn(new RollbackRunResult(List.of(), 7, List.of(deleted("tagKey", true))));

    final DeleteEntityReport report = reliableHardDelete.delete(opContext, URN);

    verify(entityService).deleteUrn(any(), eq(URN), eq(CEILING));
    assertEquals(report.outcome(), ConditionalDeleteOutcome.DELETED);
    assertEquals(report.rowsDeleted(), 7L);
  }

  /** A caller deleting several entities captures first; the given bound is used as it is. */
  @Test
  public void aGivenCeilingIsUsedWithoutCapturingAgain() {
    final Optional<DeleteCeiling> given =
        Optional.of(new DeleteCeiling(Map.of("tagKey", 1L, "tagProperties", 2L), 1L));
    when(entityService.deleteUrn(any(), eq(URN), eq(given.get())))
        .thenReturn(new RollbackRunResult(List.of(), 4, List.of(deleted("tagKey", true))));

    final DeleteEntityReport report = reliableHardDelete.delete(opContext, URN, given);

    verify(entityService).deleteUrn(any(), eq(URN), eq(given.get()));
    verify(entityService, never()).captureDeleteCeiling(any(), any());
    assertEquals(report.outcome(), ConditionalDeleteOutcome.DELETED);
  }

  /**
   * Data written since the capture keeps the key: only captured aspects were deleted, and the
   * request fails so the caller does not remove references to an entity that still exists.
   */
  @Test
  public void anEntityWrittenSinceTheCaptureStaysAndTheDeleteFails() {
    when(entityService.deleteUrn(any(), eq(URN), eq(CEILING)))
        .thenReturn(new RollbackRunResult(List.of(), 0, List.of(deleted("tagProperties", false))));

    expectThrows(IllegalStateException.class, () -> reliableHardDelete.delete(opContext, URN));
    verify(entityService).deleteUrn(any(), eq(URN), eq(CEILING));
  }

  /** The key survived only because a concurrent request deleted the entity: nothing is left. */
  @Test
  public void anEntityDeletedByAConcurrentRequestIsAlreadyDeletedNotPartial() {
    when(entityService.captureDeleteCeiling(any(), eq(URN)))
        .thenReturn(Optional.of(CEILING), Optional.empty());
    when(entityService.deleteUrn(any(), eq(URN), eq(CEILING)))
        .thenReturn(new RollbackRunResult(List.of(), 0, List.of(deleted("tagProperties", false))));

    final DeleteEntityReport report = reliableHardDelete.delete(opContext, URN);

    assertEquals(report.outcome(), ConditionalDeleteOutcome.ALREADY_DELETED);
    assertEquals(report.rowsDeleted(), 1L);
  }

  /** A delete today's checks reject (e.g. a structured property not soft-deleted) fails. */
  @Test
  public void aRejectedDeletePropagates() {
    when(entityService.deleteUrn(any(), eq(URN), eq(CEILING)))
        .thenThrow(new IllegalArgumentException("soft-delete the structured property first"));

    expectThrows(IllegalArgumentException.class, () -> reliableHardDelete.delete(opContext, URN));
  }

  private static RollbackResult deleted(final String aspectName, final boolean keyAspect) {
    return new RollbackResult(
        URN, "tag", aspectName, null, null, null, null, ChangeType.DELETE, keyAspect, 0);
  }
}

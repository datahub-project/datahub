package com.linkedin.metadata.resources.entity;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.DeleteCascadeListener;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.run.DeleteEntityResponse;
import com.linkedin.metadata.run.DeleteReferencesResponse;
import com.linkedin.metadata.run.RelatedAspectArray;
import com.linkedin.metadata.service.async.delete.DeleteEntityReport;
import com.linkedin.metadata.service.async.delete.ReliableHardDelete;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The reliable hard delete behind the Rest.li {@code delete} and {@code deleteReferences} actions,
 * kept out of {@link EntityResource} so that file only gains the two branches that call it.
 */
final class ReliableDeleteActions {

  private ReliableDeleteActions() {}

  static boolean isEnabled(@Nullable final ReliableHardDelete reliableHardDelete) {
    return reliableHardDelete != null && reliableHardDelete.isEnabled();
  }

  /** A whole-entity delete; an aspect or time-window delete keeps the existing code. */
  static boolean appliesToDelete(
      @Nullable final ReliableHardDelete reliableHardDelete,
      @Nullable final String aspectName,
      @Nullable final Long startTimeMillis,
      @Nullable final Long endTimeMillis) {
    return isEnabled(reliableHardDelete)
        && aspectName == null
        && startTimeMillis == null
        && endTimeMillis == null;
  }

  @Nonnull
  static DeleteEntityResponse deleteEntity(
      @Nonnull final ReliableHardDelete reliableHardDelete,
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final String urnStr) {
    final DeleteEntityReport report = reliableHardDelete.delete(opContext, urn);
    return new DeleteEntityResponse()
        .setUrn(urnStr)
        .setRows(report.rowsDeleted())
        .setTimeseriesRows(report.timeseriesRowsDeleted());
  }

  /** Removes the references only; the entity stays. A referrer that cannot be cleaned fails it. */
  @Nonnull
  static DeleteReferencesResponse removeReferences(
      @Nonnull final DeleteEntityService deleteEntityService,
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn) {
    return new DeleteReferencesResponse()
        .setTotal(
            deleteEntityService.removeReferencesResumable(
                opContext, urn, null, DeleteCascadeListener.NOOP))
        .setRelatedAspects(new RelatedAspectArray());
  }
}

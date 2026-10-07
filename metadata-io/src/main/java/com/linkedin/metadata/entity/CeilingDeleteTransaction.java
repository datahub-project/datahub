package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;
import static com.linkedin.metadata.utils.metrics.ExceptionUtils.collectMetrics;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.entity.ebean.batch.DeleteItemImpl;
import com.linkedin.metadata.entity.validation.ValidationException;
import io.datahubproject.metadata.context.OperationContext;
import jakarta.persistence.EntityNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The in-transaction half of a ceiling-bounded hard delete: lock, decide ({@link
 * CeilingDeletePlan}), delete. Runs inside the caller's transaction, opens none of its own and
 * writes only through {@link AspectDao}, so no nested transaction of the same thread ever waits on
 * the row locks it takes. Post-commit work (side effects, MCLs) belongs to the caller.
 */
final class CeilingDeleteTransaction {

  /**
   * Per entity type, the aspect whose pre-delete value the key-delete side effects consume. Keep in
   * sync with the pre-images {@code EntityServiceImpl#deleteAspectWithoutMCL} captures before a key
   * delete (structured property definition, data product properties).
   */
  static final Map<String, String> KEY_DELETE_SIDE_EFFECT_ASPECTS =
      Map.of(
          STRUCTURED_PROPERTY_ENTITY_NAME, STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
          DATA_PRODUCT_ENTITY_NAME, DATA_PRODUCT_PROPERTIES_ASPECT_NAME);

  /**
   * What one transaction did. {@code deletedKey} is set only when the whole entity went ({@code
   * DELETED} by {@link #deleteEntity}); {@code deletedLatest} lists the aspects whose latest row
   * was removed one by one. {@code rowsDeleted} counts every primary-storage row removed, history
   * rows included, as today's {@code deleteUrn} does.
   */
  record Result(
      @Nonnull ConditionalDeleteOutcome outcome,
      @Nullable SystemAspect deletedKey,
      @Nonnull Map<String, RecordTemplate> keyDeleteSideEffectPreImages,
      @Nonnull List<SystemAspect> deletedLatest,
      int rowsDeleted) {

    static Result alreadyDeleted() {
      return new Result(ConditionalDeleteOutcome.ALREADY_DELETED, null, Map.of(), List.of(), 0);
    }
  }

  private final AspectDao aspectDao;

  CeilingDeleteTransaction(@Nonnull AspectDao aspectDao) {
    this.aspectDao = aspectDao;
  }

  /**
   * Locks the key row first (the order pessimistic writers use), checks that the urn was not
   * recreated since the capture, locks every latest row in key order, then deletes what the ceiling
   * covers: the whole entity when nothing survives, otherwise aspect by aspect.
   */
  @Nonnull
  Result deleteEntity(
      @Nonnull OperationContext opContext,
      @Nullable TransactionContext txContext,
      @Nonnull Urn urn,
      @Nonnull DeleteCeiling ceiling) {
    final String keyAspectName = opContext.getKeyAspectName(urn);
    final SystemAspect key = lockedLatestOrNull(opContext, urn, keyAspectName);
    if (key == null || DeleteCeiling.keyCreatedMillisOf(key) != ceiling.keyCreatedMillis()) {
      // Absent, or hard-deleted and recreated since the capture: the entity this request saw is
      // gone and the current one is newer than the request.
      return Result.alreadyDeleted();
    }
    final Map<String, SystemAspect> latest = aspectDao.getLatestAspectsForDecision(opContext, urn);
    final CeilingDeletePlan plan =
        CeilingDeletePlan.of(
            keyAspectName, latest, ceiling.aspectVersions(), ceiling.capturedAtMillis());
    if (plan.survivors().isEmpty()) {
      // Nothing is newer than the request: today's whole-entity delete, unchanged.
      final int rowsDeleted = aspectDao.deleteUrn(opContext, txContext, urn.toString());
      return new Result(
          ConditionalDeleteOutcome.DELETED,
          key,
          keyDeleteSideEffectPreImages(urn, latest),
          List.of(),
          rowsDeleted);
    }
    return deleteAspects(opContext, urn, plan, latest, ConditionalDeleteOutcome.PARTIAL);
  }

  /** One non-key aspect. Locks only its latest row, never the key. */
  @Nonnull
  Result deleteAspect(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      long ceilingVersion) {
    final SystemAspect row = lockedLatestOrNull(opContext, urn, aspectName);
    if (row == null) {
      return Result.alreadyDeleted();
    }
    final Map<String, SystemAspect> latest = Map.of(aspectName, row);
    final CeilingDeletePlan plan =
        CeilingDeletePlan.of(
            opContext.getKeyAspectName(urn),
            latest,
            Map.of(aspectName, ceilingVersion),
            Long.MAX_VALUE);
    return deleteAspects(
        opContext,
        urn,
        plan,
        latest,
        plan.survivors().isEmpty()
            ? ConditionalDeleteOutcome.DELETED
            : ConditionalDeleteOutcome.PARTIAL);
  }

  @Nonnull
  private Result deleteAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull CeilingDeletePlan plan,
      @Nonnull Map<String, SystemAspect> latest,
      @Nonnull ConditionalDeleteOutcome outcome) {
    final List<SystemAspect> removed = plan.deleteWhole().stream().map(latest::get).toList();
    validatePreCommit(opContext, removed);
    int rowsDeleted = 0;
    for (SystemAspect row : removed) {
      rowsDeleted +=
          aspectDao.deleteAspectVersionRange(
              opContext, urn, row.getAspectName(), ASPECT_LATEST_VERSION, Long.MAX_VALUE);
    }
    for (Map.Entry<String, Long> history : plan.deleteHistoryUpTo().entrySet()) {
      rowsDeleted +=
          aspectDao.deleteAspectVersionRange(
              opContext, urn, history.getKey(), 1L, history.getValue());
    }
    return new Result(outcome, null, Map.of(), removed, rowsDeleted);
  }

  /** The DELETE validators the single-aspect delete runs today, on the rows removed whole. */
  private static void validatePreCommit(
      @Nonnull OperationContext opContext, @Nonnull List<SystemAspect> rows) {
    if (rows.isEmpty()) {
      return;
    }
    final ValidationExceptionCollection exceptions =
        AspectsBatch.validatePreCommit(
            opContext,
            rows.stream()
                .map(
                    row ->
                        DeleteItemImpl.builder()
                            .urn(row.getUrn())
                            .aspectName(row.getAspectName())
                            .auditStamp(opContext.getAuditStamp())
                            .previousSystemAspect(row)
                            .build(opContext.getAspectRetriever()))
                .collect(Collectors.toList()),
            opContext.getRetrieverContext(),
            opContext);
    if (!exceptions.isEmpty()) {
      throw new ValidationException(
          collectMetrics(opContext.getMetricUtils().orElse(null), exceptions).toString());
    }
  }

  @Nullable
  private SystemAspect lockedLatestOrNull(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull String aspectName) {
    try {
      return aspectDao.getLatestAspectForDecision(opContext, urn.toString(), aspectName);
    } catch (EntityNotFoundException e) {
      return null;
    }
  }

  @Nonnull
  private static Map<String, RecordTemplate> keyDeleteSideEffectPreImages(
      @Nonnull Urn urn, @Nonnull Map<String, SystemAspect> latest) {
    final String aspectName = KEY_DELETE_SIDE_EFFECT_ASPECTS.get(urn.getEntityType());
    final SystemAspect row = aspectName == null ? null : latest.get(aspectName);
    return row == null || row.getRecordTemplate() == null
        ? Map.of()
        : Map.of(aspectName, row.getRecordTemplate());
  }
}

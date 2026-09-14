package com.linkedin.metadata.columnview.validation;

import com.datahub.context.OperationFingerprint;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.utils.columnview.ColumnViewColumnKinds;
import com.linkedin.view.DataHubColumnViewColumn;
import com.linkedin.view.DataHubColumnViewColumnType;
import com.linkedin.view.DataHubColumnViewInfo;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Validates {@code dataHubColumnViewInfo} writes:
 *
 * <ul>
 *   <li>every column kind is allowed for the view's target
 *   <li>params are consistent with the kind (structuredPropertyParams iff STRUCTURED_PROPERTY,
 *       labelParams iff LABEL with a tag/glossaryTerm urn); {@code display} bounds
 *   <li>no two columns share an identity
 *   <li>the sort column, if any, is one of the view's columns and is not a GRAPH column
 *   <li>no filter field names a GRAPH column
 *   <li>at most {@link ColumnViewColumnKinds#MAX_GRAPH_COLUMNS} GRAPH columns
 * </ul>
 *
 * All kind semantics come from {@link ColumnViewColumnKinds}; this class never switches on the enum
 * directly.
 */
@Setter
@Getter
@Slf4j
@Accessors(chain = true)
public class ColumnViewInfoValidator extends AspectPayloadValidator {

  @Nonnull private AspectPluginConfig config;

  /**
   * Whole-aspect writes (CREATE / UPSERT / UPDATE) are validated up front. A PATCH item carries
   * only its delta here ({@code getAspect} is null), so it is deferred to {@link
   * #validatePreCommitAspects}, where the patch has been applied and the materialized aspect can be
   * checked with the same rules.
   */
  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return validateColumnViewInfoUpserts(
        mcpItems.stream()
            .filter(item -> !ChangeType.PATCH.equals(item.getChangeType()))
            .collect(Collectors.toList()));
  }

  /**
   * Pre-commit sees every item with its final aspect, including PATCH items after application.
   * Whole-aspect items already passed {@link #validateProposedAspects} (a failure there aborts the
   * batch), so re-running the pure checks on them is a cheap no-op; validating all items here keeps
   * PATCH coverage independent of how the materialized item reports its change type.
   */
  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return validateColumnViewInfoUpserts(changeMCPs);
  }

  @VisibleForTesting
  public static Stream<AspectValidationException> validateColumnViewInfoUpserts(
      @Nonnull Collection<? extends BatchItem> mcpItems) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    for (BatchItem item : mcpItems) {
      final DataHubColumnViewInfo info = item.getAspect(DataHubColumnViewInfo.class);
      if (info != null) {
        validate(item, info, exceptions);
      }
    }
    return exceptions.streamAllExceptions();
  }

  private static void validate(
      BatchItem item, DataHubColumnViewInfo info, ValidationExceptionCollection exceptions) {
    // Whole-aspect size caps (name/description length, column count, filter size) first.
    ColumnViewColumnKinds.definitionProblems(info)
        .forEach(problem -> exceptions.addException(item, problem));
    final Set<String> seen = new HashSet<>();
    int graphColumns = 0;
    for (DataHubColumnViewColumn column : info.getDefinition().getColumns()) {
      if (!ColumnViewColumnKinds.isAllowedFor(column.getType(), info.getTarget())) {
        exceptions.addException(
            item,
            String.format(
                "Column kind %s is not valid for target %s", column.getType(), info.getTarget()));
      }
      ColumnViewColumnKinds.paramProblems(column)
          .forEach(problem -> exceptions.addException(item, problem));
      if (ColumnViewColumnKinds.isGraph(column)) {
        graphColumns++;
      }
      final String identity = ColumnViewColumnKinds.identity(column);
      if (!seen.add(identity)) {
        exceptions.addException(item, String.format("Duplicate column %s", identity));
      }
    }
    if (graphColumns > ColumnViewColumnKinds.MAX_GRAPH_COLUMNS) {
      exceptions.addException(
          item,
          String.format(
              "At most %d relationship columns are allowed per Column View (found %d)",
              ColumnViewColumnKinds.MAX_GRAPH_COLUMNS, graphColumns));
    }
    if (info.getDefinition().hasSort()) {
      final DataHubColumnViewColumn sortColumn = info.getDefinition().getSort().getColumn();
      final String sortIdentity = ColumnViewColumnKinds.identity(sortColumn);
      if (!seen.contains(sortIdentity)) {
        exceptions.addException(
            item, String.format("Sort column %s is not one of the view's columns", sortIdentity));
      }
      if (ColumnViewColumnKinds.isGraph(sortColumn)) {
        exceptions.addException(
            item, String.format("Cannot sort by relationship column %s", sortIdentity));
      }
    }
    if (info.getDefinition().hasFilter()) {
      validateFilterFields(item, info.getDefinition().getFilter(), exceptions);
    }
  }

  /**
   * Filter fields are free-form strings evaluated client-side; the only server rule is that none of
   * them names a GRAPH column kind (relationship values are paged previews, not row attributes).
   */
  private static void validateFilterFields(
      BatchItem item, Filter filter, ValidationExceptionCollection exceptions) {
    if (!filter.hasOr()) {
      return;
    }
    for (ConjunctiveCriterion conjunction : filter.getOr()) {
      for (Criterion criterion : conjunction.getAnd()) {
        final DataHubColumnViewColumnType asKind = kindNamedBy(criterion.getField());
        if (asKind != null && ColumnViewColumnKinds.isGraph(asKind)) {
          exceptions.addException(
              item, String.format("Cannot filter by relationship column %s", criterion.getField()));
        }
      }
    }
  }

  private static DataHubColumnViewColumnType kindNamedBy(String field) {
    try {
      return DataHubColumnViewColumnType.valueOf(field);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}

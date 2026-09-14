package com.linkedin.datahub.graphql.resolvers.columnview;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DataHubColumnViewColumnDisplayInput;
import com.linkedin.datahub.graphql.generated.DataHubColumnViewColumnInput;
import com.linkedin.datahub.graphql.generated.DataHubColumnViewDefinitionInput;
import com.linkedin.datahub.graphql.resolvers.view.ViewUtils;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.service.ColumnViewService;
import com.linkedin.metadata.utils.columnview.ColumnViewColumnKinds;
import com.linkedin.view.DataHubColumnViewColumn;
import com.linkedin.view.DataHubColumnViewColumnArray;
import com.linkedin.view.DataHubColumnViewColumnDisplay;
import com.linkedin.view.DataHubColumnViewColumnType;
import com.linkedin.view.DataHubColumnViewDefinition;
import com.linkedin.view.DataHubColumnViewExpand;
import com.linkedin.view.DataHubColumnViewInfo;
import com.linkedin.view.DataHubColumnViewLabelParams;
import com.linkedin.view.DataHubColumnViewLabelStyle;
import com.linkedin.view.DataHubColumnViewOverflow;
import com.linkedin.view.DataHubColumnViewSort;
import com.linkedin.view.DataHubColumnViewStructuredPropertyParams;
import com.linkedin.view.DataHubColumnViewTarget;
import com.linkedin.view.DataHubViewType;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Authorization + input mapping helpers for Column Views. Mirrors {@link ViewUtils}. */
public class ColumnViewUtils {

  /**
   * PERSONAL column views may be created by anyone; GLOBAL ones require the same platform privilege
   * as global Views (MANAGE_GLOBAL_VIEWS). A dedicated MANAGE_GLOBAL_COLUMN_VIEWS privilege is a
   * follow-up; reusing the existing one keeps this branch free of policy-engine changes.
   */
  public static boolean canCreateColumnView(
      @Nonnull DataHubViewType type, @Nonnull QueryContext context) {
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(context, "context must not be null");
    return DataHubViewType.PERSONAL.equals(type)
        || (DataHubViewType.GLOBAL.equals(type)
            && AuthorizationUtils.canManageGlobalViews(context));
  }

  /**
   * Edit / delete access. GLOBAL column views require MANAGE_GLOBAL_VIEWS at the time of the call —
   * unlike Views there is no creator fallback, so a creator who loses the privilege also loses
   * write access to the global views they made. PERSONAL ones may only be modified by their
   * creator.
   */
  public static boolean canUpdateColumnView(
      @Nonnull ColumnViewService columnViewService,
      @Nonnull Urn viewUrn,
      @Nonnull QueryContext context) {
    Objects.requireNonNull(columnViewService, "columnViewService must not be null");
    Objects.requireNonNull(viewUrn, "viewUrn must not be null");
    Objects.requireNonNull(context, "context must not be null");

    final DataHubColumnViewInfo info =
        columnViewService.getColumnViewInfo(context.getOperationContext(), viewUrn);
    if (info == null) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to modify Column View. Column View with urn %s does not exist.", viewUrn));
    }
    if (DataHubViewType.GLOBAL.equals(info.getType())) {
      return AuthorizationUtils.canManageGlobalViews(context);
    }
    return info.getCreated().getActor().equals(UrnUtils.getUrn(context.getActorUrn()));
  }

  /**
   * Read access. GLOBAL column views are visible to everyone; PERSONAL ones only to their creator
   * (and to global-view managers). Tighter than Views, which let any authenticated user resolve a
   * personal view by urn.
   */
  public static boolean canReadColumnView(
      @Nonnull DataHubColumnViewInfo info, @Nonnull QueryContext context) {
    Objects.requireNonNull(info, "info must not be null");
    Objects.requireNonNull(context, "context must not be null");
    if (DataHubViewType.GLOBAL.equals(info.getType())) {
      return true;
    }
    return info.getCreated().getActor().equals(UrnUtils.getUrn(context.getActorUrn()))
        || AuthorizationUtils.canManageGlobalViews(context);
  }

  /**
   * Guards the default-view settings mutations: the referenced Column View must exist, be readable
   * by the caller, target the same table the default is being set for, and — for the organization
   * default — be GLOBAL. Views has no equivalent check, which is how a personal view could become
   * an org default there.
   */
  public static void validateDefaultColumnView(
      @Nonnull ColumnViewService columnViewService,
      @Nonnull String viewUrnString,
      @Nonnull DataHubColumnViewTarget target,
      boolean requireGlobal,
      @Nonnull QueryContext context) {
    final Urn viewUrn = UrnUtils.getUrn(viewUrnString);
    final DataHubColumnViewInfo info =
        columnViewService.getColumnViewInfo(context.getOperationContext(), viewUrn);
    if (info == null) {
      throw new IllegalArgumentException(
          String.format("Column View with urn %s does not exist.", viewUrn));
    }
    if (!canReadColumnView(info, context)) {
      throw new AuthorizationException(
          "Unauthorized to use this Column View. Please contact your DataHub administrator.");
    }
    if (!target.equals(info.getTarget())) {
      throw new IllegalArgumentException(
          String.format(
              "Column View %s targets %s and cannot be the default for %s.",
              viewUrn, info.getTarget(), target));
    }
    if (requireGlobal && !DataHubViewType.GLOBAL.equals(info.getType())) {
      throw new IllegalArgumentException(
          String.format(
              "Only public (GLOBAL) Column Views can be the organization default; %s is %s.",
              viewUrn, info.getType()));
    }
  }

  /** Map a GraphQL definition input to the GMS model. */
  @Nonnull
  public static DataHubColumnViewDefinition mapDefinition(
      @Nonnull final DataHubColumnViewDefinitionInput input,
      @Nullable AspectRetriever aspectRetriever) {
    Objects.requireNonNull(input, "input must not be null");
    final DataHubColumnViewDefinition result = new DataHubColumnViewDefinition();
    result.setColumns(
        new DataHubColumnViewColumnArray(
            input.getColumns().stream()
                .map(ColumnViewUtils::mapColumn)
                .collect(Collectors.toList())));
    if (input.getSort() != null) {
      result.setSort(
          new DataHubColumnViewSort()
              .setColumn(mapColumn(input.getSort().getColumn()))
              .setOrder(SortOrder.valueOf(input.getSort().getOrder().toString())));
    }
    if (input.getFilter() != null) {
      result.setFilter(
          ViewUtils.mapFilter(input.getFilter(), aspectRetriever), SetMode.IGNORE_NULL);
    }
    return result;
  }

  /**
   * Input-side column mapping: translate shape, then fail fast on param/kind mismatches using the
   * same {@link ColumnViewColumnKinds#paramProblems} the aspect validator applies, so the UI gets a
   * readable GraphQL error rather than a validator rejection.
   */
  @Nonnull
  public static DataHubColumnViewColumn mapColumn(
      @Nonnull final DataHubColumnViewColumnInput input) {
    final DataHubColumnViewColumn column = new DataHubColumnViewColumn();
    final DataHubColumnViewColumnType type =
        DataHubColumnViewColumnType.valueOf(input.getType().toString());
    column.setType(type);
    if (input.getStructuredPropertyParams() != null) {
      column.setStructuredPropertyParams(
          new DataHubColumnViewStructuredPropertyParams()
              .setUrn(UrnUtils.getUrn(input.getStructuredPropertyParams().getUrn())));
    }
    if (input.getLabelParams() != null) {
      column.setLabelParams(
          new DataHubColumnViewLabelParams()
              .setUrn(UrnUtils.getUrn(input.getLabelParams().getUrn())));
    }
    if (input.getDisplay() != null) {
      column.setDisplay(mapDisplay(input.getDisplay()));
    }
    final List<String> problems = ColumnViewColumnKinds.paramProblems(column);
    if (!problems.isEmpty()) {
      throw new IllegalArgumentException(String.join("; ", problems));
    }
    return column;
  }

  /** Presentation hints. Bounds are enforced by {@link ColumnViewColumnKinds#paramProblems}. */
  @Nonnull
  public static DataHubColumnViewColumnDisplay mapDisplay(
      @Nonnull final DataHubColumnViewColumnDisplayInput input) {
    final DataHubColumnViewColumnDisplay display = new DataHubColumnViewColumnDisplay();
    if (input.getWidth() != null) {
      display.setWidth(input.getWidth());
    }
    if (input.getMaxItems() != null) {
      display.setMaxItems(input.getMaxItems());
    }
    if (input.getLabelStyle() != null) {
      display.setLabelStyle(DataHubColumnViewLabelStyle.valueOf(input.getLabelStyle().name()));
    }
    if (input.getOverflow() != null) {
      display.setOverflow(DataHubColumnViewOverflow.valueOf(input.getOverflow().name()));
    }
    if (input.getExpand() != null) {
      display.setExpand(DataHubColumnViewExpand.valueOf(input.getExpand().name()));
    }
    if (input.getCustom() != null) {
      final StringMap custom = new StringMap();
      input.getCustom().forEach(e -> custom.put(e.getKey(), e.getValue()));
      display.setCustom(custom);
    }
    return display;
  }

  private ColumnViewUtils() {}
}

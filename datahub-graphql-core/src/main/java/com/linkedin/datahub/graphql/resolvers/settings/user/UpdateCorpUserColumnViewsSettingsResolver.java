package com.linkedin.datahub.graphql.resolvers.settings.user;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.UpdateCorpUserColumnViewsSettingsInput;
import com.linkedin.datahub.graphql.resolvers.columnview.ColumnViewUtils;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserColumnViewDefault;
import com.linkedin.identity.CorpUserColumnViewDefaultArray;
import com.linkedin.identity.CorpUserColumnViewsSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.service.ColumnViewService;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.view.DataHubColumnViewTarget;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver responsible for updating the authenticated user's Column View settings: sets or clears
 * the personal default Column View for one target table.
 */
@Slf4j
@RequiredArgsConstructor
public class UpdateCorpUserColumnViewsSettingsResolver
    implements DataFetcher<CompletableFuture<Boolean>> {

  private final SettingsService _settingsService;
  private final ColumnViewService _columnViewService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateCorpUserColumnViewsSettingsInput input =
        bindArgument(
            environment.getArgument("input"), UpdateCorpUserColumnViewsSettingsInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final DataHubColumnViewTarget target =
              DataHubColumnViewTarget.valueOf(input.getTarget().toString());
          if (input.getDefaultView() != null) {
            // Exists, readable by this user, and for this target — before any settings write.
            ColumnViewUtils.validateDefaultColumnView(
                _columnViewService, input.getDefaultView(), target, false, context);
          }
          try {
            final Urn userUrn = UrnUtils.getUrn(context.getActorUrn());
            final CorpUserSettings maybeSettings =
                _settingsService.getCorpUserSettings(context.getOperationContext(), userUrn);
            final CorpUserSettings newSettings =
                maybeSettings == null
                    ? new CorpUserSettings()
                        .setAppearance(
                            new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false))
                    : maybeSettings;

            final CorpUserColumnViewsSettings columnViews =
                newSettings.hasColumnViews()
                    ? newSettings.getColumnViews()
                    : new CorpUserColumnViewsSettings();
            columnViews.setDefaults(
                ColumnViewDefaultsUtils.upsertCorpUserDefault(
                    columnViews.getDefaults(), target, input.getDefaultView()));
            newSettings.setColumnViews(columnViews);

            _settingsService.updateCorpUserSettings(
                context.getOperationContext(), userUrn, newSettings);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform user column view settings update against input {}, {}",
                input.toString(),
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to perform update to user column view settings against input %s",
                    input.toString()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /** Per-target default list manipulation, shared in spirit with the global settings resolver. */
  static final class ColumnViewDefaultsUtils {
    static CorpUserColumnViewDefaultArray upsertCorpUserDefault(
        @Nonnull final CorpUserColumnViewDefaultArray existing,
        @Nonnull final DataHubColumnViewTarget target,
        final String viewUrnOrNull) {
      final CorpUserColumnViewDefaultArray result =
          new CorpUserColumnViewDefaultArray(
              existing.stream()
                  .filter(d -> !target.equals(d.getTarget()))
                  .collect(Collectors.toList()));
      if (viewUrnOrNull != null) {
        result.add(
            new CorpUserColumnViewDefault()
                .setTarget(target)
                .setView(UrnUtils.getUrn(viewUrnOrNull)));
      }
      return result;
    }

    private ColumnViewDefaultsUtils() {}
  }
}

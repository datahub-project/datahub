package com.linkedin.datahub.graphql.resolvers.settings.view;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateGlobalColumnViewsSettingsInput;
import com.linkedin.datahub.graphql.resolvers.columnview.ColumnViewUtils;
import com.linkedin.metadata.service.ColumnViewService;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.GlobalColumnViewDefault;
import com.linkedin.settings.global.GlobalColumnViewDefaultArray;
import com.linkedin.settings.global.GlobalColumnViewsSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.view.DataHubColumnViewTarget;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Resolver responsible for setting/clearing the organization default Column View for one target.
 *
 * <p>Requires the 'MANAGE_GLOBAL_VIEWS' Platform Privilege (shared with Views for now).
 */
public class UpdateGlobalColumnViewsSettingsResolver
    implements DataFetcher<CompletableFuture<Boolean>> {

  private final SettingsService _settingsService;
  private final ColumnViewService _columnViewService;

  public UpdateGlobalColumnViewsSettingsResolver(
      @Nonnull final SettingsService settingsService,
      @Nonnull final ColumnViewService columnViewService) {
    _settingsService = Objects.requireNonNull(settingsService, "settingsService must not be null");
    _columnViewService =
        Objects.requireNonNull(columnViewService, "columnViewService must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateGlobalColumnViewsSettingsInput input =
        bindArgument(environment.getArgument("input"), UpdateGlobalColumnViewsSettingsInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (AuthorizationUtils.canManageGlobalViews(context)) {
            final DataHubColumnViewTarget target =
                DataHubColumnViewTarget.valueOf(input.getTarget().toString());
            if (input.getDefaultView() != null) {
              // The org default must be an existing GLOBAL view for this target.
              ColumnViewUtils.validateDefaultColumnView(
                  _columnViewService, input.getDefaultView(), target, true, context);
            }
            try {
              final GlobalSettingsInfo maybeGlobalSettings =
                  _settingsService.getGlobalSettings(context.getOperationContext());
              final GlobalSettingsInfo newGlobalSettings =
                  maybeGlobalSettings != null ? maybeGlobalSettings : new GlobalSettingsInfo();
              final GlobalColumnViewsSettings columnViews =
                  newGlobalSettings.hasColumnViews()
                      ? newGlobalSettings.getColumnViews()
                      : new GlobalColumnViewsSettings();

              final GlobalColumnViewDefaultArray defaults =
                  new GlobalColumnViewDefaultArray(
                      columnViews.getDefaults().stream()
                          .filter(d -> !target.equals(d.getTarget()))
                          .collect(Collectors.toList()));
              if (input.getDefaultView() != null) {
                defaults.add(
                    new GlobalColumnViewDefault()
                        .setTarget(target)
                        .setView(UrnUtils.getUrn(input.getDefaultView())));
              }
              columnViews.setDefaults(defaults);
              newGlobalSettings.setColumnViews(columnViews);

              _settingsService.updateGlobalSettings(
                  context.getOperationContext(), newGlobalSettings);
              return true;
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to update global column view settings! %s", input), e);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        },
        this.getClass().getSimpleName(),
        "get");
  }
}

package com.linkedin.datahub.graphql.resolvers.settings;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.GlobalSettings;
import com.linkedin.datahub.graphql.generated.GlobalVisualSettings;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.GlobalSettingsInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

/** Retrieves the platform-level Global Settings. */
public class GlobalSettingsResolver implements DataFetcher<CompletableFuture<GlobalSettings>> {

  private final SettingsService _settingsService;

  public GlobalSettingsResolver(@Nonnull final SettingsService settingsService) {
    _settingsService = Objects.requireNonNull(settingsService, "settingsService must not be null");
  }

  @Override
  public CompletableFuture<GlobalSettings> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final GlobalSettingsInfo globalSettings =
                _settingsService.getGlobalSettings(context.getOperationContext());
            final GlobalSettings result = new GlobalSettings();
            if (globalSettings != null && globalSettings.hasVisual()) {
              result.setVisualSettings(mapVisualSettings(globalSettings.getVisual()));
            }
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve Global Settings", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private static GlobalVisualSettings mapVisualSettings(
      @Nonnull final com.linkedin.settings.global.GlobalVisualSettings settings) {
    final GlobalVisualSettings result = new GlobalVisualSettings();
    result.setShowEnvironmentBadge(settings.isShowEnvironmentBadge());
    return result;
  }
}

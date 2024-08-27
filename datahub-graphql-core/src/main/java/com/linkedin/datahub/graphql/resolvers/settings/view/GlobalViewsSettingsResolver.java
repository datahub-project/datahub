package com.linkedin.datahub.graphql.resolvers.settings.view;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.GlobalViewsSettings;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.GlobalSettingsInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Retrieves the Global Settings related to the Views feature.
 *
 * <p>This capability requires the 'MANAGE_GLOBAL_VIEWS' Platform Privilege.
 */
@Slf4j
public class GlobalViewsSettingsResolver
    implements DataFetcher<CompletableFuture<GlobalViewsSettings>> {

  private final SettingsService _settingsService;

  public GlobalViewsSettingsResolver(final SettingsService settingsService) {
    _settingsService = Objects.requireNonNull(settingsService, "settingsService must not be null");
  }

  @Override
  public CompletableFuture<GlobalViewsSettings> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final GlobalSettingsInfo globalSettings =
                _settingsService.getGlobalSettings(context.getOperationContext());
            return globalSettings != null && globalSettings.hasViews()
                ? mapGlobalViewsSettings(globalSettings.getViews())
                : new GlobalViewsSettings();
          } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve Global Views Settings", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private static GlobalViewsSettings mapGlobalViewsSettings(
      @Nonnull final com.linkedin.settings.global.GlobalViewsSettings settings) {
    final GlobalViewsSettings result = new GlobalViewsSettings();
    if (settings.hasDefaultView()) {
      result.setDefaultView(settings.getDefaultView().toString());
    }
    return result;
  }
}

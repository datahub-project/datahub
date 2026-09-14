package com.linkedin.datahub.graphql.resolvers.settings.view;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.DataHubColumnViewTarget;
import com.linkedin.datahub.graphql.generated.GlobalColumnViewDefault;
import com.linkedin.datahub.graphql.generated.GlobalColumnViewsSettings;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.GlobalSettingsInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Retrieves the Global Settings related to the Column Views feature (readable by any user). */
@Slf4j
public class GlobalColumnViewsSettingsResolver
    implements DataFetcher<CompletableFuture<GlobalColumnViewsSettings>> {

  private final SettingsService _settingsService;

  public GlobalColumnViewsSettingsResolver(final SettingsService settingsService) {
    _settingsService = Objects.requireNonNull(settingsService, "settingsService must not be null");
  }

  @Override
  public CompletableFuture<GlobalColumnViewsSettings> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final GlobalSettingsInfo globalSettings =
                _settingsService.getGlobalSettings(context.getOperationContext());
            final GlobalColumnViewsSettings result = new GlobalColumnViewsSettings();
            result.setDefaults(Collections.emptyList());
            if (globalSettings != null && globalSettings.hasColumnViews()) {
              result.setDefaults(mapDefaults(globalSettings.getColumnViews()));
            }
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve Global Column Views Settings", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private static java.util.List<GlobalColumnViewDefault> mapDefaults(
      @Nonnull final com.linkedin.settings.global.GlobalColumnViewsSettings settings) {
    return settings.getDefaults().stream()
        .map(
            d -> {
              final GlobalColumnViewDefault result = new GlobalColumnViewDefault();
              result.setTarget(DataHubColumnViewTarget.valueOf(d.getTarget().toString()));
              result.setView(d.getView().toString());
              return result;
            })
        .collect(Collectors.toList());
  }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.settings.homePage;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.DataHubPageTemplate;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GlobalHomePageSettings;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.GlobalSettingsInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Retrieves the Global Settings related to the Home Page feature. */
@Slf4j
public class GlobalHomePageSettingsResolver
    implements DataFetcher<CompletableFuture<GlobalHomePageSettings>> {

  private final SettingsService _settingsService;

  public GlobalHomePageSettingsResolver(final SettingsService settingsService) {
    _settingsService = Objects.requireNonNull(settingsService, "settingsService must not be null");
  }

  @Override
  public CompletableFuture<GlobalHomePageSettings> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final GlobalSettingsInfo globalSettings =
                _settingsService.getGlobalSettings(context.getOperationContext());
            final GlobalHomePageSettings defaultSettings = new GlobalHomePageSettings();
            return globalSettings != null && globalSettings.hasHomePage()
                ? mapGlobalHomePageSettings(globalSettings.getHomePage())
                : defaultSettings;
          } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve Global Home Page Settings", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private static GlobalHomePageSettings mapGlobalHomePageSettings(
      @Nonnull final com.linkedin.settings.global.GlobalHomePageSettings settings) {
    final GlobalHomePageSettings result = new GlobalHomePageSettings();

    // Map defaultTemplate settings field
    if (settings.hasDefaultTemplate()) {
      DataHubPageTemplate template = new DataHubPageTemplate();
      template.setUrn(settings.getDefaultTemplate().toString());
      template.setType(EntityType.DATAHUB_PAGE_TEMPLATE);
      result.setDefaultTemplate(template);
    }

    return result;
  }
}

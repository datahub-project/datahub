/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.settings.docPropagation;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.DocPropagationSettings;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.GlobalSettingsInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Retrieves the Global Settings related to the Actions feature. */
@Slf4j
public class DocPropagationSettingsResolver
    implements DataFetcher<CompletableFuture<DocPropagationSettings>> {

  private final SettingsService _settingsService;

  public DocPropagationSettingsResolver(final SettingsService settingsService) {
    _settingsService = Objects.requireNonNull(settingsService, "settingsService must not be null");
  }

  @Override
  public CompletableFuture<DocPropagationSettings> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final GlobalSettingsInfo globalSettings =
                _settingsService.getGlobalSettings(context.getOperationContext());
            final DocPropagationSettings defaultSettings = new DocPropagationSettings();
            // TODO: Enable by default. Currently the automation trusts the settings aspect, which
            // does not have this.
            defaultSettings.setDocColumnPropagation(false);
            return globalSettings != null && globalSettings.hasDocPropagation()
                ? mapDocPropagationSettings(globalSettings.getDocPropagation())
                : defaultSettings;
          } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve Action Settings", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private static DocPropagationSettings mapDocPropagationSettings(
      @Nonnull final com.linkedin.settings.global.DocPropagationFeatureSettings settings) {
    final DocPropagationSettings result = new DocPropagationSettings();

    // Map docColumnPropagation settings field
    result.setDocColumnPropagation(settings.isColumnPropagationEnabled());

    return result;
  }
}

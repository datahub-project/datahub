package com.linkedin.datahub.graphql.resolvers.settings;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.GlobalSettings;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.settings.global.GlobalSettingsInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/**
 * Gets a particular Ingestion Source by urn.
 */
@Slf4j
public class GlobalSettingsResolver implements DataFetcher<CompletableFuture<GlobalSettings>> {

  private final EntityClient _entityClient;

  public GlobalSettingsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<GlobalSettings> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    if (SettingsUtils.canManageGlobalSettings(context)) {
      return CompletableFuture.supplyAsync(() -> {
        try {
          GlobalSettingsInfo globalSettings = SettingsUtils.getGlobalSettings(_entityClient, context.getAuthentication());
          return SettingsUtils.mapGlobalSettings(globalSettings);
        } catch (Exception e) {
          throw new RuntimeException("Failed to retrieve Global Settings", e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
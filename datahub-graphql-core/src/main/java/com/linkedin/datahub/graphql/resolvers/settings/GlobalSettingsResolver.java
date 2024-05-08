package com.linkedin.datahub.graphql.resolvers.settings;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GlobalSettings;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.settings.global.GlobalSettingsInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.services.SecretService;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/** Gets a particular Ingestion Source by urn. */
@Slf4j
public class GlobalSettingsResolver implements DataFetcher<CompletableFuture<GlobalSettings>> {

  private final EntityClient _entityClient;
  private final SettingsMapper _settingsMapper;

  public GlobalSettingsResolver(
      final EntityClient entityClient, final SecretService secretService) {
    _entityClient = entityClient;
    _settingsMapper = new SettingsMapper(secretService);
  }

  @Override
  public CompletableFuture<GlobalSettings> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            GlobalSettingsInfo globalSettings =
                SettingsMapper.getGlobalSettings(context.getOperationContext(), _entityClient);
            return _settingsMapper.mapGlobalSettings(context, globalSettings);
          } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve Global Settings", e);
          }
        });
  }
}

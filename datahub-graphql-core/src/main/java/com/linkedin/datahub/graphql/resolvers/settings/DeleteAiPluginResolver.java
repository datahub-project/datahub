package com.linkedin.datahub.graphql.resolvers.settings;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.buildMetadataChangeProposalWithUrn;
import static com.linkedin.metadata.Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_SETTINGS_URN;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.settings.global.AiPluginConfigArray;
import com.linkedin.settings.global.AiPluginSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Resolver for deleting an AI plugin configuration from GlobalSettings. */
@Slf4j
public class DeleteAiPluginResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient entityClient;

  public DeleteAiPluginResolver(@Nonnull final EntityClient entityClient) {
    this.entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final String pluginId = environment.getArgument("id");

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (SettingsMapper.canManageGlobalSettings(context)) {
            try {
              // Fetch the existing global settings
              GlobalSettingsInfo globalSettings =
                  SettingsMapper.getGlobalSettings(context.getOperationContext(), entityClient);

              // Check if aiPluginSettings exists and contains the plugin
              if (!globalSettings.hasAiPluginSettings()
                  || globalSettings.getAiPluginSettings() == null
                  || !globalSettings.getAiPluginSettings().hasPlugins()) {
                log.warn("AI plugin with ID {} not found (no plugins configured)", pluginId);
                return false;
              }

              AiPluginSettings pluginSettings = globalSettings.getAiPluginSettings();
              AiPluginConfigArray pluginsArray = pluginSettings.getPlugins();

              // Check if plugin exists
              boolean exists = pluginsArray.stream().anyMatch(p -> pluginId.equals(p.getId()));
              if (!exists) {
                log.warn("AI plugin with ID {} not found", pluginId);
                return false;
              }

              // Remove the plugin from the array
              pluginsArray.removeIf(p -> pluginId.equals(p.getId()));
              pluginSettings.setPlugins(pluginsArray);
              globalSettings.setAiPluginSettings(pluginSettings);

              // Write back the updated settings
              final MetadataChangeProposal proposal =
                  buildMetadataChangeProposalWithUrn(
                      GLOBAL_SETTINGS_URN, GLOBAL_SETTINGS_INFO_ASPECT_NAME, globalSettings);
              entityClient.ingestProposal(context.getOperationContext(), proposal, false);

              log.info("Deleted AI plugin: {}", pluginId);
              return true;

            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to delete AI plugin: %s", pluginId), e);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        },
        this.getClass().getSimpleName(),
        "get");
  }
}

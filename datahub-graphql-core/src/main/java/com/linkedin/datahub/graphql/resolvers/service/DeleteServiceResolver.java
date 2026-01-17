package com.linkedin.datahub.graphql.resolvers.service;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.buildMetadataChangeProposalWithUrn;
import static com.linkedin.metadata.Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_SETTINGS_URN;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.resolvers.connection.ConnectionUtils;
import com.linkedin.datahub.graphql.resolvers.settings.SettingsMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
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

/** Deletes a Service entity. */
@Slf4j
public class DeleteServiceResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient entityClient;

  public DeleteServiceResolver(@Nonnull final EntityClient entityClient) {
    this.entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final String urnStr = environment.getArgument("urn");
    final Urn serviceUrn = UrnUtils.getUrn(urnStr);

    // Reuse MANAGE_CONNECTIONS privilege since Services are external connections
    if (!ConnectionUtils.canManageConnections(context)) {
      throw new AuthorizationException(
          "Unauthorized to delete services. Please contact your DataHub administrator.");
    }

    // Verify it's a service URN
    if (!Constants.SERVICE_ENTITY_NAME.equals(serviceUrn.getEntityType())) {
      throw new IllegalArgumentException(String.format("URN %s is not a service URN", serviceUrn));
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            // First, remove from GlobalSettings.aiPlugins
            removeFromGlobalSettings(context, serviceUrn);

            // Then delete the Service entity
            entityClient.deleteEntity(context.getOperationContext(), serviceUrn);
            log.info("Deleted service: {}", serviceUrn);
            return true;
          } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to delete service %s", serviceUrn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Removes the AiPluginConfig for this service from GlobalSettings.aiPluginSettings.
   *
   * <p>TODO: This method currently leaves orphaned credentials behind. When a service is configured
   * with SHARED_API_KEY authentication, the associated DataHubConnection entity (referenced by
   * sharedApiKeyConfig.credentialUrn) is not deleted. This leaves orphaned encrypted credentials
   * with no way to clean them up through normal operations. To fix this:
   *
   * <ol>
   *   <li>Before removing the plugin config, check if it has sharedApiKeyConfig
   *   <li>If so, read sharedApiKeyConfig.credentialUrn
   *   <li>Delete the corresponding DataHubConnection entity
   * </ol>
   */
  private void removeFromGlobalSettings(final QueryContext context, final Urn serviceUrn)
      throws Exception {

    // Fetch existing GlobalSettings
    GlobalSettingsInfo globalSettings =
        SettingsMapper.getGlobalSettings(context.getOperationContext(), entityClient);

    if (!globalSettings.hasAiPluginSettings()
        || globalSettings.getAiPluginSettings() == null
        || !globalSettings.getAiPluginSettings().hasPlugins()) {
      return; // Nothing to remove
    }

    AiPluginSettings pluginSettings = globalSettings.getAiPluginSettings();
    AiPluginConfigArray pluginsArray = pluginSettings.getPlugins();

    // TODO: Before removing, extract and delete any associated credentials
    // (sharedApiKeyConfig.credentialUrn -> DataHubConnection entity)

    // Remove the plugin entry matching this service URN
    boolean removed = pluginsArray.removeIf(config -> config.getServiceUrn().equals(serviceUrn));

    if (!removed) {
      return; // Service not in plugins
    }

    pluginSettings.setPlugins(pluginsArray);
    globalSettings.setAiPluginSettings(pluginSettings);

    // Write back to GlobalSettings
    final MetadataChangeProposal proposal =
        buildMetadataChangeProposalWithUrn(
            GLOBAL_SETTINGS_URN, GLOBAL_SETTINGS_INFO_ASPECT_NAME, globalSettings);
    entityClient.ingestProposal(context.getOperationContext(), proposal, false);

    log.info("Removed AiPluginConfig for service {} from GlobalSettings", serviceUrn);
  }
}

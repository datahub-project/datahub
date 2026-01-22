package com.linkedin.datahub.graphql.resolvers.settings.user;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.UpdateUserAiPluginSettingsInput;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.identity.UserAiPluginConfig;
import com.linkedin.identity.UserAiPluginConfigArray;
import com.linkedin.identity.UserAiPluginSettings;
import com.linkedin.identity.UserApiKeyConnectionConfig;
import com.linkedin.identity.UserOAuthConnectionConfig;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver responsible for updating the authenticated user's AI Plugin-specific settings. This is a
 * per-plugin update - only the specified plugin is modified.
 */
@Slf4j
@RequiredArgsConstructor
public class UpdateUserAiPluginSettingsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final SettingsService _settingsService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateUserAiPluginSettingsInput input =
        bindArgument(environment.getArgument("input"), UpdateUserAiPluginSettingsInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {

            final Urn userUrn = UrnUtils.getUrn(context.getActorUrn());

            final CorpUserSettings maybeSettings =
                _settingsService.getCorpUserSettings(context.getOperationContext(), userUrn);

            final CorpUserSettings newSettings =
                maybeSettings == null
                    ? new CorpUserSettings()
                        .setAppearance(
                            new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false))
                    : maybeSettings;

            // Patch the user plugin settings. This does a R-M-W.
            updateUserPluginSettings(newSettings, input, userUrn);

            _settingsService.updateCorpUserSettings(
                context.getOperationContext(), userUrn, newSettings);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform user AI plugin settings update against input {}, {}",
                input.toString(),
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to perform update to user AI plugin settings against input %s",
                    input.toString()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Updates the user's AI plugin settings for the specified plugin. If the plugin config doesn't
   * exist, it creates a new one. If it exists, it updates the fields that are provided in the
   * input.
   */
  private static void updateUserPluginSettings(
      @Nonnull final CorpUserSettings settings,
      @Nonnull final UpdateUserAiPluginSettingsInput input,
      @Nonnull final Urn userUrn) {

    // Get or create aiPluginSettings
    UserAiPluginSettings aiPluginSettings =
        settings.hasAiPluginSettings()
            ? settings.getAiPluginSettings()
            : new UserAiPluginSettings().setPlugins(new UserAiPluginConfigArray());

    // Find or create the plugin config for this plugin ID
    UserAiPluginConfig pluginConfig =
        findOrCreatePluginConfig(aiPluginSettings, input.getPluginId());

    // Update the fields that are provided
    if (input.getEnabled() != null) {
      pluginConfig.setEnabled(input.getEnabled());
    }

    if (input.getAllowedTools() != null) {
      pluginConfig.setAllowedTools(
          new com.linkedin.data.template.StringArray(input.getAllowedTools()));
    }

    // Handle API key update
    if (input.getApiKey() != null) {
      if (input.getApiKey().isEmpty()) {
        // Empty string means disconnect - remove the apiKeyConfig
        pluginConfig.removeApiKeyConfig();
      } else {
        // Non-empty string means create/update the API key
        // The actual credential storage is handled by the integrations service OAuth endpoints
        // Here we just track the connection URN
        String connectionUrn = buildUserConnectionUrn(userUrn.toString(), input.getPluginId());
        pluginConfig.setApiKeyConfig(
            new UserApiKeyConnectionConfig().setConnectionUrn(UrnUtils.getUrn(connectionUrn)));
      }
    }

    // Handle OAuth connection - oauthConnectionUrn takes precedence over disconnectOAuth
    if (input.getOauthConnectionUrn() != null) {
      // Set the OAuth connection (used by integrations service after OAuth flow completes)
      pluginConfig.setOauthConfig(
          new UserOAuthConnectionConfig()
              .setConnectionUrn(UrnUtils.getUrn(input.getOauthConnectionUrn())));
    } else if (Boolean.TRUE.equals(input.getDisconnectOAuth())) {
      // Handle OAuth disconnect
      pluginConfig.removeOauthConfig();
    }

    settings.setAiPluginSettings(aiPluginSettings);
  }

  /** Finds an existing plugin config by ID or creates a new one if not found. */
  private static UserAiPluginConfig findOrCreatePluginConfig(
      @Nonnull UserAiPluginSettings settings, @Nonnull String pluginId) {

    UserAiPluginConfigArray plugins = settings.getPlugins();

    // Find existing config
    for (UserAiPluginConfig config : plugins) {
      if (pluginId.equals(config.getId())) {
        return config;
      }
    }

    // Create new config
    UserAiPluginConfig newConfig = new UserAiPluginConfig().setId(pluginId);
    plugins.add(newConfig);
    return newConfig;
  }

  /**
   * Builds a DataHubConnection URN for user credentials. Format:
   * urn:li:dataHubConnection:<sanitized_user>__<sanitized_plugin>
   *
   * <p>Uses `__` as separator (not a tuple) because DataHubConnection expects a single-string key.
   * Using tuple format (a,b) causes DataHub to interpret it as a multi-part key which fails.
   *
   * <p>All special characters are sanitized to underscores for URN safety.
   *
   * <p>This must match the Python implementation in credential_store.py's build_connection_urn().
   */
  private static String buildUserConnectionUrn(String userUrn, String pluginId) {
    // Sanitize both parts - replace colons, commas, and parentheses with underscores
    String sanitizedUserUrn =
        userUrn.replace(":", "_").replace(",", "_").replace("(", "_").replace(")", "_");
    String sanitizedPluginId =
        pluginId.replace(":", "_").replace(",", "_").replace("(", "_").replace(")", "_");
    return String.format("urn:li:dataHubConnection:%s__%s", sanitizedUserUrn, sanitizedPluginId);
  }
}

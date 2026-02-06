package com.linkedin.datahub.graphql.resolvers.settings;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AiInstructionInput;
import com.linkedin.datahub.graphql.generated.AiPluginConfigInput;
import com.linkedin.datahub.graphql.generated.TeamsChannelInput;
import com.linkedin.datahub.graphql.generated.UpdateAiAssistantSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateDocumentationAiSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateEmailIntegrationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateGlobalIntegrationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateGlobalNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateGlobalSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateOidcSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateSlackIntegrationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateSsoSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateTeamsIntegrationSettingsInput;
import com.linkedin.datahub.graphql.resolvers.settings.ai.AiInstructionValidationUtils;
import com.linkedin.datahub.graphql.types.notification.mappers.NotificationSettingMapMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.settings.NotificationSettingMap;
import com.linkedin.settings.global.AiAssistantSettings;
import com.linkedin.settings.global.AiInstruction;
import com.linkedin.settings.global.AiInstructionArray;
import com.linkedin.settings.global.AiInstructionType;
import com.linkedin.settings.global.AiPluginAuthType;
import com.linkedin.settings.global.AiPluginConfig;
import com.linkedin.settings.global.AiPluginConfigArray;
import com.linkedin.settings.global.AiPluginSettings;
import com.linkedin.settings.global.AiPluginType;
import com.linkedin.settings.global.DocumentationAiSettings;
import com.linkedin.settings.global.EmailIntegrationSettings;
import com.linkedin.settings.global.GlobalIntegrationSettings;
import com.linkedin.settings.global.GlobalNotificationSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.OAuthAiPluginConfig;
import com.linkedin.settings.global.OidcSettings;
import com.linkedin.settings.global.SharedApiKeyAiPluginConfig;
import com.linkedin.settings.global.SlackIntegrationSettings;
import com.linkedin.settings.global.SsoSettings;
import com.linkedin.settings.global.TeamsChannel;
import com.linkedin.settings.global.TeamsIntegrationSettings;
import com.linkedin.settings.global.UserApiKeyAiPluginConfig;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.services.SecretService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateGlobalSettingsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;
  private final SecretService _secretService;
  private final IntegrationsService _integrationsService;

  public UpdateGlobalSettingsResolver(
      final EntityClient entityClient,
      final SecretService secretService,
      final IntegrationsService integrationsService) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    _secretService = Objects.requireNonNull(secretService, "secretService must not be null");
    _integrationsService =
        Objects.requireNonNull(integrationsService, "integrationsService must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = getQueryContext(environment);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (SettingsMapper.canManageGlobalSettings(context)) {

            final UpdateGlobalSettingsInput input =
                bindArgument(environment.getArgument("input"), UpdateGlobalSettingsInput.class);

            // First, fetch the existing global settings.
            GlobalSettingsInfo globalSettings =
                SettingsMapper.getGlobalSettings(context.getOperationContext(), _entityClient);

            // Next, patch the global settings.
            updateSettings(globalSettings, input, context);

            // Finally, write it back in a new aspect.
            final MetadataChangeProposal proposal =
                buildMetadataChangeProposalWithUrn(
                    GLOBAL_SETTINGS_URN, GLOBAL_SETTINGS_INFO_ASPECT_NAME, globalSettings);
            try {
              _entityClient.ingestProposal(context.getOperationContext(), proposal, false);
              return true;
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to update global settings! %s", input), e);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void updateSettings(
      final GlobalSettingsInfo existingSettings,
      final UpdateGlobalSettingsInput update,
      @Nonnull final QueryContext context) {
    if (update.getIntegrationSettings() != null) {
      updateGlobalIntegrationSettings(
          existingSettings.getIntegrations(), update.getIntegrationSettings());
    }
    if (update.getNotificationSettings() != null) {
      updateGlobalNotificationSettings(
          existingSettings.getNotifications(), update.getNotificationSettings());
    }
    if (update.getSsoSettings() != null) {
      SsoSettings existingSsoSettings =
          existingSettings.hasSso() ? existingSettings.getSso() : new SsoSettings();
      updateSsoSettings(existingSsoSettings, update.getSsoSettings());
      existingSettings.setSso(existingSsoSettings);
    }
    if (update.getDocumentationAi() != null) {
      DocumentationAiSettings docAiSettings =
          existingSettings.hasDocumentationAi()
              ? existingSettings.getDocumentationAi()
              : new DocumentationAiSettings();
      updateDocumentationAiSettings(docAiSettings, update.getDocumentationAi(), context);
      existingSettings.setDocumentationAi(docAiSettings);
    }
    if (update.getAiAssistant() != null) {
      AiAssistantSettings aiAssistantSettings =
          existingSettings.hasAiAssistant()
              ? existingSettings.getAiAssistant()
              : new AiAssistantSettings();
      updateAiAssistantSettings(aiAssistantSettings, update.getAiAssistant(), context);
      existingSettings.setAiAssistant(aiAssistantSettings);
    }
    if (update.getAiPlugins() != null) {
      updateAiPlugins(existingSettings, update.getAiPlugins());
    }
  }

  private void updateGlobalIntegrationSettings(
      final GlobalIntegrationSettings existingSettings,
      final UpdateGlobalIntegrationSettingsInput update) {
    if (update.getSlackSettings() != null) {
      SlackIntegrationSettings existingSlackSettings =
          existingSettings.hasSlackSettings()
              ? existingSettings.getSlackSettings()
              : new SlackIntegrationSettings();
      updateSlackIntegrationSettings(existingSlackSettings, update.getSlackSettings());
      existingSettings.setSlackSettings(existingSlackSettings);
    }
    if (update.getEmailSettings() != null) {
      EmailIntegrationSettings existingEmailSettings =
          existingSettings.hasEmailSettings()
              ? existingSettings.getEmailSettings()
              : new EmailIntegrationSettings();
      updateEmailIntegrationSettings(existingEmailSettings, update.getEmailSettings());
      existingSettings.setEmailSettings(existingEmailSettings);
    }
    if (update.getTeamsSettings() != null) {
      TeamsIntegrationSettings existingTeamsSettings =
          existingSettings.hasTeamsSettings()
              ? existingSettings.getTeamsSettings()
              : new TeamsIntegrationSettings();
      updateTeamsIntegrationSettings(existingTeamsSettings, update.getTeamsSettings());
      existingSettings.setTeamsSettings(existingTeamsSettings);
    }
  }

  private void updateSlackIntegrationSettings(
      final SlackIntegrationSettings existingSettings,
      final UpdateSlackIntegrationSettingsInput update) {
    boolean hasSlackChanges = false;

    existingSettings.setEnabled(true);
    if (update.getDefaultChannelName() != null) {
      existingSettings.setDefaultChannelName(update.getDefaultChannelName());
      hasSlackChanges = true;
    }
    if (update.getBotToken() != null) {
      existingSettings.setEncryptedBotToken(_secretService.encrypt(update.getBotToken()));
      hasSlackChanges = true;
    }
    if (update.getDatahubAtMentionEnabled() != null) {
      Boolean oldValue =
          existingSettings.hasDatahubAtMentionEnabled()
              ? existingSettings.isDatahubAtMentionEnabled()
              : null;
      Boolean newValue = update.getDatahubAtMentionEnabled();

      existingSettings.setDatahubAtMentionEnabled(newValue);

      // Check if the setting actually changed
      if (!Objects.equals(oldValue, newValue)) {
        hasSlackChanges = true;
        log.info("Slack @DataHub mention setting changed from {} to {}", oldValue, newValue);
      }
    }

    // Trigger reload if any Slack settings changed
    if (hasSlackChanges) {
      try {
        log.info("Triggering integrations service reload due to Slack settings changes");
        _integrationsService.reloadCredentials();
      } catch (Exception e) {
        log.warn(
            "Failed to reload integrations service credentials after Slack settings update", e);
        // Don't fail the update operation if reload fails
      }
    }
  }

  private void updateEmailIntegrationSettings(
      final EmailIntegrationSettings existingSettings,
      final UpdateEmailIntegrationSettingsInput update) {
    existingSettings.setDefaultEmail(update.getDefaultEmail(), SetMode.IGNORE_NULL);
  }

  private void updateTeamsIntegrationSettings(
      final TeamsIntegrationSettings existingSettings,
      final UpdateTeamsIntegrationSettingsInput update) {
    existingSettings.setEnabled(true);
    if (update.getDefaultChannel() != null) {
      TeamsChannel defaultChannel =
          existingSettings.hasDefaultChannel()
              ? existingSettings.getDefaultChannel()
              : new TeamsChannel();
      updateTeamsChannel(defaultChannel, update.getDefaultChannel());
      existingSettings.setDefaultChannel(defaultChannel);
    }
  }

  private void updateTeamsChannel(
      final TeamsChannel existingChannel, final TeamsChannelInput update) {
    if (update.getId() != null) {
      existingChannel.setId(update.getId());
    }
    if (update.getName() != null) {
      existingChannel.setName(update.getName(), SetMode.IGNORE_NULL);
      // TODO: Set lastUpdated timestamp for TTL cache invalidation
      // This would require adding a lastUpdated field to the PDL model
    }
  }

  private void updateGlobalNotificationSettings(
      final GlobalNotificationSettings existingSettings,
      final UpdateGlobalNotificationSettingsInput update) {
    if (update.getSettings() != null) {
      NotificationSettingMap newSettings =
          NotificationSettingMapMapper.mapNotificationSettingInputList(update.getSettings());
      if (existingSettings.hasSettings()) {
        // Simply overwrite what's already there.
        existingSettings.getSettings().putAll(newSettings);
      } else {
        existingSettings.setSettings(newSettings);
      }
    }
  }

  private void updateSsoSettings(
      final SsoSettings ssoSettings, final UpdateSsoSettingsInput update) {
    ssoSettings.setBaseUrl(update.getBaseUrl());

    if (update.getOidcSettings() != null) {
      OidcSettings oidcSettings =
          ssoSettings.hasOidcSettings() ? ssoSettings.getOidcSettings() : new OidcSettings();
      updateOidcSettings(oidcSettings, update.getOidcSettings());
      ssoSettings.setOidcSettings(oidcSettings);
    }
  }

  private void updateOidcSettings(
      final OidcSettings oidcSettings, final UpdateOidcSettingsInput update) {
    oidcSettings.setEnabled(update.getEnabled());
    oidcSettings.setClientId(update.getClientId());
    oidcSettings.setClientSecret(_secretService.encrypt(update.getClientSecret()));
    oidcSettings.setDiscoveryUri(update.getDiscoveryUri());
    if (update.getUserNameClaim() != null) {
      oidcSettings.setUserNameClaim(update.getUserNameClaim());
    }
    if (update.getUserNameClaimRegex() != null) {
      oidcSettings.setUserNameClaimRegex(update.getUserNameClaimRegex());
    }
    if (update.getScope() != null) {
      oidcSettings.setScope(update.getScope());
    }
    if (update.getClientAuthenticationMethod() != null) {
      oidcSettings.setClientAuthenticationMethod(update.getClientAuthenticationMethod());
    }
    if (update.getJitProvisioningEnabled() != null) {
      oidcSettings.setJitProvisioningEnabled(update.getJitProvisioningEnabled());
    }
    if (update.getPreProvisioningRequired() != null) {
      oidcSettings.setPreProvisioningRequired(update.getPreProvisioningRequired());
    }
    if (update.getExtractGroupsEnabled() != null) {
      oidcSettings.setExtractGroupsEnabled(update.getExtractGroupsEnabled());
    }
    if (update.getGroupsClaim() != null) {
      oidcSettings.setGroupsClaim(update.getGroupsClaim());
    }
    if (update.getResponseType() != null) {
      oidcSettings.setResponseType(update.getResponseType());
    }
    if (update.getResponseMode() != null) {
      oidcSettings.setResponseMode(update.getResponseMode());
    }
    if (update.getUseNonce() != null) {
      oidcSettings.setUseNonce(update.getUseNonce());
    }
    if (update.getReadTimeout() != null) {
      oidcSettings.setReadTimeout(update.getReadTimeout());
    }
    if (update.getExtractJwtAccessTokenClaims() != null) {
      oidcSettings.setExtractJwtAccessTokenClaims(update.getExtractJwtAccessTokenClaims());
    }
    if (update.getPreferredJwsAlgorithm() != null) {
      oidcSettings.setPreferredJwsAlgorithm2(update.getPreferredJwsAlgorithm());
    }
    oidcSettings.removePreferredJwsAlgorithm();
  }

  private void updateDocumentationAiSettings(
      final DocumentationAiSettings existingSettings,
      final UpdateDocumentationAiSettingsInput update,
      @Nonnull final QueryContext context) {
    if (update.getEnabled() != null) {
      existingSettings.setEnabled(update.getEnabled());
    }
    if (update.getInstructions() != null) {
      // Validate instructions before processing
      AiInstructionValidationUtils.validateAiInstructions(update.getInstructions());

      // Replace entire set of instructions (not append)
      AiInstructionArray instructions = mapAiInstructionInputs(update.getInstructions(), context);
      existingSettings.setInstructions(instructions);
    }
  }

  private void updateAiAssistantSettings(
      final AiAssistantSettings existingSettings,
      final UpdateAiAssistantSettingsInput update,
      @Nonnull final QueryContext context) {
    if (update.getInstructions() != null) {
      // Validate instructions before processing
      AiInstructionValidationUtils.validateAiInstructions(update.getInstructions());

      // Replace entire set of instructions (not append)
      AiInstructionArray instructions = mapAiInstructionInputs(update.getInstructions(), context);
      existingSettings.setInstructions(instructions);
    }
  }

  private AiInstructionArray mapAiInstructionInputs(
      java.util.List<AiInstructionInput> inputs, @Nonnull final QueryContext context) {
    AiInstructionArray instructions = new AiInstructionArray();
    final long currentTimeMs = System.currentTimeMillis();
    final Urn actor = UrnUtils.getUrn(context.getActorUrn());

    for (AiInstructionInput input : inputs) {
      AiInstruction instruction = new AiInstruction();

      // Set ID - use provided ID or generate UUID
      String id = input.getId();
      if (id == null || id.trim().isEmpty()) {
        id = UUID.randomUUID().toString();
      }
      instruction.setId(id);
      instruction.setType(AiInstructionType.valueOf(input.getType().toString()));

      // Set state - use provided state or default to ACTIVE
      com.linkedin.datahub.graphql.generated.AiInstructionState graphqlState = input.getState();
      com.linkedin.settings.global.AiInstructionState pdlState;
      if (graphqlState == null) {
        pdlState = com.linkedin.settings.global.AiInstructionState.ACTIVE;
      } else {
        pdlState = mapGraphqlStateToPdlState(graphqlState);
      }
      instruction.setState(pdlState);

      // Set instruction text
      instruction.setInstruction(input.getInstruction());

      // Set audit stamps (server-managed)
      AuditStamp auditStamp = new AuditStamp();
      auditStamp.setTime(currentTimeMs);
      auditStamp.setActor(actor);
      instruction.setCreated(auditStamp);
      instruction.setLastModified(auditStamp);

      instructions.add(instruction);
    }
    return instructions;
  }

  private com.linkedin.settings.global.AiInstructionState mapGraphqlStateToPdlState(
      com.linkedin.datahub.graphql.generated.AiInstructionState graphqlState) {
    switch (graphqlState) {
      case ACTIVE:
        return com.linkedin.settings.global.AiInstructionState.ACTIVE;
      case INACTIVE:
        return com.linkedin.settings.global.AiInstructionState.INACTIVE;
      default:
        throw new IllegalArgumentException("Unknown GraphQL AiInstructionState: " + graphqlState);
    }
  }

  /**
   * Updates AI plugin configurations using merge/upsert semantics.
   *
   * <p>Per the GraphQL schema documentation: "Plugins not included will remain unchanged." This
   * method merges the provided updates with existing plugins by ID:
   *
   * <ul>
   *   <li>Plugins in updates with matching ID → replaced with updated config
   *   <li>Plugins in updates with new ID → added
   *   <li>Existing plugins not in updates → preserved unchanged
   * </ul>
   *
   * @param existingSettings The current global settings
   * @param updates The plugin configurations to upsert
   */
  private void updateAiPlugins(
      final GlobalSettingsInfo existingSettings, final List<AiPluginConfigInput> updates) {
    // Get or create the aiPluginSettings wrapper
    AiPluginSettings pluginSettings =
        existingSettings.hasAiPluginSettings() && existingSettings.getAiPluginSettings() != null
            ? existingSettings.getAiPluginSettings()
            : new AiPluginSettings();

    // Build a map of existing plugins by ID for efficient lookup
    Map<String, AiPluginConfig> existingPluginsById = new HashMap<>();
    if (pluginSettings.hasPlugins() && pluginSettings.getPlugins() != null) {
      for (AiPluginConfig existing : pluginSettings.getPlugins()) {
        existingPluginsById.put(existing.getId(), existing);
      }
    }

    // Apply updates (upsert by ID)
    for (AiPluginConfigInput input : updates) {
      AiPluginConfig config = mapAiPluginConfigInput(input);
      existingPluginsById.put(config.getId(), config); // Replace or add
    }

    // Build final plugins array from merged map
    AiPluginConfigArray pluginsArray = new AiPluginConfigArray(existingPluginsById.values());

    pluginSettings.setPlugins(pluginsArray);
    existingSettings.setAiPluginSettings(pluginSettings);
  }

  private AiPluginConfig mapAiPluginConfigInput(final AiPluginConfigInput input) {
    AiPluginConfig config = new AiPluginConfig();

    config.setId(input.getId());
    config.setType(AiPluginType.valueOf(input.getType().toString()));
    config.setServiceUrn(UrnUtils.getUrn(input.getServiceUrn()));
    config.setEnabled(input.getEnabled() != null ? input.getEnabled() : true);

    if (input.getInstructions() != null) {
      config.setInstructions(input.getInstructions());
    }

    config.setAuthType(AiPluginAuthType.valueOf(input.getAuthType().toString()));

    // Map oauthConfig (for USER_OAUTH)
    if (input.getOauthConfig() != null) {
      OAuthAiPluginConfig oauthConfig = new OAuthAiPluginConfig();
      oauthConfig.setServerUrn(UrnUtils.getUrn(input.getOauthConfig().getServerUrn()));
      if (input.getOauthConfig().getRequiredScopes() != null
          && !input.getOauthConfig().getRequiredScopes().isEmpty()) {
        oauthConfig.setRequiredScopes(new StringArray(input.getOauthConfig().getRequiredScopes()));
      }
      config.setOauthConfig(oauthConfig);
    }

    // Map sharedApiKeyConfig (for SHARED_API_KEY)
    if (input.getSharedApiKeyConfig() != null) {
      SharedApiKeyAiPluginConfig sharedConfig = new SharedApiKeyAiPluginConfig();
      if (input.getSharedApiKeyConfig().getCredentialUrn() != null) {
        sharedConfig.setCredentialUrn(
            UrnUtils.getUrn(input.getSharedApiKeyConfig().getCredentialUrn()));
      }

      // Map auth injection settings
      if (input.getSharedApiKeyConfig().getAuthLocation() != null) {
        sharedConfig.setAuthLocation(
            com.linkedin.settings.global.AuthInjectionLocation.valueOf(
                input.getSharedApiKeyConfig().getAuthLocation().toString()));
      }
      if (input.getSharedApiKeyConfig().getAuthHeaderName() != null) {
        sharedConfig.setAuthHeaderName(input.getSharedApiKeyConfig().getAuthHeaderName());
      }
      if (input.getSharedApiKeyConfig().getAuthScheme() != null) {
        sharedConfig.setAuthScheme(input.getSharedApiKeyConfig().getAuthScheme());
      }
      if (input.getSharedApiKeyConfig().getAuthQueryParam() != null) {
        sharedConfig.setAuthQueryParam(input.getSharedApiKeyConfig().getAuthQueryParam());
      }

      config.setSharedApiKeyConfig(sharedConfig);
    }

    // Map userApiKeyConfig (for USER_API_KEY)
    if (input.getUserApiKeyConfig() != null) {
      UserApiKeyAiPluginConfig userConfig = new UserApiKeyAiPluginConfig();

      // Map auth injection settings
      if (input.getUserApiKeyConfig().getAuthLocation() != null) {
        userConfig.setAuthLocation(
            com.linkedin.settings.global.AuthInjectionLocation.valueOf(
                input.getUserApiKeyConfig().getAuthLocation().toString()));
      }
      if (input.getUserApiKeyConfig().getAuthHeaderName() != null) {
        userConfig.setAuthHeaderName(input.getUserApiKeyConfig().getAuthHeaderName());
      }
      if (input.getUserApiKeyConfig().getAuthScheme() != null) {
        userConfig.setAuthScheme(input.getUserApiKeyConfig().getAuthScheme());
      }
      if (input.getUserApiKeyConfig().getAuthQueryParam() != null) {
        userConfig.setAuthQueryParam(input.getUserApiKeyConfig().getAuthQueryParam());
      }

      config.setUserApiKeyConfig(userConfig);
    }

    return config;
  }
}

package com.linkedin.datahub.graphql.resolvers.service;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.getQueryContext;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.buildMetadataChangeProposalWithUrn;
import static com.linkedin.metadata.Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_SETTINGS_URN;

import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.connection.DataHubConnectionDetailsType;
import com.linkedin.connection.DataHubJsonConnection;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AiPluginAuthType;
import com.linkedin.datahub.graphql.generated.McpServerPropertiesInput;
import com.linkedin.datahub.graphql.generated.Service;
import com.linkedin.datahub.graphql.generated.ServiceSubType;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.generated.UpsertAiPluginInput;
import com.linkedin.datahub.graphql.generated.UpsertOAuthAuthorizationServerInput;
import com.linkedin.datahub.graphql.resolvers.connection.ConnectionUtils;
import com.linkedin.datahub.graphql.resolvers.settings.SettingsMapper;
import com.linkedin.datahub.graphql.types.service.ServiceType;
import com.linkedin.datahub.graphql.types.service.mappers.ServiceMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.connection.ConnectionService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.oauth.AuthLocation;
import com.linkedin.oauth.OAuthAuthorizationServerProperties;
import com.linkedin.oauth.TokenAuthMethod;
import com.linkedin.service.McpServerProperties;
import com.linkedin.service.McpTransport;
import com.linkedin.service.ServiceProperties;
import com.linkedin.settings.global.AiPluginConfig;
import com.linkedin.settings.global.AiPluginConfigArray;
import com.linkedin.settings.global.AiPluginSettings;
import com.linkedin.settings.global.AiPluginType;
import com.linkedin.settings.global.AuthInjectionLocation;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.OAuthAiPluginConfig;
import com.linkedin.settings.global.SharedApiKeyAiPluginConfig;
import com.linkedin.settings.global.UserApiKeyAiPluginConfig;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.services.SecretService;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Creates or updates a Service entity. */
@Slf4j
public class UpsertAiPluginResolver implements DataFetcher<CompletableFuture<Service>> {

  private static final Urn AI_PLUGIN_PLATFORM_URN = UrnUtils.getUrn("urn:li:dataPlatform:datahub");

  private final EntityClient entityClient;
  private final SecretService secretService;
  private final ConnectionService connectionService;

  public UpsertAiPluginResolver(
      @Nonnull final EntityClient entityClient,
      @Nonnull final SecretService secretService,
      @Nonnull final ConnectionService connectionService) {
    this.entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    this.secretService = Objects.requireNonNull(secretService, "secretService must not be null");
    this.connectionService =
        Objects.requireNonNull(connectionService, "connectionService must not be null");
  }

  @Override
  public CompletableFuture<Service> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = getQueryContext(environment);
    final UpsertAiPluginInput input =
        bindArgument(environment.getArgument("input"), UpsertAiPluginInput.class);

    // Reuse MANAGE_CONNECTIONS privilege since Services are external connections
    if (!ConnectionUtils.canManageConnections(context)) {
      throw new AuthorizationException(
          "Unauthorized to create/update services. Please contact your DataHub administrator.");
    }

    // Validate input before any writes to avoid inconsistent entity state
    if (input.getSubType() == ServiceSubType.MCP_SERVER && input.getMcpServerProperties() == null) {
      throw new IllegalArgumentException(
          "mcpServerProperties is required when subType is MCP_SERVER");
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            // Generate URN
            final String serviceId =
                input.getId() != null ? input.getId() : UUID.randomUUID().toString();
            final Urn serviceUrn = UrnUtils.getUrn(String.format("urn:li:service:%s", serviceId));

            // Build ServiceProperties aspect
            final ServiceProperties serviceProperties = new ServiceProperties();
            serviceProperties.setDisplayName(input.getDisplayName());
            if (input.getDescription() != null) {
              serviceProperties.setDescription(input.getDescription());
            }

            // Ingest ServiceProperties
            final MetadataChangeProposal servicePropertiesMcp =
                buildMetadataChangeProposalWithUrn(
                    serviceUrn, Constants.SERVICE_PROPERTIES_ASPECT_NAME, serviceProperties);
            entityClient.ingestProposal(context.getOperationContext(), servicePropertiesMcp, false);

            // Build and ingest SubTypes aspect (using standard aspect for consistency)
            final SubTypes subTypes = new SubTypes();
            subTypes.setTypeNames(
                new com.linkedin.data.template.StringArray(input.getSubType().toString()));
            final MetadataChangeProposal subTypesMcp =
                buildMetadataChangeProposalWithUrn(
                    serviceUrn, Constants.SUB_TYPES_ASPECT_NAME, subTypes);
            entityClient.ingestProposal(context.getOperationContext(), subTypesMcp, false);

            // Build and ingest McpServerProperties if subType is MCP_SERVER
            if (input.getSubType() == ServiceSubType.MCP_SERVER) {
              // Note: mcpServerProperties is validated at the start of this method
              final McpServerProperties mcpProperties =
                  buildMcpServerProperties(input.getMcpServerProperties());
              final MetadataChangeProposal mcpPropertiesMcp =
                  buildMetadataChangeProposalWithUrn(
                      serviceUrn, Constants.MCP_SERVER_PROPERTIES_ASPECT_NAME, mcpProperties);
              entityClient.ingestProposal(context.getOperationContext(), mcpPropertiesMcp, false);
            }

            // Also add/update AiPluginConfig in GlobalSettings
            // Note: This is a separate operation from entity creation. If this fails,
            // the Service entity will exist but won't be configured as an AI plugin.
            // This is recoverable by retrying the upsert operation.
            try {
              updateGlobalSettingsAiPlugin(context, serviceUrn, input);
            } catch (Exception e) {
              log.error(
                  "Failed to update GlobalSettings.aiPlugins for service {}. "
                      + "Service entity was created but is not configured as an AI plugin. "
                      + "Retry the operation to fix this inconsistency.",
                  serviceUrn,
                  e);
              throw new RuntimeException(
                  String.format(
                      "Service %s was created but failed to configure as AI plugin. "
                          + "Please retry the operation.",
                      serviceUrn),
                  e);
            }

            // Fetch and return the created/updated entity
            final Map<Urn, EntityResponse> entities =
                entityClient.batchGetV2(
                    context.getOperationContext(),
                    Constants.SERVICE_ENTITY_NAME,
                    new HashSet<>(List.of(serviceUrn)),
                    ServiceType.ASPECTS_TO_FETCH);

            final EntityResponse response = entities.get(serviceUrn);
            if (response == null) {
              throw new RuntimeException(
                  String.format("Failed to retrieve created service: %s", serviceUrn));
            }

            return ServiceMapper.map(context, response);

          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to upsert service from input %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private McpServerProperties buildMcpServerProperties(McpServerPropertiesInput input) {
    final McpServerProperties result = new McpServerProperties();
    result.setUrl(input.getUrl());

    if (input.getTransport() != null) {
      result.setTransport(McpTransport.valueOf(input.getTransport().toString()));
    } else {
      result.setTransport(McpTransport.HTTP);
    }

    if (input.getTimeout() != null) {
      result.setTimeout(input.getTimeout().floatValue());
    } else {
      result.setTimeout(30.0f);
    }

    if (input.getCustomHeaders() != null && !input.getCustomHeaders().isEmpty()) {
      final StringMap headersMap = new StringMap();
      for (StringMapEntryInput entry : input.getCustomHeaders()) {
        headersMap.put(entry.getKey(), entry.getValue());
      }
      result.setCustomHeaders(headersMap);
    }

    return result;
  }

  /**
   * Updates GlobalSettings.aiPlugins to add/update an AiPluginConfig for this service. This ensures
   * the service is visible in the AI plugins list.
   *
   * <p>If newOAuthServer is provided, creates the OAuth server first and uses its URN.
   *
   * <p><b>Known Limitation:</b> This method uses a read-modify-write pattern without locking.
   * Concurrent service creations by multiple admins could result in one update overwriting another.
   * This is acceptable for Phase 1 because:
   *
   * <ul>
   *   <li>Admin operations are low frequency
   *   <li>The "lost" service entity still exists and can be re-added by retrying the upsert
   *   <li>Proper distributed locking will be added in Phase 2 when Redis infrastructure is
   *       available
   * </ul>
   */
  private void updateGlobalSettingsAiPlugin(
      final QueryContext context, final Urn serviceUrn, final UpsertAiPluginInput input)
      throws Exception {

    // If newOAuthServer is provided, create it first
    Urn oauthServerUrn = null;
    if (input.getNewOAuthServer() != null) {
      oauthServerUrn = createOAuthServer(context, input.getNewOAuthServer());
      log.info("Created inline OAuth server {} for service {}", oauthServerUrn, serviceUrn);
    } else if (input.getOauthServerUrn() != null) {
      oauthServerUrn = UrnUtils.getUrn(input.getOauthServerUrn());
    }

    // If sharedApiKey is provided, create a DataHubConnection owned by the service
    Urn sharedCredentialUrn = null;
    if (input.getSharedApiKey() != null && !input.getSharedApiKey().isEmpty()) {
      sharedCredentialUrn = createSharedCredential(context, serviceUrn, input.getSharedApiKey());
      log.info("Created shared credential {} for service {}", sharedCredentialUrn, serviceUrn);
    }

    // Fetch existing GlobalSettings
    GlobalSettingsInfo globalSettings =
        SettingsMapper.getGlobalSettings(context.getOperationContext(), entityClient);

    // Get or create the aiPluginSettings wrapper
    AiPluginSettings pluginSettings =
        globalSettings.hasAiPluginSettings()
            ? globalSettings.getAiPluginSettings()
            : new AiPluginSettings();

    // Get existing plugins or create empty array
    AiPluginConfigArray pluginsArray =
        pluginSettings.hasPlugins() ? pluginSettings.getPlugins() : new AiPluginConfigArray();

    // Find and remove existing config for this service (for updates), but keep a reference
    // to preserve credentials if not re-provided
    AiPluginConfig existingConfig = null;
    for (AiPluginConfig c : pluginsArray) {
      if (c.getServiceUrn().equals(serviceUrn)) {
        existingConfig = c;
        break;
      }
    }
    pluginsArray.removeIf(c -> c.getServiceUrn().equals(serviceUrn));

    // Build the AiPluginConfig
    AiPluginConfig config = new AiPluginConfig();

    // Set ID (use service URN string for easy correlation)
    config.setId(serviceUrn.toString());

    // Map subType to AiPluginType
    if (input.getSubType() == ServiceSubType.MCP_SERVER) {
      config.setType(AiPluginType.MCP_SERVER);
    }

    config.setServiceUrn(serviceUrn);
    config.setEnabled(input.getEnabled() != null ? input.getEnabled() : true);

    if (input.getInstructions() != null) {
      config.setInstructions(input.getInstructions());
    }

    // Map authType
    AiPluginAuthType authType = input.getAuthType();
    if (authType == null) {
      authType = AiPluginAuthType.NONE;
    }
    config.setAuthType(com.linkedin.settings.global.AiPluginAuthType.valueOf(authType.toString()));

    // Build auth config based on authType
    if (authType == AiPluginAuthType.SHARED_API_KEY) {
      // Determine which credential URN to use:
      // 1. New credential if a new API key was provided
      // 2. Existing credential from previous config if no new key provided
      Urn credentialUrnToUse = sharedCredentialUrn;
      SharedApiKeyAiPluginConfig existingSharedConfig = null;

      if (credentialUrnToUse == null
          && existingConfig != null
          && existingConfig.hasSharedApiKeyConfig()) {
        existingSharedConfig = existingConfig.getSharedApiKeyConfig();
        if (existingSharedConfig.hasCredentialUrn()) {
          credentialUrnToUse = existingSharedConfig.getCredentialUrn();
          log.info(
              "Preserving existing credential {} for service {}", credentialUrnToUse, serviceUrn);
        }
      }

      if (credentialUrnToUse != null) {
        // Shared API key config - auth injection settings are embedded directly
        SharedApiKeyAiPluginConfig sharedConfig = new SharedApiKeyAiPluginConfig();
        sharedConfig.setCredentialUrn(credentialUrnToUse);

        // Map auth injection settings from input, falling back to existing config if not provided
        if (input.getSharedApiKeyAuthLocation() != null) {
          sharedConfig.setAuthLocation(
              AuthInjectionLocation.valueOf(input.getSharedApiKeyAuthLocation().toString()));
        } else if (existingSharedConfig != null && existingSharedConfig.hasAuthLocation()) {
          sharedConfig.setAuthLocation(existingSharedConfig.getAuthLocation());
        }

        if (input.getSharedApiKeyAuthHeaderName() != null) {
          sharedConfig.setAuthHeaderName(input.getSharedApiKeyAuthHeaderName());
        } else if (existingSharedConfig != null && existingSharedConfig.hasAuthHeaderName()) {
          sharedConfig.setAuthHeaderName(existingSharedConfig.getAuthHeaderName());
        }

        if (input.getSharedApiKeyAuthScheme() != null) {
          sharedConfig.setAuthScheme(input.getSharedApiKeyAuthScheme());
        } else if (existingSharedConfig != null && existingSharedConfig.hasAuthScheme()) {
          sharedConfig.setAuthScheme(existingSharedConfig.getAuthScheme());
        }

        if (input.getSharedApiKeyAuthQueryParam() != null) {
          sharedConfig.setAuthQueryParam(input.getSharedApiKeyAuthQueryParam());
        } else if (existingSharedConfig != null && existingSharedConfig.hasAuthQueryParam()) {
          sharedConfig.setAuthQueryParam(existingSharedConfig.getAuthQueryParam());
        }

        config.setSharedApiKeyConfig(sharedConfig);
      }
    } else if (authType == AiPluginAuthType.USER_API_KEY) {
      // User API key config - auth injection settings are embedded directly
      UserApiKeyAiPluginConfig userConfig = new UserApiKeyAiPluginConfig();

      // Get existing user API key config for preservation
      UserApiKeyAiPluginConfig existingUserConfig = null;
      if (existingConfig != null && existingConfig.hasUserApiKeyConfig()) {
        existingUserConfig = existingConfig.getUserApiKeyConfig();
      }

      // Map auth injection settings from input, falling back to existing config if not provided
      if (input.getUserApiKeyAuthLocation() != null) {
        userConfig.setAuthLocation(
            AuthInjectionLocation.valueOf(input.getUserApiKeyAuthLocation().toString()));
      } else if (existingUserConfig != null && existingUserConfig.hasAuthLocation()) {
        userConfig.setAuthLocation(existingUserConfig.getAuthLocation());
      }

      if (input.getUserApiKeyAuthHeaderName() != null) {
        userConfig.setAuthHeaderName(input.getUserApiKeyAuthHeaderName());
      } else if (existingUserConfig != null && existingUserConfig.hasAuthHeaderName()) {
        userConfig.setAuthHeaderName(existingUserConfig.getAuthHeaderName());
      }

      if (input.getUserApiKeyAuthScheme() != null) {
        userConfig.setAuthScheme(input.getUserApiKeyAuthScheme());
      } else if (existingUserConfig != null && existingUserConfig.hasAuthScheme()) {
        userConfig.setAuthScheme(existingUserConfig.getAuthScheme());
      }

      if (input.getUserApiKeyAuthQueryParam() != null) {
        userConfig.setAuthQueryParam(input.getUserApiKeyAuthQueryParam());
      } else if (existingUserConfig != null && existingUserConfig.hasAuthQueryParam()) {
        userConfig.setAuthQueryParam(existingUserConfig.getAuthQueryParam());
      }

      config.setUserApiKeyConfig(userConfig);
    } else if (authType == AiPluginAuthType.USER_OAUTH) {
      // OAuth config
      OAuthAiPluginConfig oauthConfig = new OAuthAiPluginConfig();

      // Get existing OAuth config for preservation
      OAuthAiPluginConfig existingOAuthConfig = null;
      if (existingConfig != null && existingConfig.hasOauthConfig()) {
        existingOAuthConfig = existingConfig.getOauthConfig();
      }

      // Preserve serverUrn from existing config if not provided in input
      Urn serverUrnToUse = oauthServerUrn;
      if (serverUrnToUse == null
          && existingOAuthConfig != null
          && existingOAuthConfig.hasServerUrn()) {
        serverUrnToUse = existingOAuthConfig.getServerUrn();
        log.info("Preserving existing OAuth server {} for service {}", serverUrnToUse, serviceUrn);
      }

      if (serverUrnToUse != null) {
        oauthConfig.setServerUrn(serverUrnToUse);

        // Preserve requiredScopes from existing config if not provided in input
        if (input.getRequiredScopes() != null && !input.getRequiredScopes().isEmpty()) {
          oauthConfig.setRequiredScopes(
              new com.linkedin.data.template.StringArray(input.getRequiredScopes()));
        } else if (existingOAuthConfig != null && existingOAuthConfig.hasRequiredScopes()) {
          oauthConfig.setRequiredScopes(existingOAuthConfig.getRequiredScopes());
        }

        config.setOauthConfig(oauthConfig);
      }
    }

    // Add the config to the array
    pluginsArray.add(config);
    pluginSettings.setPlugins(pluginsArray);
    globalSettings.setAiPluginSettings(pluginSettings);

    // Write back to GlobalSettings
    final MetadataChangeProposal proposal =
        buildMetadataChangeProposalWithUrn(
            GLOBAL_SETTINGS_URN, GLOBAL_SETTINGS_INFO_ASPECT_NAME, globalSettings);
    entityClient.ingestProposal(context.getOperationContext(), proposal, false);

    log.info("Added/updated AiPluginConfig for service {} in GlobalSettings", serviceUrn);
  }

  /**
   * Creates a NEW OAuthAuthorizationServer entity from inline input. This method is for creation
   * only - to update an existing OAuth server, use the upsertOAuthAuthorizationServer mutation
   * directly.
   *
   * @return URN of the created OAuth server
   * @throws IllegalArgumentException if an ID is provided (use update mutation instead)
   */
  private Urn createOAuthServer(
      final QueryContext context, final UpsertOAuthAuthorizationServerInput input)
      throws Exception {

    // This method only creates new OAuth servers - updates should go through the dedicated mutation
    if (input.getId() != null) {
      throw new IllegalArgumentException(
          "Cannot update existing OAuth server via newOAuthServer. "
              + "Use upsertOAuthAuthorizationServer mutation to update, then link via oauthServerUrn.");
    }

    // Generate URN for new server
    final String serverId = UUID.randomUUID().toString();
    final Urn serverUrn =
        UrnUtils.getUrn(String.format("urn:li:oauthAuthorizationServer:%s", serverId));

    // Build OAuthAuthorizationServerProperties aspect
    final OAuthAuthorizationServerProperties properties = new OAuthAuthorizationServerProperties();

    // Basic properties
    properties.setDisplayName(input.getDisplayName());
    if (input.getDescription() != null) {
      properties.setDescription(input.getDescription());
    }

    // Handle OAuth client secret - encrypt and store as DataHubSecret
    if (input.getClientSecret() != null && !input.getClientSecret().isEmpty()) {
      final Urn secretUrn =
          createSecret(context, serverUrn, "clientSecret", input.getClientSecret());
      properties.setClientSecretUrn(secretUrn);
    }

    // OAuth config
    if (input.getClientId() != null) {
      properties.setClientId(input.getClientId());
    }
    if (input.getAuthorizationUrl() != null) {
      properties.setAuthorizationUrl(input.getAuthorizationUrl());
    }
    if (input.getTokenUrl() != null) {
      properties.setTokenUrl(input.getTokenUrl());
    }
    if (input.getScopes() != null) {
      properties.setScopes(new StringArray(input.getScopes()));
    }

    // Token auth method
    if (input.getTokenAuthMethod() != null) {
      properties.setTokenAuthMethod(TokenAuthMethod.valueOf(input.getTokenAuthMethod().toString()));
    } else {
      properties.setTokenAuthMethod(TokenAuthMethod.POST_BODY);
    }

    // Additional parameters
    if (input.getAdditionalTokenParams() != null) {
      properties.setAdditionalTokenParams(
          buildStringMapFromEntries(input.getAdditionalTokenParams()));
    }
    if (input.getAdditionalAuthParams() != null) {
      properties.setAdditionalAuthParams(
          buildStringMapFromEntries(input.getAdditionalAuthParams()));
    }

    // Auth injection config
    if (input.getAuthLocation() != null) {
      properties.setAuthLocation(AuthLocation.valueOf(input.getAuthLocation().toString()));
    } else {
      properties.setAuthLocation(AuthLocation.HEADER);
    }
    if (input.getAuthHeaderName() != null) {
      properties.setAuthHeaderName(input.getAuthHeaderName());
    } else {
      properties.setAuthHeaderName("Authorization");
    }
    if (input.getAuthScheme() != null) {
      properties.setAuthScheme(input.getAuthScheme());
    }
    if (input.getAuthQueryParam() != null) {
      properties.setAuthQueryParam(input.getAuthQueryParam());
    }

    // Ingest properties
    final MetadataChangeProposal propertiesMcp =
        buildMetadataChangeProposalWithUrn(
            serverUrn, Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME, properties);
    entityClient.ingestProposal(context.getOperationContext(), propertiesMcp, false);

    return serverUrn;
  }

  private Urn createSecret(
      final QueryContext context,
      final Urn ownerUrn,
      final String secretName,
      final String secretValue)
      throws Exception {
    // Generate a unique secret URN based on the owner and secret name
    final String secretId =
        String.format("%s_%s_%s", ownerUrn.getId(), secretName, UUID.randomUUID().toString());
    final Urn secretUrn = UrnUtils.getUrn(String.format("urn:li:dataHubSecret:%s", secretId));

    // Build and ingest the secret
    final com.linkedin.secret.DataHubSecretValue secretValueAspect =
        new com.linkedin.secret.DataHubSecretValue();
    secretValueAspect.setName(String.format("%s for %s", secretName, ownerUrn.getId()));
    secretValueAspect.setValue(secretService.encrypt(secretValue));

    final MetadataChangeProposal secretMcp =
        buildMetadataChangeProposalWithUrn(
            secretUrn, Constants.SECRET_VALUE_ASPECT_NAME, secretValueAspect);
    entityClient.ingestProposal(context.getOperationContext(), secretMcp, false);

    return secretUrn;
  }

  private StringMap buildStringMapFromEntries(final List<StringMapEntryInput> entries) {
    final StringMap map = new StringMap();
    if (entries != null) {
      for (StringMapEntryInput entry : entries) {
        map.put(entry.getKey(), entry.getValue());
      }
    }
    return map;
  }

  /**
   * Creates a DataHubConnection to store a shared API key credential.
   *
   * <p>Uses ConnectionService.upsertConnection() which properly handles: - Creating both
   * dataHubConnectionDetails and dataPlatformInstance aspects - Encrypting the entire blob (not
   * just the API key value inside it)
   *
   * @param context Query context
   * @param serviceUrn The service that owns this credential
   * @param apiKey The API key value to store
   * @return URN of the created DataHubConnection
   */
  private Urn createSharedCredential(
      final QueryContext context, final Urn serviceUrn, final String apiKey) {

    // Build the connection ID (ConnectionService will create the full URN)
    final String connectionId = buildServiceConnectionId(serviceUrn.toString(), "apiKey");

    // Build JSON payload with the plaintext API key, then encrypt the entire blob
    // ConnectionService expects the blob to already be encrypted
    final String jsonPayload = String.format("{\"apiKey\":\"%s\"}", apiKey);
    final DataHubJsonConnection json =
        new DataHubJsonConnection().setEncryptedBlob(secretService.encrypt(jsonPayload));

    // Use ConnectionService which properly adds both aspects (details + platform instance)
    return connectionService.upsertConnection(
        context.getOperationContext(),
        connectionId,
        AI_PLUGIN_PLATFORM_URN,
        DataHubConnectionDetailsType.JSON,
        json,
        "Shared API Key - " + serviceUrn);
  }

  /**
   * Builds a DataHubConnection ID for service credentials. Format:
   * &lt;sanitized_service_urn&gt;__&lt;sanitized_credential_type&gt;
   *
   * <p>Uses `__` as separator (not a tuple) because DataHubConnection expects a single-string key.
   * Using tuple format (a,b) causes DataHub to interpret it as a multi-part key which fails since
   * DataHubConnectionKey only has one field.
   *
   * <p>All special characters are sanitized to underscores for URN safety.
   *
   * <p>This must match the Python implementation in credential_store.py's build_connection_id() for
   * consistency.
   */
  private static String buildServiceConnectionId(String serviceUrn, String credentialType) {
    // Sanitize both parts - replace colons, commas, and parentheses with underscores
    String sanitizedServiceUrn =
        serviceUrn.replace(":", "_").replace(",", "_").replace("(", "_").replace(")", "_");
    String sanitizedCredentialType =
        credentialType.replace(":", "_").replace(",", "_").replace("(", "_").replace(")", "_");
    return String.format("%s__%s", sanitizedServiceUrn, sanitizedCredentialType);
  }
}

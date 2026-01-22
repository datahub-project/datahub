package com.linkedin.datahub.graphql.resolvers.service;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.buildMetadataChangeProposalWithUrn;
import static com.linkedin.metadata.Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_SETTINGS_URN;

import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
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
import com.linkedin.datahub.graphql.generated.UpsertOAuthAuthorizationServerInput;
import com.linkedin.datahub.graphql.generated.UpsertServiceInput;
import com.linkedin.datahub.graphql.resolvers.connection.ConnectionUtils;
import com.linkedin.datahub.graphql.resolvers.settings.SettingsMapper;
import com.linkedin.datahub.graphql.types.service.ServiceType;
import com.linkedin.datahub.graphql.types.service.mappers.ServiceMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
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
import com.linkedin.settings.global.GlobalSettingsInfo;
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
public class UpsertServiceResolver implements DataFetcher<CompletableFuture<Service>> {

  private final EntityClient entityClient;
  private final SecretService secretService;

  public UpsertServiceResolver(
      @Nonnull final EntityClient entityClient, @Nonnull final SecretService secretService) {
    this.entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    this.secretService = Objects.requireNonNull(secretService, "secretService must not be null");
  }

  @Override
  public CompletableFuture<Service> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final UpsertServiceInput input =
        bindArgument(environment.getArgument("input"), UpsertServiceInput.class);

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
      final QueryContext context, final Urn serviceUrn, final UpsertServiceInput input)
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

    // Remove existing config for this service if present (for updates)
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
    if (authType == AiPluginAuthType.SHARED_API_KEY && sharedCredentialUrn != null) {
      // Shared API key config - auth injection settings are embedded directly
      com.linkedin.settings.global.SharedApiKeyAiPluginConfig sharedConfig =
          new com.linkedin.settings.global.SharedApiKeyAiPluginConfig();
      sharedConfig.setCredentialUrn(sharedCredentialUrn);

      // Map auth injection settings from input
      if (input.getSharedApiKeyAuthLocation() != null) {
        sharedConfig.setAuthLocation(
            com.linkedin.settings.global.AuthInjectionLocation.valueOf(
                input.getSharedApiKeyAuthLocation().toString()));
      }
      if (input.getSharedApiKeyAuthHeaderName() != null) {
        sharedConfig.setAuthHeaderName(input.getSharedApiKeyAuthHeaderName());
      }
      if (input.getSharedApiKeyAuthScheme() != null) {
        sharedConfig.setAuthScheme(input.getSharedApiKeyAuthScheme());
      }
      if (input.getSharedApiKeyAuthQueryParam() != null) {
        sharedConfig.setAuthQueryParam(input.getSharedApiKeyAuthQueryParam());
      }

      config.setSharedApiKeyConfig(sharedConfig);
    } else if (authType == AiPluginAuthType.USER_API_KEY) {
      // User API key config - auth injection settings are embedded directly
      com.linkedin.settings.global.UserApiKeyAiPluginConfig userConfig =
          new com.linkedin.settings.global.UserApiKeyAiPluginConfig();

      // Map auth injection settings from input
      if (input.getUserApiKeyAuthLocation() != null) {
        userConfig.setAuthLocation(
            com.linkedin.settings.global.AuthInjectionLocation.valueOf(
                input.getUserApiKeyAuthLocation().toString()));
      }
      if (input.getUserApiKeyAuthHeaderName() != null) {
        userConfig.setAuthHeaderName(input.getUserApiKeyAuthHeaderName());
      }
      if (input.getUserApiKeyAuthScheme() != null) {
        userConfig.setAuthScheme(input.getUserApiKeyAuthScheme());
      }
      if (input.getUserApiKeyAuthQueryParam() != null) {
        userConfig.setAuthQueryParam(input.getUserApiKeyAuthQueryParam());
      }

      config.setUserApiKeyConfig(userConfig);
    } else if (authType == AiPluginAuthType.USER_OAUTH && oauthServerUrn != null) {
      // OAuth config
      com.linkedin.settings.global.OAuthAiPluginConfig oauthConfig =
          new com.linkedin.settings.global.OAuthAiPluginConfig();
      oauthConfig.setServerUrn(oauthServerUrn);
      if (input.getRequiredScopes() != null && !input.getRequiredScopes().isEmpty()) {
        oauthConfig.setRequiredScopes(
            new com.linkedin.data.template.StringArray(input.getRequiredScopes()));
      }
      config.setOauthConfig(oauthConfig);
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
   * Creates an OAuthAuthorizationServer entity from inline input.
   *
   * @return URN of the created OAuth server
   */
  private Urn createOAuthServer(
      final QueryContext context, final UpsertOAuthAuthorizationServerInput input)
      throws Exception {

    // Generate URN
    final String serverId = input.getId() != null ? input.getId() : UUID.randomUUID().toString();
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
   * Creates a DataHubConnection entity owned by the service to store the shared API key.
   *
   * <p>The DataHubConnection uses a sanitized combination of service URN and "apiKey" as the
   * single-string ID, making it uniquely associated with this service.
   *
   * <p>Note: DataHubConnectionKey has a single `id` field (not a compound key), so we cannot use
   * tuple format like (owner,connectionId). Instead, we use a sanitized single-string format:
   * urn:li:dataHubConnection:&lt;sanitized_service_urn&gt;__apiKey
   *
   * @param context Query context
   * @param serviceUrn The service that owns this credential
   * @param apiKey The API key value to store
   * @return URN of the created DataHubConnection
   */
  private Urn createSharedCredential(
      final QueryContext context, final Urn serviceUrn, final String apiKey) throws Exception {

    // DataHubConnection URN format: urn:li:dataHubConnection:<single_id>
    // DataHubConnectionKey has only one field (id: string), not a compound key.
    // We use double-underscore as separator to create a unique, parseable ID.
    final String connectionUrn = buildServiceConnectionUrn(serviceUrn.toString(), "apiKey");
    final Urn credentialUrn = UrnUtils.getUrn(connectionUrn);

    // Build the connection details JSON with encrypted API key
    final com.linkedin.connection.DataHubConnectionDetails connectionDetails =
        new com.linkedin.connection.DataHubConnectionDetails();
    connectionDetails.setType(com.linkedin.connection.DataHubConnectionDetailsType.JSON);

    // Build JSON payload with the API key
    final String jsonPayload = String.format("{\"apiKey\":\"%s\"}", secretService.encrypt(apiKey));
    connectionDetails.setJson(
        new com.linkedin.connection.DataHubJsonConnection().setEncryptedBlob(jsonPayload));

    // Ingest the connection
    final MetadataChangeProposal connectionMcp =
        buildMetadataChangeProposalWithUrn(
            credentialUrn, Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME, connectionDetails);
    entityClient.ingestProposal(context.getOperationContext(), connectionMcp, false);

    return credentialUrn;
  }

  /**
   * Builds a DataHubConnection URN for service credentials. Format:
   * urn:li:dataHubConnection:&lt;sanitized_service_urn&gt;__&lt;sanitized_credential_type&gt;
   *
   * <p>Uses `__` as separator (not a tuple) because DataHubConnection expects a single-string key.
   * Using tuple format (a,b) causes DataHub to interpret it as a multi-part key which fails since
   * DataHubConnectionKey only has one field.
   *
   * <p>All special characters are sanitized to underscores for URN safety.
   *
   * <p>This must match the Python implementation in credential_store.py's build_connection_urn()
   * for consistency.
   */
  private static String buildServiceConnectionUrn(String serviceUrn, String credentialType) {
    // Sanitize both parts - replace colons, commas, and parentheses with underscores
    String sanitizedServiceUrn =
        serviceUrn.replace(":", "_").replace(",", "_").replace("(", "_").replace(")", "_");
    String sanitizedCredentialType =
        credentialType.replace(":", "_").replace(",", "_").replace("(", "_").replace(")", "_");
    return String.format(
        "urn:li:dataHubConnection:%s__%s", sanitizedServiceUrn, sanitizedCredentialType);
  }
}

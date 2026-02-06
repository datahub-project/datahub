package com.linkedin.datahub.graphql.resolvers.auth;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.getQueryContext;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.buildMetadataChangeProposalWithUrn;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.OAuthAuthorizationServer;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.generated.UpsertOAuthAuthorizationServerInput;
import com.linkedin.datahub.graphql.resolvers.connection.ConnectionUtils;
import com.linkedin.datahub.graphql.types.auth.mappers.OAuthAuthorizationServerMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.oauth.AuthLocation;
import com.linkedin.oauth.OAuthAuthorizationServerProperties;
import com.linkedin.oauth.TokenAuthMethod;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.services.SecretService;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Creates or updates an OAuth Authorization Server entity. */
@Slf4j
public class UpsertOAuthAuthorizationServerResolver
    implements DataFetcher<CompletableFuture<OAuthAuthorizationServer>> {

  private static final Set<String> ASPECTS_TO_FETCH =
      Set.of(
          Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
          Constants.OWNERSHIP_ASPECT_NAME,
          Constants.STATUS_ASPECT_NAME);

  private final EntityClient entityClient;
  private final SecretService secretService;

  public UpsertOAuthAuthorizationServerResolver(
      @Nonnull final EntityClient entityClient, @Nonnull final SecretService secretService) {
    this.entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    this.secretService = Objects.requireNonNull(secretService, "secretService must not be null");
  }

  @Override
  public CompletableFuture<OAuthAuthorizationServer> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = getQueryContext(environment);
    final UpsertOAuthAuthorizationServerInput input =
        bindArgument(environment.getArgument("input"), UpsertOAuthAuthorizationServerInput.class);

    // Reuse MANAGE_CONNECTIONS privilege since OAuth auth servers are external connections
    if (!ConnectionUtils.canManageConnections(context)) {
      throw new AuthorizationException(
          "Unauthorized to create/update OAuth authorization servers. "
              + "Please contact your DataHub administrator.");
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            // Generate URN
            final String serverId =
                input.getId() != null ? input.getId() : UUID.randomUUID().toString();
            final Urn serverUrn =
                UrnUtils.getUrn(String.format("urn:li:oauthAuthorizationServer:%s", serverId));

            // Fetch existing properties (if updating) to preserve secrets when input is null
            final OAuthAuthorizationServerProperties existingProperties =
                fetchExistingProperties(context, serverUrn);

            // Build OAuthAuthorizationServerProperties aspect
            final OAuthAuthorizationServerProperties properties =
                buildProperties(context, serverUrn, input, existingProperties);

            // Ingest properties
            final MetadataChangeProposal propertiesMcp =
                buildMetadataChangeProposalWithUrn(
                    serverUrn,
                    Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
                    properties);
            entityClient.ingestProposal(context.getOperationContext(), propertiesMcp, false);

            // Fetch and return the created/updated entity
            final Map<Urn, EntityResponse> entities =
                entityClient.batchGetV2(
                    context.getOperationContext(),
                    Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME,
                    new HashSet<>(List.of(serverUrn)),
                    ASPECTS_TO_FETCH);

            final EntityResponse response = entities.get(serverUrn);
            if (response == null) {
              throw new RuntimeException(
                  String.format(
                      "Failed to retrieve created OAuth authorization server: %s", serverUrn));
            }

            return OAuthAuthorizationServerMapper.map(context, response);

          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to upsert OAuth authorization server from input %s", input),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Fetches existing properties for a server, or null if the server doesn't exist.
   *
   * @param context The query context
   * @param serverUrn The server URN to fetch
   * @return The existing properties, or null if not found
   */
  private OAuthAuthorizationServerProperties fetchExistingProperties(
      final QueryContext context, final Urn serverUrn) {
    try {
      final Map<Urn, EntityResponse> entities =
          entityClient.batchGetV2(
              context.getOperationContext(),
              Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME,
              new HashSet<>(List.of(serverUrn)),
              Set.of(Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME));

      final EntityResponse response = entities.get(serverUrn);
      if (response != null
          && response.getAspects() != null
          && response
              .getAspects()
              .containsKey(Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME)) {
        return new OAuthAuthorizationServerProperties(
            response
                .getAspects()
                .get(Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME)
                .getValue()
                .data());
      }
    } catch (Exception e) {
      log.debug("Could not fetch existing properties for server {}: {}", serverUrn, e.getMessage());
    }
    return null;
  }

  /**
   * Builds properties for the OAuth authorization server.
   *
   * <p>Secret handling follows these rules:
   *
   * <ul>
   *   <li>null input → preserve existing secret (if any)
   *   <li>empty string → clear the secret
   *   <li>non-empty string → create new encrypted secret
   * </ul>
   *
   * @param context The query context
   * @param serverUrn The server URN
   * @param input The input from GraphQL
   * @param existingProperties Existing properties (may be null for new entities)
   * @return The built properties
   */
  private OAuthAuthorizationServerProperties buildProperties(
      final QueryContext context,
      final Urn serverUrn,
      final UpsertOAuthAuthorizationServerInput input,
      final OAuthAuthorizationServerProperties existingProperties)
      throws Exception {
    final OAuthAuthorizationServerProperties properties = new OAuthAuthorizationServerProperties();

    // Basic properties
    properties.setDisplayName(input.getDisplayName());
    if (input.getDescription() != null) {
      properties.setDescription(input.getDescription());
    }

    // Handle OAuth client secret - encrypt and store as DataHubSecret
    // Rules: null = preserve existing, empty = clear, non-empty = create new
    if (input.getClientSecret() == null) {
      // Preserve existing secret if available
      if (existingProperties != null && existingProperties.hasClientSecretUrn()) {
        Urn existingSecretUrn = existingProperties.getClientSecretUrn();
        if (existingSecretUrn != null) {
          properties.setClientSecretUrn(existingSecretUrn);
        }
      }
    } else if (!input.getClientSecret().isEmpty()) {
      // Create new secret
      final Urn secretUrn =
          createSecret(context, serverUrn, "clientSecret", input.getClientSecret());
      properties.setClientSecretUrn(secretUrn);
    }
    // Empty string = clear secret (don't set URN)

    // OAuth config - preserve existing values if input is null
    if (input.getClientId() != null) {
      properties.setClientId(input.getClientId());
    } else if (existingProperties != null && existingProperties.hasClientId()) {
      properties.setClientId(existingProperties.getClientId());
    }
    if (input.getAuthorizationUrl() != null) {
      properties.setAuthorizationUrl(input.getAuthorizationUrl());
    } else if (existingProperties != null && existingProperties.hasAuthorizationUrl()) {
      properties.setAuthorizationUrl(existingProperties.getAuthorizationUrl());
    }
    if (input.getTokenUrl() != null) {
      properties.setTokenUrl(input.getTokenUrl());
    } else if (existingProperties != null && existingProperties.hasTokenUrl()) {
      properties.setTokenUrl(existingProperties.getTokenUrl());
    }
    if (input.getScopes() != null) {
      properties.setScopes(new StringArray(input.getScopes()));
    } else if (existingProperties != null && existingProperties.hasScopes()) {
      properties.setScopes(existingProperties.getScopes());
    }

    // Token auth method
    if (input.getTokenAuthMethod() != null) {
      properties.setTokenAuthMethod(TokenAuthMethod.valueOf(input.getTokenAuthMethod().toString()));
    } else {
      properties.setTokenAuthMethod(TokenAuthMethod.POST_BODY);
    }

    // Additional parameters
    if (input.getAdditionalTokenParams() != null) {
      properties.setAdditionalTokenParams(buildStringMap(input.getAdditionalTokenParams()));
    }
    if (input.getAdditionalAuthParams() != null) {
      properties.setAdditionalAuthParams(buildStringMap(input.getAdditionalAuthParams()));
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

    return properties;
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

  private StringMap buildStringMap(final List<StringMapEntryInput> entries) {
    final StringMap map = new StringMap();
    if (entries != null) {
      for (StringMapEntryInput entry : entries) {
        map.put(entry.getKey(), entry.getValue());
      }
    }
    return map;
  }
}

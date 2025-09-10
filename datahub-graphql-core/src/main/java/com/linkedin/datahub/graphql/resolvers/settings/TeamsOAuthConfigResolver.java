package com.linkedin.datahub.graphql.resolvers.settings;

import static com.datahub.authorization.AuthUtil.isAuthorized;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.TeamsOAuthConfig;
import com.linkedin.metadata.integration.IntegrationsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for Teams OAuth configuration.
 *
 * <p>Provides OAuth configuration parameters needed by the frontend to initiate Microsoft OAuth
 * flows for Teams integration setup.
 */
@Slf4j
@RequiredArgsConstructor
public class TeamsOAuthConfigResolver implements DataFetcher<CompletableFuture<TeamsOAuthConfig>> {

  private final IntegrationsService integrationsService;

  @Override
  public CompletableFuture<TeamsOAuthConfig> get(@Nonnull DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    // Check authorization - users need to be able to manage notifications to access OAuth config
    if (!isAuthorized(context)) {
      throw new AuthorizationException(
          "Unauthorized to access Teams OAuth configuration. Requires 'Manage Global Settings' privilege.");
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return getTeamsOAuthConfig();
          } catch (Exception e) {
            log.error("Failed to get Teams OAuth configuration", e);
            throw new RuntimeException("Failed to get Teams OAuth configuration", e);
          }
        },
        this.getClass().getSimpleName(),
        "getTeamsOAuthConfig");
  }

  private boolean isAuthorized(@Nonnull QueryContext context) {
    // TODO: Implement proper authorization check
    // For now, allow all authenticated users
    return context.getActorUrn() != null;
  }

  private TeamsOAuthConfig getTeamsOAuthConfig() {
    try {
      // Call the integrations service to get OAuth configuration
      CompletableFuture<Map<String, Object>> configFuture =
          integrationsService.getTeamsOAuthConfig();
      Map<String, Object> config = configFuture.join();

      log.info("Retrieved Teams OAuth configuration from integrations service: {}", config);

      return TeamsOAuthConfig.builder()
          .setAppId((String) config.get("app_id"))
          .setRedirectUri((String) config.get("redirect_uri"))
          .setScopes((String) config.get("scopes"))
          .setBaseAuthUrl((String) config.get("base_auth_url"))
          .build();
    } catch (Exception e) {
      log.error("Failed to get Teams OAuth configuration from integrations service", e);

      // Fallback to environment variables if service call fails
      String appId = System.getenv("DATAHUB_TEAMS_APP_ID");
      String redirectUri = System.getenv("DATAHUB_TEAMS_OAUTH_REDIRECT_URI");
      String scopes = System.getenv("DATAHUB_TEAMS_OAUTH_SCOPES");
      String baseAuthUrl = System.getenv("DATAHUB_TEAMS_OAUTH_BASE_AUTH_URL");

      // Provide defaults if environment variables are not set
      if (appId == null || appId.isEmpty()) {
        appId = "8f862cd7-07e2-49c1-8871-5db61a205433"; // Default Teams app ID
      }

      if (redirectUri == null || redirectUri.isEmpty()) {
        redirectUri =
            "https://router.datahub.com/public/teams/oauth/callback"; // Default redirect URI
      }

      if (scopes == null || scopes.isEmpty()) {
        scopes = "https://graph.microsoft.com/.default offline_access"; // Default OAuth scopes
      }

      if (baseAuthUrl == null || baseAuthUrl.isEmpty()) {
        baseAuthUrl = "https://login.microsoftonline.com"; // Microsoft OAuth base URL
      }

      log.warn(
          "Using fallback Teams OAuth configuration: appId={}, redirectUri={}, scopes={}, baseAuthUrl={}",
          appId,
          redirectUri,
          scopes,
          baseAuthUrl);

      return TeamsOAuthConfig.builder()
          .setAppId(appId)
          .setRedirectUri(redirectUri)
          .setScopes(scopes)
          .setBaseAuthUrl(baseAuthUrl)
          .build();
    }
  }
}

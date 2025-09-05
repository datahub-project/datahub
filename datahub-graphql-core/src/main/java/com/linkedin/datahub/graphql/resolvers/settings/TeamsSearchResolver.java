package com.linkedin.datahub.graphql.resolvers.settings;

import static com.datahub.authorization.AuthUtil.isAuthorized;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.TeamsChannelResult;
import com.linkedin.datahub.graphql.generated.TeamsSearchInput;
import com.linkedin.datahub.graphql.generated.TeamsSearchResult;
import com.linkedin.datahub.graphql.generated.TeamsSearchType;
import com.linkedin.datahub.graphql.generated.TeamsUserSearchResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.key.DataHubConnectionKey;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.integrations.model.SearchResponse;
import io.datahubproject.integrations.model.SearchResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class TeamsSearchResolver implements DataFetcher<CompletableFuture<TeamsSearchResult>> {

  private static final String TEAMS_CONNECTION_ID = "__system_teams-0";
  private static final String TEAMS_CONNECTION_URN =
      "urn:li:dataHubConnection:" + TEAMS_CONNECTION_ID;

  private final EntityClient entityClient;
  private final SettingsService settingsService;
  private final IntegrationsService integrationsService;

  @Override
  public CompletableFuture<TeamsSearchResult> get(@Nonnull DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    // Check authorization - users need to be able to manage notifications to search Teams
    if (!isAuthorized(context)) {
      throw new AuthorizationException(
          "Unauthorized to search Teams. Requires 'Manage Global Settings' privilege.");
    }

    final TeamsSearchInput input =
        bindArgument(environment.getArgument("input"), TeamsSearchInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return performTeamsSearch(input, context);
          } catch (Exception e) {
            log.error("Failed to search Teams", e);
            throw new RuntimeException("Failed to search Teams", e);
          }
        },
        this.getClass().getSimpleName(),
        "searchTeams");
  }

  private boolean isAuthorized(@Nonnull QueryContext context) {
    // TODO: Implement proper authorization check
    // For now, allow all authenticated users
    return context.getActorUrn() != null;
  }

  private TeamsSearchResult performTeamsSearch(
      @Nonnull TeamsSearchInput input, @Nonnull QueryContext context) {
    final String query = input.getQuery();
    final TeamsSearchType searchType =
        input.getType() != null ? input.getType() : TeamsSearchType.ALL;
    final int limit = input.getLimit() != null ? input.getLimit() : 10;

    List<TeamsUserSearchResult> users = new ArrayList<>();
    List<TeamsChannelResult> channels = new ArrayList<>();
    boolean hasMore = false;

    try {
      // Check if Teams integration is configured
      if (!isTeamsIntegrationConfigured(context)) {
        log.warn("Teams integration is not configured, returning empty results");
        return createEmptyResult();
      }

      // Search users if requested
      if (searchType == TeamsSearchType.ALL || searchType == TeamsSearchType.USERS) {
        users = searchTeamsUsers(query, limit);
      }

      // Search channels if requested
      if (searchType == TeamsSearchType.ALL || searchType == TeamsSearchType.CHANNELS) {
        channels = searchTeamsChannels(query, limit);
      }

      // Simple hasMore logic - if we got the full limit, there might be more
      hasMore = users.size() >= limit || channels.size() >= limit;

    } catch (Exception e) {
      log.error("Error performing Teams search for query: {}", query, e);
      // Return partial results if available, or empty results
    }

    return TeamsSearchResult.builder()
        .setUsers(users)
        .setChannels(channels)
        .setHasMore(hasMore)
        .build();
  }

  private boolean isTeamsIntegrationConfigured(@Nonnull QueryContext context) {
    try {
      // Check if Teams connection exists in DataHub
      final DataHubConnectionKey connectionKey =
          new DataHubConnectionKey().setId(TEAMS_CONNECTION_ID);
      final Urn connectionUrn =
          EntityKeyUtils.convertEntityKeyToUrn(
              connectionKey, Constants.DATAHUB_CONNECTION_ENTITY_NAME);

      EntityResponse connectionResponse =
          entityClient.getV2(
              context.getOperationContext(),
              Constants.DATAHUB_CONNECTION_ENTITY_NAME,
              connectionUrn,
              null);

      if (connectionResponse != null
          && connectionResponse.getAspects() != null
          && connectionResponse
              .getAspects()
              .containsKey(Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME)) {
        log.debug("Teams connection found: {}", TEAMS_CONNECTION_URN);
        return true;
      }

      log.debug("Teams connection not found: {}", TEAMS_CONNECTION_URN);

      // Fallback: check if integrations service is available
      if (integrationsService != null) {
        log.debug(
            "Teams connection not found but integrations service is available, allowing search");
        return true;
      }

      log.debug("Teams integration not configured and integrations service not available");
      return false;
    } catch (Exception e) {
      log.error("Error checking Teams integration configuration", e);

      // Fallback to integrations service if connection check fails
      if (integrationsService != null) {
        log.debug(
            "Teams connection check failed but integrations service is available, allowing search");
        return true;
      }
    }
    return false;
  }

  private List<TeamsUserSearchResult> searchTeamsUsers(@Nonnull String query, int limit) {
    try {
      log.info("Searching Teams users for query: {} with limit: {}", query, limit);
      SearchResponse response = integrationsService.searchTeamsUsers(query, limit).join();
      if (response != null && response.getResults() != null) {
        return response.getResults().stream()
            .map(this::mapToTeamsUser)
            .collect(java.util.stream.Collectors.toList());
      }
      return new ArrayList<>();
    } catch (Exception e) {
      log.error("Error searching Teams users for query: {}", query, e);
      return new ArrayList<>();
    }
  }

  private List<TeamsChannelResult> searchTeamsChannels(@Nonnull String query, int limit) {
    try {
      log.info("Searching Teams channels for query: {} with limit: {}", query, limit);
      SearchResponse response = integrationsService.searchTeamsChannels(query, limit).join();
      if (response != null && response.getResults() != null) {
        return response.getResults().stream()
            .map(this::mapToTeamsChannel)
            .collect(java.util.stream.Collectors.toList());
      }
      return new ArrayList<>();
    } catch (Exception e) {
      log.error("Error searching Teams channels for query: {}", query, e);
      return new ArrayList<>();
    }
  }

  private TeamsSearchResult createEmptyResult() {
    return TeamsSearchResult.builder()
        .setUsers(new ArrayList<>())
        .setChannels(new ArrayList<>())
        .setHasMore(false)
        .build();
  }

  private TeamsUserSearchResult mapToTeamsUser(@Nonnull SearchResult result) {
    return TeamsUserSearchResult.builder()
        .setId(result.getId())
        .setDisplayName(result.getDisplayName())
        .setEmail(result.getEmail())
        .setDepartment(null) // Department not available in SearchResult
        .build();
  }

  private TeamsChannelResult mapToTeamsChannel(@Nonnull SearchResult result) {
    return TeamsChannelResult.builder()
        .setId(result.getId())
        .setDisplayName(result.getDisplayName())
        .setTeamName(result.getTeamName())
        .setMemberCount(null) // Member count not available in SearchResult
        .build();
  }
}

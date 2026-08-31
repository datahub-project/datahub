package com.linkedin.datahub.graphql.resolvers.timeline;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.VersionProperties;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ChangeCategoryType;
import com.linkedin.datahub.graphql.generated.GetTimelineInput;
import com.linkedin.datahub.graphql.generated.GetTimelineResult;
import com.linkedin.datahub.graphql.types.timeline.mappers.ChangeTransactionMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.authorization.EntityAuthorizationUtils;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.timeline.TimelineFetchResult;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.utils.CriterionUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/*
Returns the timeline in its original form, with optional version-set expansion.
 */
@Slf4j
public class GetTimelineResolver implements DataFetcher<CompletableFuture<GetTimelineResult>> {

  private static final int MAX_VERSION_WALK = 50;
  private static final String VERSION_SET_SEARCH_FIELD = "versionSet";

  private final TimelineService _timelineService;
  private final EntityClient _entityClient;

  public GetTimelineResolver(TimelineService timelineService, EntityClient entityClient) {
    _timelineService = timelineService;
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<GetTimelineResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final GetTimelineInput input =
        bindArgument(environment.getArgument("input"), GetTimelineInput.class);

    final String entityUrnString = input.getUrn();
    final List<ChangeCategoryType> changeCategories = input.getChangeCategories();
    final boolean includeVersionSet = Boolean.TRUE.equals(input.getIncludeVersionSet());

    final Urn entityUrn = UrnUtils.getUrn(entityUrnString);
    // Explicit GraphQL operation: always evaluate shared entity VIEW privileges, independent of
    // View Authorization.
    if (!EntityAuthorizationUtils.canViewEntity(context.getOperationContext(), entityUrn)) {
      throw new AuthorizationException(
          "Unauthorized to view change history for entity: " + entityUrn);
    }

    final List<Urn> authorizedVersionUrns;
    final int unauthorizedVersionCount;
    final int truncatedVersionCount;
    if (includeVersionSet) {
      // Resolve and authorize siblings on the request thread so policy checks are not deferred to
      // the async timeline fetch (and so denied siblings never reach TimelineService).
      VersionSetResolution resolution = resolveVersionSetUrns(entityUrn, context);
      truncatedVersionCount = resolution.getTruncatedCount();
      List<Urn> authorized = new ArrayList<>();
      int unauthorized = 0;
      for (Urn versionUrn : resolution.getUrns()) {
        if (EntityAuthorizationUtils.canViewEntity(context.getOperationContext(), versionUrn)) {
          authorized.add(versionUrn);
        } else {
          unauthorized++;
        }
      }
      authorizedVersionUrns = authorized;
      unauthorizedVersionCount = unauthorized;
    } else {
      authorizedVersionUrns = List.of();
      unauthorizedVersionCount = 0;
      truncatedVersionCount = 0;
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Set<ChangeCategory> changeCategorySet =
                changeCategories != null
                    ? changeCategories.stream()
                        .map(c -> ChangeCategory.valueOf(c.toString()))
                        .collect(Collectors.toSet())
                    : Arrays.stream(ChangeCategory.values()).collect(Collectors.toSet());

            final List<ChangeTransaction> changeTransactionList;
            int skippedVersionCount = 0;

            if (includeVersionSet) {
              TimelineFetchResult fetchResult =
                  _timelineService.getTimelineForUrns(
                      context.getOperationContext(),
                      authorizedVersionUrns,
                      changeCategorySet,
                      false);
              changeTransactionList = fetchResult.getTransactions();
              skippedVersionCount =
                  fetchResult.getSkippedUrnCount()
                      + truncatedVersionCount
                      + unauthorizedVersionCount;
            } else {
              changeTransactionList =
                  _timelineService.getTimeline(
                      context.getOperationContext(),
                      entityUrn,
                      changeCategorySet,
                      TimelineService.DEFAULT_MAX_CHANGE_TRANSACTIONS,
                      false);
            }

            GetTimelineResult result = new GetTimelineResult();
            result.setChangeTransactions(
                changeTransactionList.stream()
                    .map(ChangeTransactionMapper::map)
                    .filter(t -> t.getChanges() != null && !t.getChanges().isEmpty())
                    .collect(Collectors.toList()));
            result.setSkippedVersionCount(skippedVersionCount);
            return result;
          } catch (Exception e) {
            log.error(
                String.format("Failed to get timeline data for entity %s", entityUrnString), e);
            return null;
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Resolves all version URNs in the same VersionSet as {@code urn}.
   *
   * <p>Steps:
   *
   * <ol>
   *   <li>Fetch {@code versionProperties} of {@code urn} to get the {@code versionSet} URN.
   *   <li>Search the entity index for all entities whose {@code versionSet} field equals that URN,
   *       capped at {@link #MAX_VERSION_WALK}.
   *   <li>Return the combined list (current URN + all siblings), deduplicated, alongside the count
   *       of versions that exist beyond the walk limit so the caller can warn the user that the
   *       merged view is partial.
   * </ol>
   *
   * Falls back to a singleton list containing only {@code urn} if any step fails.
   */
  private VersionSetResolution resolveVersionSetUrns(
      @Nonnull Urn urn, @Nonnull QueryContext context) {
    try {
      EntityResponse entityResponse =
          _entityClient.getV2(
              context.getOperationContext(),
              urn.getEntityType(),
              urn,
              Collections.singleton(VERSION_PROPERTIES_ASPECT_NAME));

      if (entityResponse == null
          || !entityResponse.getAspects().containsKey(VERSION_PROPERTIES_ASPECT_NAME)) {
        return VersionSetResolution.singleton(urn);
      }

      VersionProperties vp =
          new VersionProperties(
              entityResponse.getAspects().get(VERSION_PROPERTIES_ASPECT_NAME).getValue().data());
      Urn versionSetUrn = vp.getVersionSet();

      // Include all versions, not just the latest, when walking the version set.
      OperationContext versionSearchContext =
          context
              .getOperationContext()
              .withSearchFlags(flags -> flags.setFilterNonLatestVersions(false));

      SearchResult searchResult =
          _entityClient.search(
              versionSearchContext,
              urn.getEntityType(),
              "*",
              QueryUtils.newFilter(
                  CriterionUtils.buildCriterion(
                      VERSION_SET_SEARCH_FIELD, Condition.EQUAL, versionSetUrn.toString())),
              null,
              0,
              MAX_VERSION_WALK);

      if (searchResult == null
          || searchResult.getEntities() == null
          || searchResult.getEntities().isEmpty()) {
        return VersionSetResolution.singleton(urn);
      }

      List<Urn> urns =
          searchResult.getEntities().stream()
              .map(SearchEntity::getEntity)
              .collect(Collectors.toCollection(ArrayList::new));

      if (!urns.contains(urn)) {
        urns.add(urn);
      }

      int total = (int) searchResult.getNumEntities();
      int truncated = Math.max(0, total - urns.size());
      return new VersionSetResolution(urns, truncated);

    } catch (Exception e) {
      log.warn(
          "Failed to resolve version set URNs for {}, falling back to single-entity timeline: {}",
          urn,
          e.getMessage());
      return VersionSetResolution.singleton(urn);
    }
  }

  /** Result of expanding a single URN into its full VersionSet membership. */
  private static class VersionSetResolution {
    private final List<Urn> urns;
    private final int truncatedCount;

    VersionSetResolution(List<Urn> urns, int truncatedCount) {
      this.urns = urns;
      this.truncatedCount = truncatedCount;
    }

    static VersionSetResolution singleton(Urn urn) {
      return new VersionSetResolution(Collections.singletonList(urn), 0);
    }

    List<Urn> getUrns() {
      return urns;
    }

    int getTruncatedCount() {
      return truncatedCount;
    }
  }
}

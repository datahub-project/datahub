package com.linkedin.datahub.graphql.resolvers.timeline;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.ChangeCategoryType;
import com.linkedin.datahub.graphql.generated.GetTimelineInput;
import com.linkedin.datahub.graphql.generated.GetTimelineResult;
import com.linkedin.datahub.graphql.types.timeline.mappers.ChangeTransactionMapper;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/*
Returns the timeline in its original form
 */
@Slf4j
public class GetTimelineResolver implements DataFetcher<CompletableFuture<GetTimelineResult>> {
  private final TimelineService _timelineService;

  // GraphQL ChangeCategoryType -> backend ChangeCategory mapping.
  // Needed because GraphQL uses OWNERSHIP while backend uses OWNER.
  static final Map<ChangeCategoryType, ChangeCategory> CATEGORY_INPUT_MAP =
      Map.of(
          ChangeCategoryType.DOCUMENTATION, ChangeCategory.DOCUMENTATION,
          ChangeCategoryType.GLOSSARY_TERM, ChangeCategory.GLOSSARY_TERM,
          ChangeCategoryType.OWNERSHIP, ChangeCategory.OWNER,
          ChangeCategoryType.TECHNICAL_SCHEMA, ChangeCategory.TECHNICAL_SCHEMA,
          ChangeCategoryType.TAG, ChangeCategory.TAG,
          ChangeCategoryType.PARENT, ChangeCategory.PARENT,
          ChangeCategoryType.RELATED_ENTITIES, ChangeCategory.RELATED_ENTITIES,
          ChangeCategoryType.DOMAIN, ChangeCategory.DOMAIN,
          ChangeCategoryType.STRUCTURED_PROPERTY, ChangeCategory.STRUCTURED_PROPERTY,
          ChangeCategoryType.APPLICATION, ChangeCategory.APPLICATION);

  @Nonnull
  private static ChangeCategory mapToBackendCategory(
      @Nonnull ChangeCategoryType changeCategoryType) {
    ChangeCategory mapped = CATEGORY_INPUT_MAP.get(changeCategoryType);
    if (mapped == null) {
      log.warn(
          "No backend ChangeCategory mapping for GraphQL type: {}. Falling back to valueOf.",
          changeCategoryType);
      return ChangeCategory.valueOf(changeCategoryType.toString());
    }
    return mapped;
  }

  public GetTimelineResolver(TimelineService timelineService) {
    _timelineService = timelineService;
  }

  @Override
  public CompletableFuture<GetTimelineResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final GetTimelineInput input =
        bindArgument(environment.getArgument("input"), GetTimelineInput.class);

    final String datasetUrnString = input.getUrn();
    final List<ChangeCategoryType> changeCategories = input.getChangeCategories();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Set<ChangeCategory> changeCategorySet =
                changeCategories != null
                    ? changeCategories.stream()
                        .map(GetTimelineResolver::mapToBackendCategory)
                        .collect(Collectors.toSet())
                    : Arrays.stream(ChangeCategory.values()).collect(Collectors.toSet());
            final Urn datasetUrn = Urn.createFromString(datasetUrnString);
            final List<ChangeTransaction> changeTransactionList =
                _timelineService.getTimeline(
                    datasetUrn,
                    changeCategorySet,
                    TimelineService.DEFAULT_MAX_CHANGE_TRANSACTIONS,
                    false);
            GetTimelineResult result = new GetTimelineResult();
            result.setChangeTransactions(
                changeTransactionList.stream()
                    .map(ChangeTransactionMapper::map)
                    .filter(t -> t.getChanges() != null && !t.getChanges().isEmpty())
                    .collect(Collectors.toList()));
            return result;
          } catch (URISyntaxException u) {
            log.error(
                String.format(
                    "Failed to list schema blame data, likely due to the Urn %s being invalid",
                    datasetUrnString),
                u);
            return null;
          } catch (Exception e) {
            log.error("Failed to list schema blame data", e);
            return null;
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}

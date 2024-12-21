package com.linkedin.datahub.graphql.resolvers.timeseries;

import com.linkedin.common.Operation;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.AssetStatsResult;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.TimeseriesCapabilitiesResult;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.dataset.DatasetUsageStatistics;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TimeseriesCapabilitiesResolver
    implements DataFetcher<CompletableFuture<TimeseriesCapabilitiesResult>> {

  final SortCriterion OLDEST_FIRST_SORT =
      new SortCriterion().setField(Constants.ES_FIELD_TIMESTAMP).setOrder(SortOrder.ASCENDING);

  private final EntityClient entityClient;

  public TimeseriesCapabilitiesResolver(final EntityClient client) {
    this.entityClient = client;
  }

  @Override
  public CompletableFuture<TimeseriesCapabilitiesResult> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Urn resourceUrn = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          AssetStatsResult assetStatsResult = new AssetStatsResult();
          try {
            if (resourceUrn.getEntityType().equals(Constants.DATASET_ENTITY_NAME)) {
              setOldestOperationTime(context, resourceUrn, assetStatsResult);
              setOldestDatasetUsageTime(context, resourceUrn, assetStatsResult);
              setOldestDatasetProfileTime(context, resourceUrn, assetStatsResult);
            }
          } catch (Exception e) {
            log.error(
                String.format(
                    "Failed to load timeseries capabilities for resource %s", resourceUrn),
                e);
            MetricUtils.counter(this.getClass(), "timeseries_capabilities_dropped").inc();
          }
          TimeseriesCapabilitiesResult result = new TimeseriesCapabilitiesResult();
          result.setAssetStats(assetStatsResult);
          return result;
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void setOldestOperationTime(
      @Nonnull QueryContext context, @Nonnull Urn urn, @Nonnull AssetStatsResult assetStatsResult)
      throws Exception {
    if (!AuthorizationUtils.isViewDatasetOperationsAuthorized(context, urn)) {
      log.debug(
          "User {} is not authorized to view operation information for dataset {}",
          context.getActorUrn(),
          urn);
      return;
    }
    List<EnvelopedAspect> aspects =
        getOldestAspect(
            context.getOperationContext(),
            urn.toString(),
            Constants.DATASET_ENTITY_NAME,
            Constants.OPERATION_ASPECT_NAME);

    if (aspects.size() > 0) {
      Operation operation =
          GenericRecordUtils.deserializeAspect(
              aspects.get(0).getAspect().getValue(),
              aspects.get(0).getAspect().getContentType(),
              Operation.class);
      assetStatsResult.setOldestOperationTime(operation.getTimestampMillis());
    }
  }

  private void setOldestDatasetUsageTime(
      @Nonnull QueryContext context, @Nonnull Urn urn, @Nonnull AssetStatsResult assetStatsResult)
      throws Exception {
    if (!AuthorizationUtils.isViewDatasetUsageAuthorized(context, urn)) {
      log.debug(
          "User {} is not authorized to view usage information for dataset {}",
          context.getActorUrn(),
          urn);
      return;
    }

    List<EnvelopedAspect> aspects =
        getOldestAspect(
            context.getOperationContext(),
            urn.toString(),
            Constants.DATASET_ENTITY_NAME,
            Constants.DATASET_USAGE_STATISTICS_ASPECT_NAME);

    if (aspects.size() > 0) {
      DatasetUsageStatistics usage =
          GenericRecordUtils.deserializeAspect(
              aspects.get(0).getAspect().getValue(),
              aspects.get(0).getAspect().getContentType(),
              DatasetUsageStatistics.class);
      assetStatsResult.setOldestDatasetUsageTime(usage.getTimestampMillis());
    }
  }

  private void setOldestDatasetProfileTime(
      @Nonnull QueryContext context, @Nonnull Urn urn, @Nonnull AssetStatsResult assetStatsResult)
      throws Exception {
    if (!AuthorizationUtils.isViewDatasetProfileAuthorized(context, urn)) {
      log.debug(
          "User {} is not authorized to view profile information for dataset {}",
          context.getActorUrn(),
          urn);
      return;
    }

    List<EnvelopedAspect> aspects =
        getOldestAspect(
            context.getOperationContext(),
            urn.toString(),
            Constants.DATASET_ENTITY_NAME,
            Constants.DATASET_PROFILE_ASPECT_NAME);

    if (aspects.size() > 0) {
      DatasetProfile profile =
          GenericRecordUtils.deserializeAspect(
              aspects.get(0).getAspect().getValue(),
              aspects.get(0).getAspect().getContentType(),
              DatasetProfile.class);
      assetStatsResult.setOldestDatasetProfileTime(profile.getTimestampMillis());
    }
  }

  private List<EnvelopedAspect> getOldestAspect(
      @Nonnull OperationContext context,
      @Nonnull String urn,
      @Nonnull String entityName,
      @Nonnull String aspectName)
      throws Exception {
    return entityClient.getTimeseriesAspectValues(
        context, urn, entityName, aspectName, null, null, 1, null, OLDEST_FIRST_SORT);
  }
}

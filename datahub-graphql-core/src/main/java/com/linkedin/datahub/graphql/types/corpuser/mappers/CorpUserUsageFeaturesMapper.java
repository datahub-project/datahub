package com.linkedin.datahub.graphql.types.corpuser.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CorpUserUsageFeatures;
import com.linkedin.datahub.graphql.generated.FloatMapEntry;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class CorpUserUsageFeaturesMapper
    implements ModelMapper<
        com.linkedin.metadata.search.features.CorpUserUsageFeatures, CorpUserUsageFeatures> {

  public static final CorpUserUsageFeaturesMapper INSTANCE = new CorpUserUsageFeaturesMapper();

  public static CorpUserUsageFeatures map(
      @Nullable QueryContext context,
      @Nonnull
          final com.linkedin.metadata.search.features.CorpUserUsageFeatures corpUserUsageFeatures) {
    return INSTANCE.apply(context, corpUserUsageFeatures);
  }

  @Override
  public CorpUserUsageFeatures apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.metadata.search.features.CorpUserUsageFeatures gmsUsageFeatures) {
    final CorpUserUsageFeatures result = new CorpUserUsageFeatures();

    if (gmsUsageFeatures.hasUserUsageTotalPast30Days()) {
      result.setUserUsageTotalPast30Days(gmsUsageFeatures.getUserUsageTotalPast30Days());
    }

    if (gmsUsageFeatures.hasUserPlatformUsageTotalsPast30Days()) {
      List<FloatMapEntry> platformTotals =
          gmsUsageFeatures.getUserPlatformUsageTotalsPast30Days().entrySet().stream()
              .map(
                  entry -> {
                    FloatMapEntry mapEntry = new FloatMapEntry();
                    mapEntry.setKey(entry.getKey());
                    mapEntry.setValue(entry.getValue().floatValue());
                    return mapEntry;
                  })
              .collect(Collectors.toList());
      result.setUserPlatformUsageTotalsPast30Days(platformTotals);
    }

    if (gmsUsageFeatures.hasUserPlatformUsagePercentilePast30Days()) {
      List<FloatMapEntry> platformPercentiles =
          gmsUsageFeatures.getUserPlatformUsagePercentilePast30Days().entrySet().stream()
              .map(
                  entry -> {
                    FloatMapEntry mapEntry = new FloatMapEntry();
                    mapEntry.setKey(entry.getKey());
                    mapEntry.setValue(entry.getValue().floatValue());
                    return mapEntry;
                  })
              .collect(Collectors.toList());
      result.setUserPlatformUsagePercentilePast30Days(platformPercentiles);
    }

    if (gmsUsageFeatures.hasUserUsagePercentilePast30Days()) {
      result.setUserUsagePercentilePast30Days(
          gmsUsageFeatures.getUserUsagePercentilePast30Days().floatValue());
    }

    if (gmsUsageFeatures.hasUserTopDatasetsByUsage()) {
      List<FloatMapEntry> topDatasets =
          gmsUsageFeatures.getUserTopDatasetsByUsage().entrySet().stream()
              .map(
                  entry -> {
                    FloatMapEntry mapEntry = new FloatMapEntry();
                    mapEntry.setKey(entry.getKey());
                    mapEntry.setValue(entry.getValue().floatValue());
                    return mapEntry;
                  })
              .collect(Collectors.toList());
      result.setUserTopDatasetsByUsage(topDatasets);
    }

    return result;
  }
}

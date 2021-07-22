package com.linkedin.datahub.graphql.resolvers.load;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.generated.Aspect;
import com.linkedin.datahub.graphql.generated.DataProfile;
import com.linkedin.datahub.graphql.generated.DatasetFieldProfile;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.TimeRange;
import com.linkedin.datahub.graphql.generated.TimeSeriesAspect;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;


/**
 * Generic GraphQL resolver responsible for
 *
 *    1. Generating a single input AspectLoadKey.
 *    2. Resolving a single {@link Aspect}.
 *
 */
public class TimeSeriesAspectRangeResolver
    implements DataFetcher<CompletableFuture<List<TimeSeriesAspect>>> {

  private final String _aspectName;

  public TimeSeriesAspectRangeResolver(final String aspectName) {
    _aspectName = aspectName;
  }

  @Override
  public CompletableFuture<List<TimeSeriesAspect>> get(DataFetchingEnvironment environment) {

    final String urn = ((Entity) environment.getSource()).getUrn();

    // TODO: Also support a start and end time. Or a time and count.
    // Currently, we only support this less granular look-back window.
    // For operability we'll likely want to permit a range.

    TimeRange range = null;
    final String maybeTimeRange = environment.getArgumentOrDefault
        ("range", null);
    if (maybeTimeRange != null) {
      range = TimeRange.valueOf(maybeTimeRange);
    }

    // Max number of aspects to return.
    final Integer limit = environment.getArgumentOrDefault("count", null);

    if (range != null) {
      // Query for specific time range, not simply most recent.
      // Then use another set of aspects.
      final List<TimeSeriesAspect> dataProfileList = new ArrayList<>();
      final DataProfile profile1 = new DataProfile();
      profile1.setColumnCount(12);
      profile1.setTimestampMillis(1626764400000L);
      profile1.setRowCount(13);
      dataProfileList.add(profile1);

      final DataProfile profile2 = new DataProfile();
      profile2.setColumnCount(45);
      profile2.setTimestampMillis(1626678000000L);
      profile2.setRowCount(34);
      dataProfileList.add(profile2);
      return CompletableFuture.completedFuture(dataProfileList);
    } else {
      // No time bounds. Default to getting the latest.
      final List<TimeSeriesAspect> dataProfileList = new ArrayList<>();
      final DataProfile testProfile = new DataProfile();
      testProfile.setColumnCount(124);
      testProfile.setTimestampMillis(1626906097326L);
      testProfile.setRowCount(123);

      DatasetFieldProfile fieldProfile = new DatasetFieldProfile();
      fieldProfile.setFieldPath("myColumnName");
      fieldProfile.setMax(10.0);
      fieldProfile.setMin(11.0);
      fieldProfile.setNullCount(45L);
      fieldProfile.setNullProportion(0.45);
      fieldProfile.setUniqueCount(100L);
      fieldProfile.setUniqueProportion(0.67);
      fieldProfile.setStdev(0.1);
      fieldProfile.setSampleValues(ImmutableList.of("value1", "value2", "value3", "value4"));

      DatasetFieldProfile fieldProfile2 = new DatasetFieldProfile();
      fieldProfile2.setFieldPath("myColumnName2");
      fieldProfile2.setMax(14.0);
      fieldProfile2.setMin(12.0);
      fieldProfile2.setNullCount(12L);
      fieldProfile2.setNullProportion(0.3);
      fieldProfile2.setMean(12.0);
      fieldProfile2.setMedian(4.0);
      fieldProfile2.setUniqueCount(20L);
      fieldProfile2.setUniqueProportion(0.67);
      fieldProfile2.setStdev(0.1);

      testProfile.setFieldProfiles(ImmutableList.of(fieldProfile, fieldProfile2));

      dataProfileList.add(testProfile);
      return CompletableFuture.completedFuture(dataProfileList);
    }
  }
}

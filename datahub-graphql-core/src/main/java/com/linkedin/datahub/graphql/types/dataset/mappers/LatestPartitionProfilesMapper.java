package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.PartitionProfile;
import com.linkedin.dataset.LatestPartitionProfiles;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class LatestPartitionProfilesMapper {

  private LatestPartitionProfilesMapper() {}

  public static com.linkedin.datahub.graphql.generated.LatestPartitionProfiles map(
      @Nullable QueryContext context, @Nonnull final LatestPartitionProfiles gmsLatestProfiles) {
    final com.linkedin.datahub.graphql.generated.LatestPartitionProfiles result =
        new com.linkedin.datahub.graphql.generated.LatestPartitionProfiles();
    result.setTimestampMillis(gmsLatestProfiles.getTimestampMillis());
    result.setPartitionProfiles(
        gmsLatestProfiles.getPartitionProfiles().stream()
            .map(LatestPartitionProfilesMapper::mapPartitionProfile)
            .collect(Collectors.toList()));
    return result;
  }

  public static com.linkedin.datahub.graphql.generated.LatestPartitionProfiles map(
      @Nullable QueryContext context, @Nonnull final DataMap dataMap) {
    return map(context, new LatestPartitionProfiles(dataMap));
  }

  private static PartitionProfile mapPartitionProfile(
      com.linkedin.dataset.PartitionProfile gmsPartitionProfile) {
    PartitionProfile profile = new PartitionProfile();
    profile.setPartition(gmsPartitionProfile.getPartition());
    profile.setRowCount(gmsPartitionProfile.getRowCount());
    profile.setColumnCount(gmsPartitionProfile.getColumnCount());
    profile.setSizeInBytes(gmsPartitionProfile.getSizeInBytes());
    profile.setPartitionCount(gmsPartitionProfile.getPartitionCount());
    return profile;
  }
}

package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.BrowsePathsV2;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BrowsePathEntry;
import com.linkedin.datahub.graphql.generated.BrowsePathV2;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class BrowsePathsV2Mapper implements ModelMapper<BrowsePathsV2, BrowsePathV2> {

  public static final BrowsePathsV2Mapper INSTANCE = new BrowsePathsV2Mapper();

  public static BrowsePathV2 map(
      @Nullable QueryContext context, @Nonnull final BrowsePathsV2 metadata) {
    return INSTANCE.apply(context, metadata);
  }

  @Override
  public BrowsePathV2 apply(@Nullable QueryContext context, @Nonnull final BrowsePathsV2 input) {
    final BrowsePathV2 result = new BrowsePathV2();
    final List<BrowsePathEntry> path =
        input.getPath().stream()
            .map(p -> mapBrowsePathEntry(context, p))
            .collect(Collectors.toList());
    result.setPath(path);
    return result;
  }

  private BrowsePathEntry mapBrowsePathEntry(
      @Nullable QueryContext context, com.linkedin.common.BrowsePathEntry pathEntry) {
    final BrowsePathEntry entry = new BrowsePathEntry();
    entry.setName(pathEntry.getId());
    if (pathEntry.hasUrn() && pathEntry.getUrn() != null) {
      entry.setEntity(UrnToEntityMapper.map(context, pathEntry.getUrn()));
    }
    return entry;
  }
}

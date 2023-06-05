package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.BrowsePathsV2;
import com.linkedin.datahub.graphql.generated.BrowsePathEntry;
import com.linkedin.datahub.graphql.generated.BrowsePathV2;
import com.linkedin.datahub.graphql.generated.Embed;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class BrowsePathsV2Mapper implements ModelMapper<BrowsePathsV2, BrowsePathV2> {

  public static final BrowsePathsV2Mapper INSTANCE = new BrowsePathsV2Mapper();

  public static BrowsePathV2 map(@Nonnull final BrowsePathsV2 metadata) {
    return INSTANCE.apply(metadata);
  }

  @Override
  public BrowsePathV2 apply(@Nonnull final BrowsePathsV2 input) {
    final BrowsePathV2 result = new BrowsePathV2();
    final List<BrowsePathEntry> path = new ArrayList<>();
    input.getPath().forEach(pathEntry -> {
      final BrowsePathEntry entry = new BrowsePathEntry();
      entry.setName(pathEntry.getId());
      if (pathEntry.hasUrn() && pathEntry.getUrn() != null) {
        entry.setEntity(UrnToEntityMapper.map(pathEntry.getUrn()));
      }
      path.add(entry);
    });
    result.setPath(path);
    return result;
  }
}

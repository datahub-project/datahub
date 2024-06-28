package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class BrowsePathsMapper implements ModelMapper<List<String>, List<BrowsePath>> {

  public static final BrowsePathsMapper INSTANCE = new BrowsePathsMapper();

  public static List<BrowsePath> map(
      @Nullable final QueryContext context, @Nonnull final List<String> input) {
    return INSTANCE.apply(context, input);
  }

  @Override
  public List<BrowsePath> apply(
      @Nullable final QueryContext context, @Nonnull final List<String> input) {
    List<BrowsePath> results = new ArrayList<>();
    for (String pathStr : input) {
      results.add(BrowsePathMapper.map(context, pathStr));
    }
    return results;
  }
}

package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.datahub.graphql.generated.BrowsePath;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

public class BrowsePathsMapper implements ModelMapper<List<String>, List<BrowsePath>> {

  public static final BrowsePathsMapper INSTANCE = new BrowsePathsMapper();

  public static List<BrowsePath> map(@Nonnull final List<String> input) {
    return INSTANCE.apply(input);
  }

  @Override
  public List<BrowsePath> apply(@Nonnull final List<String> input) {
    List<BrowsePath> results = new ArrayList<>();
    for (String pathStr : input) {
      results.add(BrowsePathMapper.map(pathStr));
    }
    return results;
  }
}

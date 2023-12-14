package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.datahub.graphql.Constants;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class BrowsePathMapper implements ModelMapper<String, BrowsePath> {

  public static final BrowsePathMapper INSTANCE = new BrowsePathMapper();

  public static BrowsePath map(@Nonnull final String input) {
    return INSTANCE.apply(input);
  }

  @Override
  public BrowsePath apply(@Nonnull final String input) {
    final BrowsePath browsePath = new BrowsePath();
    final List<String> path =
        Arrays.stream(input.split(Constants.BROWSE_PATH_DELIMITER))
            .filter(pathComponent -> !"".equals(pathComponent))
            .collect(Collectors.toList());
    browsePath.setPath(path);
    return browsePath;
  }
}

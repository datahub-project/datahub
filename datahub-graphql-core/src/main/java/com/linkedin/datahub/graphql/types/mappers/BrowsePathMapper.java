/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.datahub.graphql.Constants;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class BrowsePathMapper implements ModelMapper<String, BrowsePath> {

  public static final BrowsePathMapper INSTANCE = new BrowsePathMapper();

  public static BrowsePath map(@Nullable final QueryContext context, @Nonnull final String input) {
    return INSTANCE.apply(context, input);
  }

  @Override
  public BrowsePath apply(@Nullable final QueryContext context, @Nonnull final String input) {
    final BrowsePath browsePath = new BrowsePath();
    final List<String> path =
        Arrays.stream(input.split(Constants.BROWSE_PATH_DELIMITER))
            .filter(pathComponent -> !"".equals(pathComponent))
            .collect(Collectors.toList());
    browsePath.setPath(path);
    return browsePath;
  }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.SubTypes;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.ArrayList;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SubTypesMapper
    implements ModelMapper<SubTypes, com.linkedin.datahub.graphql.generated.SubTypes> {

  public static final SubTypesMapper INSTANCE = new SubTypesMapper();

  public static com.linkedin.datahub.graphql.generated.SubTypes map(
      @Nullable QueryContext context, @Nonnull final SubTypes metadata) {
    return INSTANCE.apply(context, metadata);
  }

  @Override
  public com.linkedin.datahub.graphql.generated.SubTypes apply(
      @Nullable QueryContext context, @Nonnull final SubTypes input) {
    final com.linkedin.datahub.graphql.generated.SubTypes result =
        new com.linkedin.datahub.graphql.generated.SubTypes();
    result.setTypeNames(new ArrayList<>(input.getTypeNames()));
    return result;
  }
}

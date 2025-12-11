/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.common.mappers;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.SiblingProperties;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class SiblingsMapper
    implements ModelMapper<com.linkedin.common.Siblings, SiblingProperties> {

  public static final SiblingsMapper INSTANCE = new SiblingsMapper();

  public static SiblingProperties map(
      @Nullable QueryContext context, @Nonnull final com.linkedin.common.Siblings siblings) {
    return INSTANCE.apply(context, siblings);
  }

  @Override
  public SiblingProperties apply(
      @Nullable QueryContext context, @Nonnull final com.linkedin.common.Siblings siblings) {
    final SiblingProperties result = new SiblingProperties();
    result.setIsPrimary(siblings.isPrimary());
    result.setSiblings(
        siblings.getSiblings().stream()
            .filter(s -> context == null | canView(context.getOperationContext(), s))
            .map(s -> UrnToEntityMapper.map(context, s))
            .collect(Collectors.toList()));
    return result;
  }
}

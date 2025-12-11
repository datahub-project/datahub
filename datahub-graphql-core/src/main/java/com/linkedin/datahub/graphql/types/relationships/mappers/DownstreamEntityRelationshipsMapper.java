/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.relationships.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DownstreamEntityRelationships;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DownstreamEntityRelationshipsMapper
    implements ModelMapper<com.linkedin.common.EntityRelationships, DownstreamEntityRelationships> {

  public static final DownstreamEntityRelationshipsMapper INSTANCE =
      new DownstreamEntityRelationshipsMapper();

  public static DownstreamEntityRelationships map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.EntityRelationships relationships) {
    return INSTANCE.apply(context, relationships);
  }

  @Override
  public DownstreamEntityRelationships apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.EntityRelationships input) {
    final DownstreamEntityRelationships result = new DownstreamEntityRelationships();
    result.setEntities(
        input.getRelationships().stream()
            .map(r -> EntityRelationshipLegacyMapper.map(context, r))
            .collect(Collectors.toList()));
    return result;
  }
}

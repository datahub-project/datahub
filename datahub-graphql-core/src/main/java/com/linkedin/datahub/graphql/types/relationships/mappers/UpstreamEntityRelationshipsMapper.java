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
import com.linkedin.datahub.graphql.generated.UpstreamEntityRelationships;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UpstreamEntityRelationshipsMapper
    implements ModelMapper<com.linkedin.common.EntityRelationships, UpstreamEntityRelationships> {

  public static final UpstreamEntityRelationshipsMapper INSTANCE =
      new UpstreamEntityRelationshipsMapper();

  public static UpstreamEntityRelationships map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.EntityRelationships relationships) {
    return INSTANCE.apply(context, relationships);
  }

  @Override
  public UpstreamEntityRelationships apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.EntityRelationships input) {
    final UpstreamEntityRelationships result = new UpstreamEntityRelationships();
    result.setEntities(
        input.getRelationships().stream()
            .map(r -> EntityRelationshipLegacyMapper.map(context, r))
            .collect(Collectors.toList()));
    return result;
  }
}

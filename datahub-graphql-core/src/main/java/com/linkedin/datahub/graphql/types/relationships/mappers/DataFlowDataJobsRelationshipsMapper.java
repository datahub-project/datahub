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
import com.linkedin.datahub.graphql.generated.DataFlowDataJobsRelationships;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DataFlowDataJobsRelationshipsMapper
    implements ModelMapper<com.linkedin.common.EntityRelationships, DataFlowDataJobsRelationships> {

  public static final DataFlowDataJobsRelationshipsMapper INSTANCE =
      new DataFlowDataJobsRelationshipsMapper();

  public static DataFlowDataJobsRelationships map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.EntityRelationships relationships) {
    return INSTANCE.apply(context, relationships);
  }

  @Override
  public DataFlowDataJobsRelationships apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.EntityRelationships input) {
    final DataFlowDataJobsRelationships result = new DataFlowDataJobsRelationships();
    result.setEntities(
        input.getRelationships().stream()
            .map(r -> EntityRelationshipLegacyMapper.map(context, r))
            .collect(Collectors.toList()));
    return result;
  }
}

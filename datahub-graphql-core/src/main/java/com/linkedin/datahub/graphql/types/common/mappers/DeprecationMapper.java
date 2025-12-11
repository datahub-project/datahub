/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Deprecation;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DeprecationMapper
    implements ModelMapper<com.linkedin.common.Deprecation, Deprecation> {
  public static final DeprecationMapper INSTANCE = new DeprecationMapper();

  public static Deprecation map(
      @Nullable QueryContext context, @Nonnull final com.linkedin.common.Deprecation deprecation) {
    return INSTANCE.apply(context, deprecation);
  }

  @Override
  public Deprecation apply(
      @Nullable QueryContext context, @Nonnull final com.linkedin.common.Deprecation input) {
    final Deprecation result = new Deprecation();
    result.setActor(input.getActor().toString());
    result.setActorEntity(UrnToEntityMapper.map(context, input.getActor()));
    result.setDeprecated(input.isDeprecated());
    result.setDecommissionTime(input.getDecommissionTime());
    if (input.getReplacement() != null) {
      result.setReplacement(UrnToEntityMapper.map(context, input.getReplacement()));
    }
    result.setNote(input.getNote());
    return result;
  }
}

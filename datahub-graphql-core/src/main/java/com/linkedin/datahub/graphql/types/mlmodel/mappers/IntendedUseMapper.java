/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.IntendedUse;
import com.linkedin.datahub.graphql.generated.IntendedUserType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.NonNull;

public class IntendedUseMapper
    implements ModelMapper<com.linkedin.ml.metadata.IntendedUse, IntendedUse> {

  public static final IntendedUseMapper INSTANCE = new IntendedUseMapper();

  public static IntendedUse map(
      @Nullable QueryContext context,
      @NonNull final com.linkedin.ml.metadata.IntendedUse intendedUse) {
    return INSTANCE.apply(context, intendedUse);
  }

  @Override
  public IntendedUse apply(
      @Nullable QueryContext context,
      @NonNull final com.linkedin.ml.metadata.IntendedUse intendedUse) {
    final IntendedUse result = new IntendedUse();
    result.setOutOfScopeUses(intendedUse.getOutOfScopeUses());
    result.setPrimaryUses(intendedUse.getPrimaryUses());
    if (intendedUse.getPrimaryUsers() != null) {
      result.setPrimaryUsers(
          intendedUse.getPrimaryUsers().stream()
              .map(v -> IntendedUserType.valueOf(v.toString()))
              .collect(Collectors.toList()));
    }
    return result;
  }
}

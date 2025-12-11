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
import com.linkedin.datahub.graphql.generated.CaveatDetails;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nullable;
import lombok.NonNull;

public class CaveatsDetailsMapper
    implements ModelMapper<com.linkedin.ml.metadata.CaveatDetails, CaveatDetails> {

  public static final CaveatsDetailsMapper INSTANCE = new CaveatsDetailsMapper();

  public static CaveatDetails map(
      @Nullable QueryContext context, @NonNull final com.linkedin.ml.metadata.CaveatDetails input) {
    return INSTANCE.apply(context, input);
  }

  @Override
  public CaveatDetails apply(
      @Nullable QueryContext context, @NonNull final com.linkedin.ml.metadata.CaveatDetails input) {
    final CaveatDetails result = new CaveatDetails();

    result.setCaveatDescription(input.getCaveatDescription());
    result.setGroupsNotRepresented(input.getGroupsNotRepresented());
    result.setNeedsFurtherTesting(input.isNeedsFurtherTesting());
    return result;
  }
}

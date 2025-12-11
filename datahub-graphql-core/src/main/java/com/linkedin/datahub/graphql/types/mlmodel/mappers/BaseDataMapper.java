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
import com.linkedin.datahub.graphql.generated.BaseData;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nullable;
import lombok.NonNull;

public class BaseDataMapper implements ModelMapper<com.linkedin.ml.metadata.BaseData, BaseData> {
  public static final BaseDataMapper INSTANCE = new BaseDataMapper();

  public static BaseData map(
      @Nullable QueryContext context, @NonNull final com.linkedin.ml.metadata.BaseData input) {
    return INSTANCE.apply(context, input);
  }

  @Override
  public BaseData apply(
      @Nullable QueryContext context, @NonNull final com.linkedin.ml.metadata.BaseData input) {
    final BaseData result = new BaseData();
    result.setDataset(input.getDataset().toString());
    result.setMotivation(input.getMotivation());
    result.setPreProcessing(input.getPreProcessing());
    return result;
  }
}

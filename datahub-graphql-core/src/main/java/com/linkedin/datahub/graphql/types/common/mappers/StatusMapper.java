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
import com.linkedin.datahub.graphql.generated.Status;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class StatusMapper implements ModelMapper<com.linkedin.common.Status, Status> {

  public static final StatusMapper INSTANCE = new StatusMapper();

  public static Status map(
      @Nullable QueryContext context, @Nonnull final com.linkedin.common.Status metadata) {
    return INSTANCE.apply(context, metadata);
  }

  @Override
  public Status apply(
      @Nullable QueryContext context, @Nonnull final com.linkedin.common.Status input) {
    final Status result = new Status();
    result.setRemoved(input.isRemoved());
    return result;
  }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.corpuser.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CorpUserStatus;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class CorpUserStatusMapper
    implements ModelMapper<com.linkedin.identity.CorpUserStatus, CorpUserStatus> {

  public static final CorpUserStatusMapper INSTANCE = new CorpUserStatusMapper();

  public static CorpUserStatus map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.identity.CorpUserStatus corpUserStatus) {
    return INSTANCE.apply(context, corpUserStatus);
  }

  @Override
  public CorpUserStatus apply(
      @Nullable QueryContext context, @Nonnull final com.linkedin.identity.CorpUserStatus status) {
    // Warning- if the backend provides an unexpected value this will fail.
    return CorpUserStatus.valueOf(status.getStatus());
  }
}

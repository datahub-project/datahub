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
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.CorpUserInfo;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class CorpUserInfoMapper
    implements ModelMapper<com.linkedin.identity.CorpUserInfo, CorpUserInfo> {

  public static final CorpUserInfoMapper INSTANCE = new CorpUserInfoMapper();

  public static CorpUserInfo map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.identity.CorpUserInfo corpUserInfo) {
    return INSTANCE.apply(context, corpUserInfo);
  }

  @Override
  public CorpUserInfo apply(
      @Nullable QueryContext context, @Nonnull final com.linkedin.identity.CorpUserInfo info) {
    final CorpUserInfo result = new CorpUserInfo();
    result.setActive(info.isActive());
    result.setCountryCode(info.getCountryCode());
    result.setDepartmentId(info.getDepartmentId());
    result.setDepartmentName(info.getDepartmentName());
    result.setEmail(info.getEmail());
    result.setDisplayName(info.getDisplayName());
    result.setFirstName(info.getFirstName());
    result.setLastName(info.getLastName());
    result.setFullName(info.getFullName());
    result.setTitle(info.getTitle());
    if (info.hasManagerUrn()) {
      result.setManager(new CorpUser.Builder().setUrn(info.getManagerUrn().toString()).build());
    }
    return result;
  }
}

package com.linkedin.datahub.graphql.types.corpuser.mappers;

import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.CorpUserInfo;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class CorpUserInfoMapper
    implements ModelMapper<com.linkedin.identity.CorpUserInfo, CorpUserInfo> {

  public static final CorpUserInfoMapper INSTANCE = new CorpUserInfoMapper();

  public static CorpUserInfo map(@Nonnull final com.linkedin.identity.CorpUserInfo corpUserInfo) {
    return INSTANCE.apply(corpUserInfo);
  }

  @Override
  public CorpUserInfo apply(@Nonnull final com.linkedin.identity.CorpUserInfo info) {
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

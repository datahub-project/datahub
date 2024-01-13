package com.linkedin.datahub.graphql.types.corpgroup.mappers;

import com.linkedin.datahub.graphql.generated.CorpGroupInfo;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class CorpGroupInfoMapper
    implements ModelMapper<com.linkedin.identity.CorpGroupInfo, CorpGroupInfo> {

  public static final CorpGroupInfoMapper INSTANCE = new CorpGroupInfoMapper();

  public static CorpGroupInfo map(
      @Nonnull final com.linkedin.identity.CorpGroupInfo corpGroupInfo) {
    return INSTANCE.apply(corpGroupInfo);
  }

  @Override
  public CorpGroupInfo apply(@Nonnull final com.linkedin.identity.CorpGroupInfo info) {
    final CorpGroupInfo result = new CorpGroupInfo();
    result.setEmail(info.getEmail());
    result.setDescription(info.getDescription());
    result.setDisplayName(info.getDisplayName());
    if (info.hasAdmins()) {
      result.setAdmins(
          info.getAdmins().stream()
              .map(
                  urn -> {
                    final CorpUser corpUser = new CorpUser();
                    corpUser.setUrn(urn.toString());
                    return corpUser;
                  })
              .collect(Collectors.toList()));
    }
    if (info.hasMembers()) {
      result.setMembers(
          info.getMembers().stream()
              .map(
                  urn -> {
                    final CorpUser corpUser = new CorpUser();
                    corpUser.setUrn(urn.toString());
                    return corpUser;
                  })
              .collect(Collectors.toList()));
    }
    if (info.hasGroups()) {
      result.setGroups(
          info.getGroups().stream().map(urn -> (urn.toString())).collect(Collectors.toList()));
    }
    return result;
  }
}

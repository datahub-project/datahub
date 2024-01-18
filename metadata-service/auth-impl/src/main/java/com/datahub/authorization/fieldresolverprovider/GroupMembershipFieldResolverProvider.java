package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.GROUP_MEMBERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.NativeGroupMembership;
import com.linkedin.metadata.Constants;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Provides field resolver for owners given entitySpec */
@Slf4j
@RequiredArgsConstructor
public class GroupMembershipFieldResolverProvider implements EntityFieldResolverProvider {

  private final EntityClient _entityClient;
  private final Authentication _systemAuthentication;

  @Override
  public List<EntityFieldType> getFieldTypes() {
    return Collections.singletonList(EntityFieldType.GROUP_MEMBERSHIP);
  }

  @Override
  public FieldResolver getFieldResolver(EntitySpec entitySpec) {
    return FieldResolver.getResolverFromFunction(entitySpec, this::getGroupMembership);
  }

  private FieldResolver.FieldValue getGroupMembership(EntitySpec entitySpec) {
    Urn entityUrn = UrnUtils.getUrn(entitySpec.getEntity());
    EnvelopedAspect groupMembershipAspect;
    EnvelopedAspect nativeGroupMembershipAspect;
    List<Urn> groups = new ArrayList<>();
    try {
      EntityResponse response =
          _entityClient.getV2(
              entityUrn.getEntityType(),
              entityUrn,
              ImmutableSet.of(GROUP_MEMBERSHIP_ASPECT_NAME, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME),
              _systemAuthentication);
      if (response == null
          || !(response.getAspects().containsKey(Constants.GROUP_MEMBERSHIP_ASPECT_NAME)
              || response
                  .getAspects()
                  .containsKey(Constants.NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME))) {
        return FieldResolver.emptyFieldValue();
      }
      if (response.getAspects().containsKey(Constants.GROUP_MEMBERSHIP_ASPECT_NAME)) {
        groupMembershipAspect = response.getAspects().get(Constants.GROUP_MEMBERSHIP_ASPECT_NAME);
        GroupMembership groupMembership =
            new GroupMembership(groupMembershipAspect.getValue().data());
        groups.addAll(groupMembership.getGroups());
      }
      if (response.getAspects().containsKey(Constants.NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)) {
        nativeGroupMembershipAspect =
            response.getAspects().get(Constants.NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME);
        NativeGroupMembership nativeGroupMembership =
            new NativeGroupMembership(nativeGroupMembershipAspect.getValue().data());
        groups.addAll(nativeGroupMembership.getNativeGroups());
      }
    } catch (Exception e) {
      log.error("Error while retrieving group membership aspect for urn {}", entityUrn, e);
      return FieldResolver.emptyFieldValue();
    }
    return FieldResolver.FieldValue.builder()
        .values(groups.stream().map(Urn::toString).collect(Collectors.toSet()))
        .build();
  }
}

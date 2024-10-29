package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.GROUP_MEMBERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.NativeGroupMembership;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Provides field resolver for owners given entitySpec */
@Slf4j
@RequiredArgsConstructor
public class GroupMembershipFieldResolverProvider implements EntityFieldResolverProvider {

  private final SystemEntityClient _entityClient;

  @Override
  public List<EntityFieldType> getFieldTypes() {
    return Collections.singletonList(EntityFieldType.GROUP_MEMBERSHIP);
  }

  @Override
  public FieldResolver getFieldResolver(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {
    return FieldResolver.getResolverFromFunction(
        entitySpec, spec -> getGroupMembership(opContext, spec));
  }

  private FieldResolver.FieldValue getGroupMembership(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {

    EnvelopedAspect groupMembershipAspect;
    EnvelopedAspect nativeGroupMembershipAspect;
    List<Urn> groups = new ArrayList<>();
    try {
      if (entitySpec.getEntity().isEmpty()) {
        return FieldResolver.emptyFieldValue();
      }

      Urn entityUrn = UrnUtils.getUrn(entitySpec.getEntity());

      EntityResponse response =
          _entityClient.getV2(
              opContext,
              entityUrn.getEntityType(),
              entityUrn,
              ImmutableSet.of(GROUP_MEMBERSHIP_ASPECT_NAME, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME));
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
      log.error("Error while retrieving group membership aspect for entitySpec {}", entitySpec, e);
      return FieldResolver.emptyFieldValue();
    }
    return FieldResolver.FieldValue.builder()
        .values(groups.stream().map(Urn::toString).collect(Collectors.toSet()))
        .build();
  }
}

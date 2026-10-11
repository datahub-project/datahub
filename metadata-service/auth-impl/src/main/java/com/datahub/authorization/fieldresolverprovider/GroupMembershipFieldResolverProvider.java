package com.datahub.authorization.fieldresolverprovider;

import com.datahub.authentication.group.GroupService;
import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Provides field resolver for group membership given entitySpec */
@Slf4j
@RequiredArgsConstructor
public class GroupMembershipFieldResolverProvider implements EntityFieldResolverProvider {

  private final GroupService _groupService;

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
    try {
      if (entitySpec.getEntity().isEmpty()) {
        return FieldResolver.emptyFieldValue();
      }

      Urn entityUrn = UrnUtils.getUrn(entitySpec.getEntity());
      List<Urn> groups = _groupService.getGroupsForUser(opContext, entityUrn);
      if (groups.isEmpty()) {
        return FieldResolver.emptyFieldValue();
      }
      return FieldResolver.FieldValue.builder()
          .values(groups.stream().map(Urn::toString).collect(Collectors.toSet()))
          .build();
    } catch (Exception e) {
      log.error("Error while retrieving group membership aspect for entitySpec {}", entitySpec, e);
      return FieldResolver.emptyFieldValue();
    }
  }
}

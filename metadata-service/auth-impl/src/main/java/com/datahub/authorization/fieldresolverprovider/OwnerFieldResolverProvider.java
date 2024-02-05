package com.datahub.authorization.fieldresolverprovider;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Provides field resolver for owners given entitySpec */
@Slf4j
@RequiredArgsConstructor
public class OwnerFieldResolverProvider implements EntityFieldResolverProvider {

  private final EntityClient _entityClient;
  private final Authentication _systemAuthentication;

  @Override
  public List<EntityFieldType> getFieldTypes() {
    return Collections.singletonList(EntityFieldType.OWNER);
  }

  @Override
  public FieldResolver getFieldResolver(EntitySpec entitySpec) {
    return FieldResolver.getResolverFromFunction(entitySpec, this::getOwners);
  }

  private FieldResolver.FieldValue getOwners(EntitySpec entitySpec) {
    Urn entityUrn = UrnUtils.getUrn(entitySpec.getEntity());
    EnvelopedAspect ownershipAspect;
    try {
      EntityResponse response =
          _entityClient.getV2(
              entityUrn.getEntityType(),
              entityUrn,
              Collections.singleton(Constants.OWNERSHIP_ASPECT_NAME),
              _systemAuthentication);
      if (response == null || !response.getAspects().containsKey(Constants.OWNERSHIP_ASPECT_NAME)) {
        return FieldResolver.emptyFieldValue();
      }
      ownershipAspect = response.getAspects().get(Constants.OWNERSHIP_ASPECT_NAME);
    } catch (Exception e) {
      log.error("Error while retrieving domains aspect for urn {}", entityUrn, e);
      return FieldResolver.emptyFieldValue();
    }
    Ownership ownership = new Ownership(ownershipAspect.getValue().data());
    return FieldResolver.FieldValue.builder()
        .values(
            ownership.getOwners().stream()
                .map(owner -> owner.getOwner().toString())
                .collect(Collectors.toSet()))
        .build();
  }
}

package com.datahub.authorization.fieldresolverprovider;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.FieldResolver;
import com.datahub.authorization.ResourceSpec;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import java.util.Collections;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * Provides field resolver for owners given resourceSpec
 */
@Slf4j
@RequiredArgsConstructor
public class OwnerFieldResolverProvider implements ResourceFieldResolverProvider {

  private final EntityClient _entityClient;
  private final Authentication _systemAuthentication;

  @Override
  public ResourceFieldType getFieldType() {
    return ResourceFieldType.OWNER;
  }

  @Override
  public FieldResolver getFieldResolver(ResourceSpec resourceSpec) {
    return FieldResolver.getResolverFromFunction(resourceSpec, this::getOwners);
  }

  private FieldResolver.FieldValue getOwners(ResourceSpec resourceSpec) {
    Urn entityUrn = UrnUtils.getUrn(resourceSpec.getResource());
    EnvelopedAspect ownershipAspect;
    try {
      EntityResponse response = _entityClient.getV2(entityUrn.getEntityType(), entityUrn,
          Collections.singleton(Constants.OWNERSHIP_ASPECT_NAME), _systemAuthentication);
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
        .values(ownership.getOwners().stream().map(owner -> owner.getOwner().toString()).collect(Collectors.toSet()))
        .build();
  }
}

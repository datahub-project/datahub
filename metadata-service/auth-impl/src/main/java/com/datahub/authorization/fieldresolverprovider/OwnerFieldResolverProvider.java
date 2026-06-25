package com.datahub.authorization.fieldresolverprovider;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
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
public class OwnerFieldResolverProvider implements EntityFieldResolverProvider {

  private final SystemEntityClient _entityClient;

  @Override
  public List<EntityFieldType> getFieldTypes() {
    return Collections.singletonList(EntityFieldType.OWNER);
  }

  @Override
  public FieldResolver getFieldResolver(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {
    return FieldResolver.getResolverFromFunction(entitySpec, spec -> getOwners(opContext, spec));
  }

  private FieldResolver.FieldValue getOwners(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {
    if (entitySpec.getEntity().isEmpty()) {
      return FieldResolver.emptyFieldValue();
    }
    final Urn entityUrn = UrnUtils.getUrn(entitySpec.getEntity());
    // Share the per-request ownership cache with policy evaluation so a resource's ownership is
    // fetched at most once per request across both this field resolver and getOwnersForType.
    final List<Owner> owners =
        opContext
            .getAuthorizationContext()
            .getResourceOwnersByUrn()
            .computeIfAbsent(entityUrn, urn -> fetchOwners(opContext, urn));
    return FieldResolver.FieldValue.builder()
        .values(
            owners.stream().map(owner -> owner.getOwner().toString()).collect(Collectors.toSet()))
        .build();
  }

  private List<Owner> fetchOwners(@Nonnull OperationContext opContext, @Nonnull Urn entityUrn) {
    try {
      EntityResponse response =
          _entityClient.getV2(
              opContext,
              entityUrn.getEntityType(),
              entityUrn,
              Collections.singleton(Constants.OWNERSHIP_ASPECT_NAME));
      if (response == null || !response.getAspects().containsKey(Constants.OWNERSHIP_ASPECT_NAME)) {
        return Collections.emptyList();
      }
      // Copy out of the DataMap-backed OwnerArray before caching it in the shared per-request map.
      return new ArrayList<>(
          new Ownership(
                  response.getAspects().get(Constants.OWNERSHIP_ASPECT_NAME).getValue().data())
              .getOwners());
    } catch (Exception e) {
      log.error("Error while retrieving ownership aspect for urn {}", entityUrn, e);
      return Collections.emptyList();
    }
  }
}

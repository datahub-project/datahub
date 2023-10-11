package com.datahub.authorization.fieldresolverprovider;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.FieldResolver;
import com.datahub.authorization.ResourceFieldType;
import com.datahub.authorization.ResourceSpec;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Objects;

import static com.linkedin.metadata.Constants.*;

/**
 * Provides field resolver for domain given resourceSpec
 */
@Slf4j
@RequiredArgsConstructor
public class DataPlatformInstanceFieldResolverProvider implements ResourceFieldResolverProvider {

  private final EntityClient _entityClient;
  private final Authentication _systemAuthentication;

  @Override
  public ResourceFieldType getFieldType() {
    return ResourceFieldType.DATA_PLATFORM_INSTANCE;
  }

  @Override
  public FieldResolver getFieldResolver(ResourceSpec resourceSpec) {
    return FieldResolver.getResolverFromFunction(resourceSpec, this::getDataPlatformInstance);
  }

  private FieldResolver.FieldValue getDataPlatformInstance(ResourceSpec resourceSpec) {
    Urn entityUrn = UrnUtils.getUrn(resourceSpec.getResource());
    // In the case that the entity is a platform instance, the associated platform instance entity is the instance itself
    if (entityUrn.getEntityType().equals(DATA_PLATFORM_INSTANCE_ENTITY_NAME)) {
      return FieldResolver.FieldValue.builder()
          .values(Collections.singleton(entityUrn.toString()))
          .build();
    }

    EnvelopedAspect dataPlatformInstanceAspect;
    try {
      EntityResponse response = _entityClient.getV2(entityUrn.getEntityType(), entityUrn,
          Collections.singleton(DATA_PLATFORM_INSTANCE_ASPECT_NAME), _systemAuthentication);
      if (response == null || !response.getAspects().containsKey(DATA_PLATFORM_INSTANCE_ASPECT_NAME)) {
        return FieldResolver.emptyFieldValue();
      }
      dataPlatformInstanceAspect = response.getAspects().get(DATA_PLATFORM_INSTANCE_ASPECT_NAME);
    } catch (Exception e) {
      log.error("Error while retrieving platform instance aspect for urn {}", entityUrn, e);
      return FieldResolver.emptyFieldValue();
    }
    DataPlatformInstance dataPlatformInstance = new DataPlatformInstance(dataPlatformInstanceAspect.getValue().data());
    if (dataPlatformInstance.getInstance() == null) {
      return FieldResolver.emptyFieldValue();
    }
    return FieldResolver.FieldValue.builder()
        .values(Collections.singleton(Objects.requireNonNull(dataPlatformInstance.getInstance()).toString()))
        .build();
  }
}
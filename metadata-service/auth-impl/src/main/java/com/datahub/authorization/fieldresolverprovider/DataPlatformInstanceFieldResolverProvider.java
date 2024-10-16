package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.*;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.SystemEntityClient;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Provides field resolver for domain given resourceSpec */
@Slf4j
@RequiredArgsConstructor
public class DataPlatformInstanceFieldResolverProvider implements EntityFieldResolverProvider {

  private final SystemEntityClient _entityClient;

  @Override
  public List<EntityFieldType> getFieldTypes() {
    return Collections.singletonList(EntityFieldType.DATA_PLATFORM_INSTANCE);
  }

  @Override
  public FieldResolver getFieldResolver(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {
    return FieldResolver.getResolverFromFunction(
        entitySpec, spec -> getDataPlatformInstance(opContext, spec));
  }

  private FieldResolver.FieldValue getDataPlatformInstance(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {

    EnvelopedAspect dataPlatformInstanceAspect;
    try {
      if (entitySpec.getEntity().isEmpty()) {
        return FieldResolver.emptyFieldValue();
      }

      Urn entityUrn = UrnUtils.getUrn(entitySpec.getEntity());
      // In the case that the entity is a platform instance, the associated platform instance entity
      // is the instance itself
      if (entityUrn.getEntityType().equals(DATA_PLATFORM_INSTANCE_ENTITY_NAME)) {
        return FieldResolver.FieldValue.builder()
            .values(Collections.singleton(entityUrn.toString()))
            .build();
      }

      EntityResponse response =
          _entityClient.getV2(
              opContext,
              entityUrn.getEntityType(),
              entityUrn,
              Collections.singleton(DATA_PLATFORM_INSTANCE_ASPECT_NAME));
      if (response == null
          || !response.getAspects().containsKey(DATA_PLATFORM_INSTANCE_ASPECT_NAME)) {
        return FieldResolver.emptyFieldValue();
      }
      dataPlatformInstanceAspect = response.getAspects().get(DATA_PLATFORM_INSTANCE_ASPECT_NAME);
    } catch (Exception e) {
      log.error("Error while retrieving platform instance aspect for entitySpec {}", entitySpec, e);
      return FieldResolver.emptyFieldValue();
    }
    DataPlatformInstance dataPlatformInstance =
        new DataPlatformInstance(dataPlatformInstanceAspect.getValue().data());
    if (dataPlatformInstance.getInstance() == null) {
      return FieldResolver.emptyFieldValue();
    }
    return FieldResolver.FieldValue.builder()
        .values(
            Collections.singleton(
                Objects.requireNonNull(dataPlatformInstance.getInstance()).toString()))
        .build();
  }
}

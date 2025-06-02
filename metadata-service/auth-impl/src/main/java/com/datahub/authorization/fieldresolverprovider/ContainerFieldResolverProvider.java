package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.*;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.Container;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import io.datahubproject.metadata.context.OperationContext;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Provides field resolver for container given entitySpec */
@Slf4j
@RequiredArgsConstructor
public class ContainerFieldResolverProvider implements EntityFieldResolverProvider {

  private final SystemEntityClient _entityClient;

  @Override
  public List<EntityFieldType> getFieldTypes() {
    return Collections.singletonList(EntityFieldType.CONTAINER);
  }

  @Override
  public FieldResolver getFieldResolver(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {
    return FieldResolver.getResolverFromFunction(
        entitySpec, spec -> getContainers(opContext, spec));
  }

  private FieldResolver.FieldValue getContainers(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {

    Set<Urn> containerUrns = new HashSet<>();

    try {
      if (entitySpec.getEntity().isEmpty()) {
        return FieldResolver.emptyFieldValue();
      }

      Urn entityUrn = UrnUtils.getUrn(entitySpec.getEntity());

      // In the case that the entity is a container, include that as well
      if (entityUrn.getEntityType().equals(CONTAINER_ENTITY_NAME)) {
        containerUrns.add(entityUrn);
      }

      while (true) {
        EntityResponse response =
            _entityClient.getV2(
                opContext,
                entityUrn.getEntityType(),
                entityUrn,
                Collections.singleton(CONTAINER_ASPECT_NAME));
        if (response == null || !response.getAspects().containsKey(CONTAINER_ASPECT_NAME)) {
          break;
        }
        entityUrn =
            new Container(response.getAspects().get(CONTAINER_ASPECT_NAME).getValue().data())
                .getContainer();
        containerUrns.add(entityUrn);
      }
    } catch (Exception e) {
      log.error("Error while retrieving container aspect for entitySpec {}", entitySpec, e);
      return FieldResolver.emptyFieldValue();
    }

    return FieldResolver.FieldValue.builder()
        .values(containerUrns.stream().map(Object::toString).collect(Collectors.toSet()))
        .build();
  }
}

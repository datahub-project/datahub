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
import com.linkedin.metadata.graph.cache.client.BoundHierarchyAccess;
import com.linkedin.metadata.graph.cache.client.HierarchyBindings;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Provides field resolver for container given entitySpec */
@Slf4j
@RequiredArgsConstructor
public class ContainerFieldResolverProvider implements EntityFieldResolverProvider {

  private final SystemEntityClient _entityClient;

  @Override
  public java.util.List<EntityFieldType> getFieldTypes() {
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

    try {
      if (entitySpec.getEntity().isEmpty()) {
        return FieldResolver.emptyFieldValue();
      }

      final Urn entityUrn = UrnUtils.getUrn(entitySpec.getEntity());
      final Set<Urn> containerUrns;

      if (CONTAINER_ENTITY_NAME.equals(entityUrn.getEntityType())) {
        containerUrns =
            BoundHierarchyAccess.expandAncestors(
                opContext,
                HierarchyBindings.containerSpec(opContext),
                Collections.singleton(entityUrn));
      } else {
        Urn directParent = readDirectContainerParent(opContext, entityUrn);
        if (directParent == null) {
          return FieldResolver.emptyFieldValue();
        }
        containerUrns =
            BoundHierarchyAccess.expandAncestors(
                opContext,
                HierarchyBindings.containerSpec(opContext),
                Collections.singleton(directParent));
      }

      return FieldResolver.FieldValue.builder()
          .values(containerUrns.stream().map(Object::toString).collect(Collectors.toSet()))
          .build();
    } catch (Exception e) {
      log.error("Error while retrieving container aspect for entitySpec {}", entitySpec, e);
      return FieldResolver.emptyFieldValue();
    }
  }

  @Nullable
  private Urn readDirectContainerParent(
      @Nonnull OperationContext opContext, @Nonnull Urn entityUrn) {
    try {
      EntityResponse response =
          _entityClient.getV2(
              opContext,
              entityUrn.getEntityType(),
              entityUrn,
              Collections.singleton(CONTAINER_ASPECT_NAME));
      if (response == null || !response.getAspects().containsKey(CONTAINER_ASPECT_NAME)) {
        return null;
      }
      Container container =
          new Container(response.getAspects().get(CONTAINER_ASPECT_NAME).getValue().data());
      return container.hasContainer() ? container.getContainer() : null;
    } catch (Exception e) {
      log.error("Error while retrieving container aspect for entity {}", entityUrn, e);
      return null;
    }
  }
}

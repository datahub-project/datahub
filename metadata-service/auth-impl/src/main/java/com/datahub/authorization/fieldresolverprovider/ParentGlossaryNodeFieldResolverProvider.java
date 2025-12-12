package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.*;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Provides field resolver for parent glossary node hierarchy given a glossary node or glossary term
 * entitySpec. This resolver returns all parent nodes up the hierarchy chain.
 */
@Slf4j
@RequiredArgsConstructor
public class ParentGlossaryNodeFieldResolverProvider implements EntityFieldResolverProvider {

  private final SystemEntityClient _entityClient;

  @Override
  public List<EntityFieldType> getFieldTypes() {
    return Collections.singletonList(EntityFieldType.PARENT_GLOSSARY_NODE);
  }

  @Override
  public FieldResolver getFieldResolver(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {
    return FieldResolver.getResolverFromFunction(
        entitySpec, spec -> getParentGlossaryNodes(opContext, spec));
  }

  private FieldResolver.FieldValue getParentGlossaryNodes(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {

    try {
      if (entitySpec.getEntity().isEmpty()) {
        return FieldResolver.emptyFieldValue();
      }

      final Urn entityUrn = UrnUtils.getUrn(entitySpec.getEntity());
      final String entityType = entityUrn.getEntityType();

      if (!isGlossaryEntity(entityType)) {
        return FieldResolver.emptyFieldValue();
      }

      return buildFieldValue(collectParentNodeUrns(opContext, entityUrn, entityType));

    } catch (Exception e) {
      log.error("Error while retrieving parent glossary nodes for entitySpec {}", entitySpec, e);
      return FieldResolver.emptyFieldValue();
    }
  }

  private boolean isGlossaryEntity(String entityType) {
    return entityType.equals(GLOSSARY_NODE_ENTITY_NAME)
        || entityType.equals(GLOSSARY_TERM_ENTITY_NAME);
  }

  private Set<Urn> collectParentNodeUrns(
      @Nonnull OperationContext opContext, @Nonnull Urn startUrn, @Nonnull String startType) {
    Set<Urn> parentNodeUrns = new LinkedHashSet<>();
    Urn currentUrn = startUrn;
    String currentType = startType;

    while (currentUrn != null) {
      Urn parentNodeUrn = getParentNodeUrn(opContext, currentUrn, currentType);

      if (parentNodeUrn == null) {
        break;
      }

      if (parentNodeUrns.contains(parentNodeUrn)) {
        log.warn("Cycle detected in glossary node hierarchy at {}", parentNodeUrn);
        break;
      }

      parentNodeUrns.add(parentNodeUrn);
      currentUrn = parentNodeUrn;
      currentType = GLOSSARY_NODE_ENTITY_NAME;
    }

    return parentNodeUrns;
  }

  private Urn getParentNodeUrn(
      @Nonnull OperationContext opContext, @Nonnull Urn entityUrn, @Nonnull String entityType) {
    return entityType.equals(GLOSSARY_TERM_ENTITY_NAME)
        ? getParentNodeFromTerm(opContext, entityUrn)
        : getParentNodeFromNode(opContext, entityUrn);
  }

  private Urn getParentNodeFromTerm(@Nonnull OperationContext opContext, @Nonnull Urn termUrn) {
    try {
      EntityResponse response =
          _entityClient.getV2(
              opContext,
              GLOSSARY_TERM_ENTITY_NAME,
              termUrn,
              Collections.singleton(GLOSSARY_TERM_INFO_ASPECT_NAME));

      if (response != null && response.getAspects().containsKey(GLOSSARY_TERM_INFO_ASPECT_NAME)) {
        GlossaryTermInfo termInfo =
            new GlossaryTermInfo(
                response.getAspects().get(GLOSSARY_TERM_INFO_ASPECT_NAME).getValue().data());
        return termInfo.hasParentNode() ? termInfo.getParentNode() : null;
      }
    } catch (Exception e) {
      log.error("Error retrieving parent node for glossary term {}", termUrn, e);
    }
    return null;
  }

  private Urn getParentNodeFromNode(@Nonnull OperationContext opContext, @Nonnull Urn nodeUrn) {
    try {
      EntityResponse response =
          _entityClient.getV2(
              opContext,
              GLOSSARY_NODE_ENTITY_NAME,
              nodeUrn,
              Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME));

      if (response != null && response.getAspects().containsKey(GLOSSARY_NODE_INFO_ASPECT_NAME)) {
        GlossaryNodeInfo nodeInfo =
            new GlossaryNodeInfo(
                response.getAspects().get(GLOSSARY_NODE_INFO_ASPECT_NAME).getValue().data());
        return nodeInfo.hasParentNode() ? nodeInfo.getParentNode() : null;
      }
    } catch (Exception e) {
      log.error("Error retrieving parent node for glossary node {}", nodeUrn, e);
    }
    return null;
  }

  private FieldResolver.FieldValue buildFieldValue(Set<Urn> urns) {
    return FieldResolver.FieldValue.builder()
        .values(urns.stream().map(Object::toString).collect(Collectors.toSet()))
        .build();
  }
}

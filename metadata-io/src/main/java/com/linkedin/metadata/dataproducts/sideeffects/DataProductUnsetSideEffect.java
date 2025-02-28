package com.linkedin.metadata.dataproducts.sideeffects;

import static com.linkedin.metadata.Constants.DATA_PRODUCT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.aspect.patch.template.dataproduct.DataProductPropertiesTemplate;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.utils.QueryUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Side effect that enforces single data product being associated with each entity by removing any
 * previous relation when evaluation updates to Data Product Properties aspects.
 */
@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class DataProductUnsetSideEffect extends MCPSideEffect {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<ChangeMCP> applyMCPSideEffect(
      Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {
    return Stream.of();
  }

  @Override
  protected Stream<MCPItem> postMCPSideEffect(
      Collection<MCLItem> mclItems, @Nonnull RetrieverContext retrieverContext) {
    return mclItems.stream().flatMap(item -> generatePatchRemove(item, retrieverContext));
  }

  private static Stream<MCPItem> generatePatchRemove(
      MCLItem mclItem, @Nonnull RetrieverContext retrieverContext) {

    if (DATA_PRODUCT_PROPERTIES_ASPECT_NAME.equals(mclItem.getAspectName())) {

      DataProductProperties dataProductProperties = mclItem.getAspect(DataProductProperties.class);
      if (dataProductProperties == null) {
        log.error("Unable to process data product properties for urn: {}", mclItem.getUrn());
        return Stream.empty();
      }
      DataProductAssociationArray newDataProductAssociationArray =
          Optional.ofNullable(dataProductProperties.getAssets())
              .orElse(new DataProductAssociationArray());

      DataProductProperties previousDataProductProperties =
          mclItem.getPreviousAspect(DataProductProperties.class);

      if (!ChangeType.UPSERT.equals(mclItem.getChangeType())
          || previousDataProductProperties == null) {
        // CREATE/CREATE_ENTITY/RESTATE
        return generateUnsetMCPs(mclItem, newDataProductAssociationArray, retrieverContext);
      } else {
        // UPSERT with previous
        DataProductAssociationArray oldDataProductAssociationArray =
            Optional.ofNullable(previousDataProductProperties.getAssets())
                .orElse(new DataProductAssociationArray());

        DataProductAssociationArray additions =
            newDataProductAssociationArray.stream()
                .filter(association -> !oldDataProductAssociationArray.contains(association))
                .collect(Collectors.toCollection(DataProductAssociationArray::new));

        return generateUnsetMCPs(mclItem, additions, retrieverContext);
      }
    }
    return Stream.empty();
  }

  private static Stream<MCPItem> generateUnsetMCPs(
      @Nonnull MCLItem dataProductItem,
      @Nonnull DataProductAssociationArray dataProductAssociations,
      @Nonnull RetrieverContext retrieverContext) {
    List<MCPItem> mcpItems = new ArrayList<>();
    Map<String, List<GenericJsonPatch.PatchOp>> patchOpMap = new HashMap<>();

    for (DataProductAssociation dataProductAssociation : dataProductAssociations) {
      RelatedEntitiesScrollResult result =
          retrieverContext
              .getGraphRetriever()
              .scrollRelatedEntities(
                  null,
                  QueryUtils.newFilter(
                      "urn", dataProductAssociation.getDestinationUrn().toString()),
                  null,
                  EMPTY_FILTER,
                  ImmutableList.of("DataProductContains"),
                  QueryUtils.newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
                  Collections.emptyList(),
                  null,
                  10, // Should only ever be one, if ever greater than ten will decrease over time
                  // to become consistent
                  null,
                  null);
      if (!result.getEntities().isEmpty()) {
        for (RelatedEntities entity : result.getEntities()) {
          if (!dataProductItem.getUrn().equals(UrnUtils.getUrn(entity.getSourceUrn()))) {
            GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
            patchOp.setOp(PatchOperationType.REMOVE.getValue());
            patchOp.setPath(String.format("/assets/%s", entity.getDestinationUrn()));
            patchOpMap
                .computeIfAbsent(entity.getSourceUrn(), urn -> new ArrayList<>())
                .add(patchOp);
          }
        }
      }
    }
    for (String urn : patchOpMap.keySet()) {
      EntitySpec entitySpec =
          retrieverContext
              .getAspectRetriever()
              .getEntityRegistry()
              .getEntitySpec(DATA_PRODUCT_ENTITY_NAME);
      mcpItems.add(
          PatchItemImpl.builder()
              .urn(UrnUtils.getUrn(urn))
              .entitySpec(
                  retrieverContext
                      .getAspectRetriever()
                      .getEntityRegistry()
                      .getEntitySpec(DATA_PRODUCT_ENTITY_NAME))
              .aspectName(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
              .aspectSpec(entitySpec.getAspectSpec(DATA_PRODUCT_PROPERTIES_ASPECT_NAME))
              .patch(
                  GenericJsonPatch.builder()
                      .arrayPrimaryKeys(
                          Map.of(
                              DataProductPropertiesTemplate.ASSETS_FIELD_NAME,
                              List.of(DataProductPropertiesTemplate.KEY_FIELD_NAME)))
                      .patch(patchOpMap.get(urn))
                      .build()
                      .getJsonPatch())
              .auditStamp(dataProductItem.getAuditStamp())
              .systemMetadata(dataProductItem.getSystemMetadata())
              .build(retrieverContext.getAspectRetriever().getEntityRegistry()));
    }

    return mcpItems.stream();
  }
}

package com.linkedin.datahub.graphql.types.ermodelrelationship.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.ERModelRelationship;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.RelationshipFieldMapping;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.ermodelrelation.ERModelRelationshipProperties;
import com.linkedin.ermodelrelation.EditableERModelRelationshipProperties;
import com.linkedin.metadata.key.ERModelRelationshipKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class ERModelRelationMapper implements ModelMapper<EntityResponse, ERModelRelationship> {

  public static final ERModelRelationMapper INSTANCE = new ERModelRelationMapper();

  public static ERModelRelationship map(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public ERModelRelationship apply(
      @Nullable final QueryContext context, final EntityResponse entityResponse) {
    final ERModelRelationship result = new ERModelRelationship();
    final Urn entityUrn = entityResponse.getUrn();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.ER_MODEL_RELATIONSHIP);

    final EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<ERModelRelationship> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(ER_MODEL_RELATIONSHIP_KEY_ASPECT_NAME, this::mapERModelRelationKey);
    mappingHelper.mapToResult(
        context, ER_MODEL_RELATIONSHIP_PROPERTIES_ASPECT_NAME, this::mapProperties);
    if (aspectMap != null
        && aspectMap.containsKey(EDITABLE_ER_MODEL_RELATIONSHIP_PROPERTIES_ASPECT_NAME)) {
      mappingHelper.mapToResult(
          EDITABLE_ER_MODEL_RELATIONSHIP_PROPERTIES_ASPECT_NAME, this::mapEditableProperties);
    }
    if (aspectMap != null && aspectMap.containsKey(INSTITUTIONAL_MEMORY_ASPECT_NAME)) {
      mappingHelper.mapToResult(
          INSTITUTIONAL_MEMORY_ASPECT_NAME,
          (ermodelrelation, dataMap) ->
              ermodelrelation.setInstitutionalMemory(
                  InstitutionalMemoryMapper.map(
                      context, new InstitutionalMemory(dataMap), entityUrn)));
    }
    if (aspectMap != null && aspectMap.containsKey(OWNERSHIP_ASPECT_NAME)) {
      mappingHelper.mapToResult(
          OWNERSHIP_ASPECT_NAME,
          (ermodelrelation, dataMap) ->
              ermodelrelation.setOwnership(
                  OwnershipMapper.map(context, new Ownership(dataMap), entityUrn)));
    }
    if (aspectMap != null && aspectMap.containsKey(STATUS_ASPECT_NAME)) {
      mappingHelper.mapToResult(
          STATUS_ASPECT_NAME,
          (ermodelrelation, dataMap) ->
              ermodelrelation.setStatus(StatusMapper.map(context, new Status(dataMap))));
    }
    if (aspectMap != null && aspectMap.containsKey(GLOBAL_TAGS_ASPECT_NAME)) {
      mappingHelper.mapToResult(
          GLOBAL_TAGS_ASPECT_NAME,
          (ermodelrelation, dataMap) ->
              this.mapGlobalTags(context, ermodelrelation, dataMap, entityUrn));
    }
    if (aspectMap != null && aspectMap.containsKey(GLOSSARY_TERMS_ASPECT_NAME)) {
      mappingHelper.mapToResult(
          GLOSSARY_TERMS_ASPECT_NAME,
          (ermodelrelation, dataMap) ->
              ermodelrelation.setGlossaryTerms(
                  GlossaryTermsMapper.map(context, new GlossaryTerms(dataMap), entityUrn)));
    }
    return mappingHelper.getResult();
  }

  private void mapEditableProperties(
      @Nonnull ERModelRelationship ermodelrelation, @Nonnull DataMap dataMap) {
    final EditableERModelRelationshipProperties editableERModelRelationProperties =
        new EditableERModelRelationshipProperties(dataMap);
    ermodelrelation.setEditableProperties(
        com.linkedin.datahub.graphql.generated.ERModelRelationshipEditableProperties.builder()
            .setDescription(editableERModelRelationProperties.getDescription())
            .setName(editableERModelRelationProperties.getName())
            .build());
  }

  private void mapERModelRelationKey(
      @Nonnull ERModelRelationship ermodelrelation, @Nonnull DataMap datamap) {
    ERModelRelationshipKey ermodelrelationKey = new ERModelRelationshipKey(datamap);
    ermodelrelation.setId(ermodelrelationKey.getId());
  }

  private void mapProperties(
      @Nullable final QueryContext context,
      @Nonnull ERModelRelationship ermodelrelation,
      @Nonnull DataMap dataMap) {
    final ERModelRelationshipProperties ermodelrelationProperties =
        new ERModelRelationshipProperties(dataMap);
    ermodelrelation.setProperties(
        com.linkedin.datahub.graphql.generated.ERModelRelationshipProperties.builder()
            .setName(ermodelrelationProperties.getName())
            .setSource(createPartialDataset(ermodelrelationProperties.getSource()))
            .setDestination(createPartialDataset(ermodelrelationProperties.getDestination()))
            .setCreatedTime(
                ermodelrelationProperties.hasCreated()
                        && ermodelrelationProperties.getCreated().getTime() > 0
                    ? ermodelrelationProperties.getCreated().getTime()
                    : 0)
            .setRelationshipFieldMappings(
                ermodelrelationProperties.hasRelationshipFieldMappings()
                    ? this.mapERModelRelationFieldMappings(ermodelrelationProperties)
                    : null)
            .build());

    if (ermodelrelationProperties.hasCreated()
        && Objects.requireNonNull(ermodelrelationProperties.getCreated()).hasActor()) {
      ermodelrelation
          .getProperties()
          .setCreatedActor(
              UrnToEntityMapper.map(context, ermodelrelationProperties.getCreated().getActor()));
    }
  }

  private Dataset createPartialDataset(@Nonnull Urn datasetUrn) {

    Dataset partialDataset = new Dataset();

    partialDataset.setUrn(datasetUrn.toString());

    return partialDataset;
  }

  private List<RelationshipFieldMapping> mapERModelRelationFieldMappings(
      ERModelRelationshipProperties ermodelrelationProperties) {
    final List<RelationshipFieldMapping> relationshipFieldMappingList = new ArrayList<>();

    ermodelrelationProperties
        .getRelationshipFieldMappings()
        .forEach(
            relationshipFieldMapping ->
                relationshipFieldMappingList.add(
                    this.mapRelationshipFieldMappings(relationshipFieldMapping)));

    return relationshipFieldMappingList;
  }

  private com.linkedin.datahub.graphql.generated.RelationshipFieldMapping
      mapRelationshipFieldMappings(
          com.linkedin.ermodelrelation.RelationshipFieldMapping relationFieldMapping) {
    return com.linkedin.datahub.graphql.generated.RelationshipFieldMapping.builder()
        .setDestinationField(relationFieldMapping.getDestinationField())
        .setSourceField(relationFieldMapping.getSourceField())
        .build();
  }

  private void mapGlobalTags(
      @Nullable final QueryContext context,
      @Nonnull ERModelRelationship ermodelrelation,
      @Nonnull DataMap dataMap,
      @Nonnull final Urn entityUrn) {
    com.linkedin.datahub.graphql.generated.GlobalTags globalTags =
        GlobalTagsMapper.map(context, new GlobalTags(dataMap), entityUrn);
    ermodelrelation.setTags(globalTags);
  }
}

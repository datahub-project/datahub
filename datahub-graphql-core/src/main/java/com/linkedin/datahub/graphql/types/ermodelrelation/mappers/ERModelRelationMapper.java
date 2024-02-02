package com.linkedin.datahub.graphql.types.ermodelrelation.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.ERModelRelation;
import com.linkedin.datahub.graphql.generated.EntityType;
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
import com.linkedin.ermodelrelation.ERModelRelationProperties;
import com.linkedin.ermodelrelation.EditableERModelRelationProperties;
import com.linkedin.metadata.key.ERModelRelationKey;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class ERModelRelationMapper implements ModelMapper<EntityResponse, ERModelRelation> {

  public static final ERModelRelationMapper INSTANCE = new ERModelRelationMapper();

  public static ERModelRelation map(@Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(entityResponse);
  }

  public ERModelRelation apply(final EntityResponse entityResponse) {
    final ERModelRelation result = new ERModelRelation();
    final Urn entityUrn = entityResponse.getUrn();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.ERMODELRELATION);

    final EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<ERModelRelation> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(ERMODELRELATION_KEY_ASPECT_NAME, this::mapERModelRelationKey);
    mappingHelper.mapToResult(ERMODELRELATION_PROPERTIES_ASPECT_NAME, this::mapProperties);
    if (aspectMap != null
        && aspectMap.containsKey(EDITABLE_ERMODELRELATION_PROPERTIES_ASPECT_NAME)) {
      mappingHelper.mapToResult(
          EDITABLE_ERMODELRELATION_PROPERTIES_ASPECT_NAME, this::mapEditableProperties);
    }
    if (aspectMap != null && aspectMap.containsKey(INSTITUTIONAL_MEMORY_ASPECT_NAME)) {
      mappingHelper.mapToResult(
          INSTITUTIONAL_MEMORY_ASPECT_NAME,
          (ermodelrelation, dataMap) ->
              ermodelrelation.setInstitutionalMemory(
                  InstitutionalMemoryMapper.map(new InstitutionalMemory(dataMap), entityUrn)));
    }
    if (aspectMap != null && aspectMap.containsKey(OWNERSHIP_ASPECT_NAME)) {
      mappingHelper.mapToResult(
          OWNERSHIP_ASPECT_NAME,
          (ermodelrelation, dataMap) ->
              ermodelrelation.setOwnership(OwnershipMapper.map(new Ownership(dataMap), entityUrn)));
    }
    if (aspectMap != null && aspectMap.containsKey(STATUS_ASPECT_NAME)) {
      mappingHelper.mapToResult(
          STATUS_ASPECT_NAME,
          (ermodelrelation, dataMap) ->
              ermodelrelation.setStatus(StatusMapper.map(new Status(dataMap))));
    }
    if (aspectMap != null && aspectMap.containsKey(GLOBAL_TAGS_ASPECT_NAME)) {
      mappingHelper.mapToResult(
          GLOBAL_TAGS_ASPECT_NAME,
          (ermodelrelation, dataMap) -> this.mapGlobalTags(ermodelrelation, dataMap, entityUrn));
    }
    if (aspectMap != null && aspectMap.containsKey(GLOSSARY_TERMS_ASPECT_NAME)) {
      mappingHelper.mapToResult(
          GLOSSARY_TERMS_ASPECT_NAME,
          (ermodelrelation, dataMap) ->
              ermodelrelation.setGlossaryTerms(
                  GlossaryTermsMapper.map(new GlossaryTerms(dataMap), entityUrn)));
    }
    return mappingHelper.getResult();
  }

  private void mapEditableProperties(
      @Nonnull ERModelRelation ermodelrelation, @Nonnull DataMap dataMap) {
    final EditableERModelRelationProperties editableERModelRelationProperties =
        new EditableERModelRelationProperties(dataMap);
    ermodelrelation.setEditableProperties(
        com.linkedin.datahub.graphql.generated.ERModelRelationEditableProperties.builder()
            .setDescription(editableERModelRelationProperties.getDescription())
            .setName(editableERModelRelationProperties.getName())
            .build());
  }

  private void mapERModelRelationKey(
      @Nonnull ERModelRelation ermodelrelation, @Nonnull DataMap datamap) {
    ERModelRelationKey ermodelrelationKey = new ERModelRelationKey(datamap);
    ermodelrelation.setErmodelrelationId(ermodelrelationKey.getErmodelrelationId());
  }

  private void mapProperties(@Nonnull ERModelRelation ermodelrelation, @Nonnull DataMap dataMap) {
    final ERModelRelationProperties ermodelrelationProperties =
        new ERModelRelationProperties(dataMap);
    ermodelrelation.setProperties(
        com.linkedin.datahub.graphql.generated.ERModelRelationProperties.builder()
            .setName(ermodelrelationProperties.getName())
            .setDatasetA(createPartialDataset(ermodelrelationProperties.getDatasetA()))
            .setDatasetB(createPartialDataset(ermodelrelationProperties.getDatasetB()))
            .setErmodelrelationFieldMapping(
                mapERModelRelationFieldMappings(ermodelrelationProperties))
            .setCreatedTime(
                ermodelrelationProperties.hasCreated()
                        && ermodelrelationProperties.getCreated().getTime() > 0
                    ? ermodelrelationProperties.getCreated().getTime()
                    : 0)
            .build());
    if (ermodelrelationProperties.hasCreated()
        && ermodelrelationProperties.getCreated().hasActor()) {
      ermodelrelation
          .getProperties()
          .setCreatedActor(
              UrnToEntityMapper.map(ermodelrelationProperties.getCreated().getActor()));
    }
  }

  private Dataset createPartialDataset(@Nonnull Urn datasetUrn) {

    Dataset partialDataset = new Dataset();

    partialDataset.setUrn(datasetUrn.toString());

    return partialDataset;
  }

  private com.linkedin.datahub.graphql.generated.ERModelRelationFieldMapping
      mapERModelRelationFieldMappings(ERModelRelationProperties ermodelrelationProperties) {
    return com.linkedin.datahub.graphql.generated.ERModelRelationFieldMapping.builder()
        .setFieldMappings(
            ermodelrelationProperties.getErmodelrelationFieldMapping().getFieldMappings().stream()
                .map(this::mapFieldMap)
                .collect(Collectors.toList()))
        .build();
  }

  private com.linkedin.datahub.graphql.generated.FieldMap mapFieldMap(
      com.linkedin.ermodelrelation.FieldMap fieldMap) {
    return com.linkedin.datahub.graphql.generated.FieldMap.builder()
        .setAfield(fieldMap.getAfield())
        .setBfield(fieldMap.getBfield())
        .build();
  }

  private void mapGlobalTags(
      @Nonnull ERModelRelation ermodelrelation,
      @Nonnull DataMap dataMap,
      @Nonnull final Urn entityUrn) {
    com.linkedin.datahub.graphql.generated.GlobalTags globalTags =
        GlobalTagsMapper.map(new GlobalTags(dataMap), entityUrn);
    ermodelrelation.setTags(globalTags);
  }
}

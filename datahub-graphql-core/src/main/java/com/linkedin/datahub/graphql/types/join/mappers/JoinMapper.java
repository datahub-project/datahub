package com.linkedin.datahub.graphql.types.join.mappers;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Join;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.join.EditableJoinProperties;
import com.linkedin.join.JoinProperties;
import com.linkedin.metadata.key.JoinKey;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class JoinMapper implements ModelMapper<EntityResponse, Join> {

  public static final JoinMapper INSTANCE = new JoinMapper();

  public static Join map(@Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(entityResponse);
  }

  public Join apply(final EntityResponse entityResponse) {
    final Join result = new Join();
    final Urn entityUrn = entityResponse.getUrn();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.JOIN);

    final EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<Join> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(JOIN_KEY_ASPECT_NAME, this::mapJoinKey);
    mappingHelper.mapToResult(JOIN_PROPERTIES_ASPECT_NAME, this::mapProperties);
    mappingHelper.mapToResult(EDITABLE_JOIN_PROPERTIES_ASPECT_NAME, this::mapEditableProperties);
    mappingHelper.mapToResult(INSTITUTIONAL_MEMORY_ASPECT_NAME, (join, dataMap) ->
        join.setInstitutionalMemory(InstitutionalMemoryMapper.map(new InstitutionalMemory(dataMap))));
    mappingHelper.mapToResult(OWNERSHIP_ASPECT_NAME, (join, dataMap) ->
        join.setOwnership(OwnershipMapper.map(new Ownership(dataMap), entityUrn)));
    mappingHelper.mapToResult(STATUS_ASPECT_NAME, (join, dataMap) ->
        join.setStatus(StatusMapper.map(new Status(dataMap))));
    mappingHelper.mapToResult(GLOBAL_TAGS_ASPECT_NAME, (join, dataMap) -> this.mapGlobalTags(join, dataMap, entityUrn));
    mappingHelper.mapToResult(GLOSSARY_TERMS_ASPECT_NAME, (join, dataMap) ->
        join.setGlossaryTerms(GlossaryTermsMapper.map(new GlossaryTerms(dataMap), entityUrn)));
    mappingHelper.mapToResult(CONTAINER_ASPECT_NAME, this::mapContainers);
    return mappingHelper.getResult();
  }

  private void mapEditableProperties(@Nonnull Join join, @Nonnull DataMap dataMap) {
    final EditableJoinProperties editableJoinProperties = new EditableJoinProperties(dataMap);
    join.setEditableProperties(
        com.linkedin.datahub.graphql.generated.JoinEditableProperties.builder()
            .setDescription(editableJoinProperties.getDescription())
            .setName(editableJoinProperties.getName())
            .build()
    );
  }


  private void mapJoinKey(@Nonnull Join join, @Nonnull DataMap datamap) {
    JoinKey joinKey = new JoinKey(datamap);
    join.setJoinId(joinKey.getJoinId());
  }

  private void mapProperties(@Nonnull Join join, @Nonnull DataMap dataMap) {
    final JoinProperties joinProperties = new JoinProperties(dataMap);
    join.setProperties(
        com.linkedin.datahub.graphql.generated.JoinProperties.builder()
            .setName(joinProperties.getName())
            .setDatasetA(joinProperties.getDatasetA().toString())
            .setDatasetB(joinProperties.getDatasetB().toString())
        .setJoinFieldMappings(mapJoinFieldMappings(joinProperties))
        .build());
  }

  private com.linkedin.datahub.graphql.generated.JoinFieldMapping mapJoinFieldMappings(JoinProperties joinProperties) {
    return com.linkedin.datahub.graphql.generated.JoinFieldMapping.builder()
        .setDetails(joinProperties.getJoinFieldMappings().getDetails())
        .setFieldMapping(joinProperties.getJoinFieldMappings()
            .getFieldMapping()
                    .stream()
            .map(this::mapFieldMap)
                    .collect(Collectors.toList()))
        .build();
  }

  private com.linkedin.datahub.graphql.generated.FieldMap mapFieldMap(com.linkedin.join.FieldMap fieldMap) {
    return com.linkedin.datahub.graphql.generated.FieldMap.builder()
        .setAfield(fieldMap.getAfield())
        .setBfield(fieldMap.getBfield())
        .build();
  }

  private void mapGlobalTags(@Nonnull Join join, @Nonnull DataMap dataMap, @Nonnull final Urn entityUrn) {
    com.linkedin.datahub.graphql.generated.GlobalTags globalTags = GlobalTagsMapper.map(new GlobalTags(dataMap), entityUrn);
    join.setTags(globalTags);
  }

  private void mapContainers(@Nonnull Join join, @Nonnull DataMap dataMap) {
    final com.linkedin.container.Container gmsContainer = new com.linkedin.container.Container(dataMap);
    join.setContainer(Container
        .builder()
        .setType(EntityType.CONTAINER)
        .setUrn(gmsContainer.getContainer().toString())
        .build());
  }

}

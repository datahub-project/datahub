package com.linkedin.datahub.graphql.types.query;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.QueryEntity;
import com.linkedin.datahub.graphql.generated.QuerySubject;
import com.linkedin.datahub.graphql.generated.SchemaFieldEntity;
import com.linkedin.datahub.graphql.types.common.mappers.QueryPropertiesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.query.QueryProperties;
import com.linkedin.query.QuerySubjects;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryMapper implements ModelMapper<EntityResponse, QueryEntity> {

  public static final QueryMapper INSTANCE = new QueryMapper();

  public static QueryEntity map(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public QueryEntity apply(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final QueryEntity result = new QueryEntity();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.QUERY);
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<QueryEntity> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(
        QUERY_PROPERTIES_ASPECT_NAME,
        (entity, dataMap) ->
            entity.setProperties(QueryPropertiesMapper.map(context, new QueryProperties(dataMap))));
    mappingHelper.mapToResult(QUERY_SUBJECTS_ASPECT_NAME, this::mapQuerySubjects);
    mappingHelper.mapToResult(DATA_PLATFORM_INSTANCE_ASPECT_NAME, this::mapPlatform);
    return mappingHelper.getResult();
  }

  private void mapPlatform(@Nonnull QueryEntity query, @Nonnull DataMap dataMap) {
    DataPlatformInstance aspect = new DataPlatformInstance(dataMap);
    if (aspect.hasPlatform()) {
      final DataPlatform platform = new DataPlatform();
      platform.setUrn(aspect.getPlatform().toString());
      platform.setType(EntityType.DATA_PLATFORM);
      query.setPlatform(platform);
    }
  }

  @Nonnull
  private void mapQuerySubjects(@Nonnull QueryEntity query, @Nonnull DataMap dataMap) {
    QuerySubjects querySubjects = new QuerySubjects(dataMap);
    List<QuerySubject> res =
        querySubjects.getSubjects().stream()
            .map(this::mapQuerySubject)
            .collect(Collectors.toList());

    query.setSubjects(res);
  }

  @Nonnull
  private QuerySubject mapQuerySubject(com.linkedin.query.QuerySubject subject) {
    QuerySubject result = new QuerySubject();
    if (subject.getEntity().getEntityType().equals(DATASET_ENTITY_NAME)) {
      result.setDataset(createPartialDataset(subject.getEntity()));
    } else if (subject.getEntity().getEntityType().equals(SCHEMA_FIELD_ENTITY_NAME)) {
      String parentDataset = subject.getEntity().getEntityKey().get(0);
      result.setDataset(createPartialDataset(UrnUtils.getUrn(parentDataset)));
      result.setSchemaField(createPartialSchemaField(subject.getEntity()));
    }
    return result;
  }

  @Nonnull
  private Dataset createPartialDataset(@Nonnull Urn datasetUrn) {
    Dataset partialDataset = new Dataset();
    partialDataset.setUrn(datasetUrn.toString());
    return partialDataset;
  }

  @Nonnull
  private SchemaFieldEntity createPartialSchemaField(@Nonnull Urn urn) {
    SchemaFieldEntity partialSchemaField = new SchemaFieldEntity();
    partialSchemaField.setUrn(urn.toString());
    return partialSchemaField;
  }
}

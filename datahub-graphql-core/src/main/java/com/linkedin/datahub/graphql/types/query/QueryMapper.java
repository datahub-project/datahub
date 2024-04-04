package com.linkedin.datahub.graphql.types.query;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.GetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AuditStamp;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.QueryEntity;
import com.linkedin.datahub.graphql.generated.QueryLanguage;
import com.linkedin.datahub.graphql.generated.QuerySource;
import com.linkedin.datahub.graphql.generated.QueryStatement;
import com.linkedin.datahub.graphql.generated.QuerySubject;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
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
    mappingHelper.mapToResult(context, QUERY_PROPERTIES_ASPECT_NAME, this::mapQueryProperties);
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

  private void mapQueryProperties(
      @Nullable final QueryContext context, @Nonnull QueryEntity query, @Nonnull DataMap dataMap) {
    QueryProperties queryProperties = new QueryProperties(dataMap);
    com.linkedin.datahub.graphql.generated.QueryProperties res =
        new com.linkedin.datahub.graphql.generated.QueryProperties();

    // Query Source must be kept in sync.
    res.setSource(QuerySource.valueOf(queryProperties.getSource().toString()));
    res.setStatement(
        new QueryStatement(
            queryProperties.getStatement().getValue(),
            QueryLanguage.valueOf(queryProperties.getStatement().getLanguage().toString())));
    res.setName(queryProperties.getName(GetMode.NULL));
    res.setDescription(queryProperties.getDescription(GetMode.NULL));
    if (queryProperties.hasOrigin() && queryProperties.getOrigin() != null) {
      res.setOrigin(UrnToEntityMapper.map(context, queryProperties.getOrigin()));
    }

    AuditStamp created = new AuditStamp();
    created.setTime(queryProperties.getCreated().getTime());
    created.setActor(queryProperties.getCreated().getActor(GetMode.NULL).toString());
    res.setCreated(created);

    AuditStamp lastModified = new AuditStamp();
    lastModified.setTime(queryProperties.getLastModified().getTime());
    lastModified.setActor(queryProperties.getLastModified().getActor(GetMode.NULL).toString());
    res.setLastModified(lastModified);

    query.setProperties(res);
  }

  @Nonnull
  private void mapQuerySubjects(@Nonnull QueryEntity query, @Nonnull DataMap dataMap) {
    QuerySubjects querySubjects = new QuerySubjects(dataMap);
    List<QuerySubject> res =
        querySubjects.getSubjects().stream()
            .map(sub -> new QuerySubject(createPartialDataset(sub.getEntity())))
            .collect(Collectors.toList());

    query.setSubjects(res);
  }

  @Nonnull
  private Dataset createPartialDataset(@Nonnull Urn datasetUrn) {
    Dataset partialDataset = new Dataset();
    partialDataset.setUrn(datasetUrn.toString());
    return partialDataset;
  }
}

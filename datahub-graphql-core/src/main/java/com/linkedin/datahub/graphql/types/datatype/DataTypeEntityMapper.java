package com.linkedin.datahub.graphql.types.datatype;

import static com.linkedin.metadata.Constants.DATA_TYPE_INFO_ASPECT_NAME;

import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataTypeEntity;
import com.linkedin.datahub.graphql.generated.DataTypeInfo;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DataTypeEntityMapper implements ModelMapper<EntityResponse, DataTypeEntity> {

  public static final DataTypeEntityMapper INSTANCE = new DataTypeEntityMapper();

  public static DataTypeEntity map(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public DataTypeEntity apply(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final DataTypeEntity result = new DataTypeEntity();
    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.DATA_TYPE);
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<DataTypeEntity> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(DATA_TYPE_INFO_ASPECT_NAME, this::mapDataTypeInfo);

    // Set the standard Type ENUM for the data type.
    if (result.getInfo() != null) {
      result.getInfo().setType(DataTypeUrnMapper.getType(entityResponse.getUrn().toString()));
    }
    return mappingHelper.getResult();
  }

  private void mapDataTypeInfo(@Nonnull DataTypeEntity dataType, @Nonnull DataMap dataMap) {
    com.linkedin.datatype.DataTypeInfo gmsInfo = new com.linkedin.datatype.DataTypeInfo(dataMap);
    DataTypeInfo info = new DataTypeInfo();
    info.setQualifiedName(gmsInfo.getQualifiedName());
    if (gmsInfo.getDisplayName() != null) {
      info.setDisplayName(gmsInfo.getDisplayName());
    }
    if (gmsInfo.getDescription() != null) {
      info.setDescription(gmsInfo.getDescription());
    }
    dataType.setInfo(info);
  }
}

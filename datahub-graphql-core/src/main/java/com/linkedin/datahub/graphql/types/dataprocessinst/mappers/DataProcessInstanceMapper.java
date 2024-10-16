package com.linkedin.datahub.graphql.types.dataprocessinst.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataProcessInstance;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.dataprocess.DataProcessInstanceProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class DataProcessInstanceMapper implements ModelMapper<EntityResponse, DataProcessInstance> {

  public static final DataProcessInstanceMapper INSTANCE = new DataProcessInstanceMapper();

  public static DataProcessInstance map(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public DataProcessInstance apply(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final DataProcessInstance result = new DataProcessInstance();
    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.DATA_PROCESS_INSTANCE);

    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<DataProcessInstance> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(
        context, DATA_PROCESS_INSTANCE_PROPERTIES_ASPECT_NAME, this::mapDataProcessProperties);

    return mappingHelper.getResult();
  }

  private void mapDataProcessProperties(
      @Nonnull QueryContext context, @Nonnull DataProcessInstance dpi, @Nonnull DataMap dataMap) {
    DataProcessInstanceProperties dataProcessInstanceProperties =
        new DataProcessInstanceProperties(dataMap);
    dpi.setName(dataProcessInstanceProperties.getName());
    if (dataProcessInstanceProperties.hasCreated()) {
      dpi.setCreated(AuditStampMapper.map(context, dataProcessInstanceProperties.getCreated()));
    }
    if (dataProcessInstanceProperties.hasExternalUrl()) {
      dpi.setExternalUrl(dataProcessInstanceProperties.getExternalUrl().toString());
    }
  }
}

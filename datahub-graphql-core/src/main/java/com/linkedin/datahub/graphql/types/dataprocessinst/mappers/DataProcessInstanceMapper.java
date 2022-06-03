package com.linkedin.datahub.graphql.types.dataprocessinst.mappers;

import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.DataProcessInstance;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.dataprocess.DataProcessInstanceProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class DataProcessInstanceMapper implements ModelMapper<EntityResponse, DataProcessInstance> {

    public static final DataProcessInstanceMapper INSTANCE = new DataProcessInstanceMapper();

    public static DataProcessInstance map(@Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(entityResponse);
    }

    @Override
    public DataProcessInstance apply(@Nonnull final EntityResponse entityResponse) {
        final DataProcessInstance result = new DataProcessInstance();
        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.DATA_PROCESS_INSTANCE);

        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        MappingHelper<DataProcessInstance> mappingHelper = new MappingHelper<>(aspectMap, result);
        mappingHelper.mapToResult(DATA_PROCESS_INSTANCE_PROPERTIES_ASPECT_NAME, this::mapDataProcessProperties);

        return mappingHelper.getResult();
    }

    private void mapDataProcessProperties(@Nonnull DataProcessInstance dpi, @Nonnull DataMap dataMap) {
        DataProcessInstanceProperties dataProcessInstanceProperties = new DataProcessInstanceProperties(dataMap);
        dpi.setName(dataProcessInstanceProperties.getName());
        if (dataProcessInstanceProperties.hasCreated()) {
            dpi.setCreated(AuditStampMapper.map(dataProcessInstanceProperties.getCreated()));
        }
        if (dataProcessInstanceProperties.hasExternalUrl()) {
            dpi.setExternalUrl(dataProcessInstanceProperties.getExternalUrl().toString());
        }
    }
}

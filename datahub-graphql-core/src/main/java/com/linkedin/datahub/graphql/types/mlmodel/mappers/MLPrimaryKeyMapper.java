package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.common.Deprecation;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.MLFeatureDataType;
import com.linkedin.datahub.graphql.generated.MLPrimaryKey;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.MLPrimaryKeyKey;
import com.linkedin.ml.metadata.MLPrimaryKeyProperties;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 */
public class MLPrimaryKeyMapper implements ModelMapper<EntityResponse, MLPrimaryKey> {

    public static final MLPrimaryKeyMapper INSTANCE = new MLPrimaryKeyMapper();

    public static MLPrimaryKey map(@Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(entityResponse);
    }

    @Override
    public MLPrimaryKey apply(@Nonnull final EntityResponse entityResponse) {
        final MLPrimaryKey result = new MLPrimaryKey();
        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.MLPRIMARY_KEY);
        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        MappingHelper<MLPrimaryKey> mappingHelper = new MappingHelper<>(aspectMap, result);
        mappingHelper.mapToResult(OWNERSHIP_ASPECT_NAME, (mlPrimaryKey, dataMap) ->
            mlPrimaryKey.setOwnership(OwnershipMapper.map(new Ownership(dataMap))));
        mappingHelper.mapToResult(ML_PRIMARY_KEY_KEY_ASPECT_NAME, this::mapMLPrimaryKeyKey);
        mappingHelper.mapToResult(ML_PRIMARY_KEY_PROPERTIES_ASPECT_NAME, this::mapMLPrimaryKeyProperties);
        mappingHelper.mapToResult(INSTITUTIONAL_MEMORY_ASPECT_NAME, (mlPrimaryKey, dataMap) ->
            mlPrimaryKey.setInstitutionalMemory(InstitutionalMemoryMapper.map(new InstitutionalMemory(dataMap))));
        mappingHelper.mapToResult(STATUS_ASPECT_NAME, (mlPrimaryKey, dataMap) ->
            mlPrimaryKey.setStatus(StatusMapper.map(new Status(dataMap))));
        mappingHelper.mapToResult(DEPRECATION_ASPECT_NAME, (mlPrimaryKey, dataMap) ->
            mlPrimaryKey.setDeprecation(DeprecationMapper.map(new Deprecation(dataMap))));

        return mappingHelper.getResult();
    }

    private void mapMLPrimaryKeyKey(MLPrimaryKey mlPrimaryKey, DataMap dataMap) {
        MLPrimaryKeyKey mlPrimaryKeyKey = new MLPrimaryKeyKey(dataMap);
        mlPrimaryKey.setName(mlPrimaryKeyKey.getName());
        mlPrimaryKey.setFeatureNamespace(mlPrimaryKeyKey.getFeatureNamespace());
    }

    private void mapMLPrimaryKeyProperties(MLPrimaryKey mlPrimaryKey, DataMap dataMap) {
        MLPrimaryKeyProperties primaryKeyProperties = new MLPrimaryKeyProperties(dataMap);
        mlPrimaryKey.setPrimaryKeyProperties(MLPrimaryKeyPropertiesMapper.map(primaryKeyProperties));
        mlPrimaryKey.setDescription(primaryKeyProperties.getDescription());
        if (primaryKeyProperties.getDataType() != null) {
            mlPrimaryKey.setDataType(MLFeatureDataType.valueOf(primaryKeyProperties.getDataType().toString()));
        }
    }
}

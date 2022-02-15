package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.common.Deprecation;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.MLFeatureTable;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.MLFeatureTableKey;
import com.linkedin.ml.metadata.MLFeatureTableProperties;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 */
public class MLFeatureTableMapper implements ModelMapper<EntityResponse, MLFeatureTable> {

    public static final MLFeatureTableMapper INSTANCE = new MLFeatureTableMapper();

    public static MLFeatureTable map(@Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(entityResponse);
    }

    @Override
    public MLFeatureTable apply(@Nonnull final EntityResponse entityResponse) {
        final MLFeatureTable result = new MLFeatureTable();
        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.MLFEATURE_TABLE);
        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        MappingHelper<MLFeatureTable> mappingHelper = new MappingHelper<>(aspectMap, result);
        mappingHelper.mapToResult(OWNERSHIP_ASPECT_NAME, (mlFeatureTable, dataMap) ->
            mlFeatureTable.setOwnership(OwnershipMapper.map(new Ownership(dataMap))));
        mappingHelper.mapToResult(ML_FEATURE_TABLE_KEY_ASPECT_NAME, this::mapMLFeatureTableKey);
        mappingHelper.mapToResult(ML_FEATURE_TABLE_PROPERTIES_ASPECT_NAME, this::mapMLFeatureTableProperties);
        mappingHelper.mapToResult(INSTITUTIONAL_MEMORY_ASPECT_NAME, (mlFeatureTable, dataMap) ->
            mlFeatureTable.setInstitutionalMemory(InstitutionalMemoryMapper.map(new InstitutionalMemory(dataMap))));
        mappingHelper.mapToResult(STATUS_ASPECT_NAME, (mlFeatureTable, dataMap) ->
            mlFeatureTable.setStatus(StatusMapper.map(new Status(dataMap))));
        mappingHelper.mapToResult(DEPRECATION_ASPECT_NAME, (mlFeatureTable, dataMap) ->
            mlFeatureTable.setDeprecation(DeprecationMapper.map(new Deprecation(dataMap))));

        return mappingHelper.getResult();
    }

    private void mapMLFeatureTableKey(@Nonnull MLFeatureTable mlFeatureTable, @Nonnull DataMap dataMap) {
        MLFeatureTableKey mlFeatureTableKey = new MLFeatureTableKey(dataMap);
        mlFeatureTable.setName(mlFeatureTableKey.getName());
        DataPlatform partialPlatform = new DataPlatform();
        partialPlatform.setUrn(mlFeatureTableKey.getPlatform().toString());
        mlFeatureTable.setPlatform(partialPlatform);
    }

    private void mapMLFeatureTableProperties(@Nonnull MLFeatureTable mlFeatureTable, @Nonnull DataMap dataMap) {
        MLFeatureTableProperties featureTableProperties = new MLFeatureTableProperties(dataMap);
        mlFeatureTable.setFeatureTableProperties(MLFeatureTablePropertiesMapper.map(featureTableProperties));
        mlFeatureTable.setDescription(featureTableProperties.getDescription());
    }
}

package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.common.Deprecation;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.MLFeature;
import com.linkedin.datahub.graphql.generated.MLFeatureDataType;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.MLFeatureKey;
import com.linkedin.ml.metadata.MLFeatureProperties;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 */
public class MLFeatureMapper implements ModelMapper<EntityResponse, MLFeature> {

    public static final MLFeatureMapper INSTANCE = new MLFeatureMapper();

    public static MLFeature map(@Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(entityResponse);
    }

    @Override
    public MLFeature apply(@Nonnull final EntityResponse entityResponse) {
        final MLFeature result = new MLFeature();
        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.MLFEATURE);
        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        MappingHelper<MLFeature> mappingHelper = new MappingHelper<>(aspectMap, result);
        mappingHelper.mapToResult(ML_FEATURE_KEY_ASPECT_NAME, this::mapMLFeatureKey);
        mappingHelper.mapToResult(OWNERSHIP_ASPECT_NAME, (mlFeature, dataMap) ->
            mlFeature.setOwnership(OwnershipMapper.map(new Ownership(dataMap))));
        mappingHelper.mapToResult(ML_FEATURE_PROPERTIES_ASPECT_NAME, this::mapMLFeatureProperties);
        mappingHelper.mapToResult(INSTITUTIONAL_MEMORY_ASPECT_NAME, (mlFeature, dataMap) ->
            mlFeature.setInstitutionalMemory(InstitutionalMemoryMapper.map(new InstitutionalMemory(dataMap))));
        mappingHelper.mapToResult(STATUS_ASPECT_NAME, (mlFeature, dataMap) ->
            mlFeature.setStatus(StatusMapper.map(new Status(dataMap))));
        mappingHelper.mapToResult(DEPRECATION_ASPECT_NAME, (mlFeature, dataMap) ->
            mlFeature.setDeprecation(DeprecationMapper.map(new Deprecation(dataMap))));

        return mappingHelper.getResult();
    }

    private void mapMLFeatureKey(@Nonnull MLFeature mlFeature, @Nonnull DataMap dataMap) {
        MLFeatureKey mlFeatureKey = new MLFeatureKey(dataMap);
        mlFeature.setName(mlFeatureKey.getName());
        mlFeature.setFeatureNamespace(mlFeatureKey.getFeatureNamespace());
    }

    private void mapMLFeatureProperties(@Nonnull MLFeature mlFeature, @Nonnull DataMap dataMap) {
        MLFeatureProperties featureProperties = new MLFeatureProperties(dataMap);
        mlFeature.setFeatureProperties(MLFeaturePropertiesMapper.map(featureProperties));
        mlFeature.setDescription(featureProperties.getDescription());
        if (featureProperties.getDataType() != null) {
            mlFeature.setDataType(MLFeatureDataType.valueOf(featureProperties.getDataType().toString()));
        }
    }
}

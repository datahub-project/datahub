package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.datahub.util.ModelUtils;
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

        ModelUtils.getAspectsFromSnapshot(entityResponse).forEach(aspect -> {
            if (aspect instanceof Ownership) {
                Ownership ownership = Ownership.class.cast(aspect);
                result.setOwnership(OwnershipMapper.map(ownership));
            } else if (aspect instanceof MLFeatureKey) {
                MLFeatureKey mlFeatureKey = MLFeatureKey.class.cast(aspect);
                result.setName(mlFeatureKey.getName());
                result.setFeatureNamespace(mlFeatureKey.getFeatureNamespace());
            } else if (aspect instanceof MLFeatureProperties) {
                MLFeatureProperties featureProperties = MLFeatureProperties.class.cast(aspect);
                result.setFeatureProperties(MLFeaturePropertiesMapper.map(featureProperties));
                result.setDescription(featureProperties.getDescription());
                result.setDataType(MLFeatureDataType.valueOf(featureProperties.getDataType().toString()));
            } else if (aspect instanceof InstitutionalMemory) {
                InstitutionalMemory institutionalMemory = InstitutionalMemory.class.cast(aspect);
                result.setInstitutionalMemory(InstitutionalMemoryMapper.map(institutionalMemory));
            } else if (aspect instanceof Status) {
                Status status = Status.class.cast(aspect);
                result.setStatus(StatusMapper.map(status));
            } else if (aspect instanceof Deprecation) {
                Deprecation deprecation = Deprecation.class.cast(aspect);
                result.setDeprecation(DeprecationMapper.map(deprecation));
            }
        });

        return mappingHelper.getResult();
    }

    private void mapMLFeatureKey(MLFeature mlFeature, DataMap dataMap) {
        MLFeatureKey mlFeatureKey = new MLFeatureKey(dataMap);
        mlFeature.setName(mlFeatureKey.getName());
        mlFeature.setFeatureNamespace(mlFeatureKey.getFeatureNamespace());
    }
}

package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.common.Deprecation;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FabricType;
import com.linkedin.datahub.graphql.generated.MLModelGroup;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.MLModelGroupKey;
import com.linkedin.ml.metadata.MLModelGroupProperties;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 */
public class MLModelGroupMapper implements ModelMapper<EntityResponse, MLModelGroup> {

    public static final MLModelGroupMapper INSTANCE = new MLModelGroupMapper();

    public static MLModelGroup map(@Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(entityResponse);
    }

    @Override
    public MLModelGroup apply(@Nonnull final EntityResponse entityResponse) {
        final MLModelGroup result = new MLModelGroup();
        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.MLMODEL_GROUP);
        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        MappingHelper<MLModelGroup> mappingHelper = new MappingHelper<>(aspectMap, result);
        mappingHelper.mapToResult(OWNERSHIP_ASPECT_NAME, (mlModelGroup, dataMap) ->
            mlModelGroup.setOwnership(OwnershipMapper.map(new Ownership(dataMap))));
        mappingHelper.mapToResult(ML_MODEL_GROUP_KEY_ASPECT_NAME, this::mapToMLModelGroupKey);
        mappingHelper.mapToResult(ML_MODEL_GROUP_PROPERTIES_ASPECT_NAME, this::mapToMLModelGroupProperties);
        mappingHelper.mapToResult(STATUS_ASPECT_NAME, (mlModelGroup, dataMap) ->
            mlModelGroup.setStatus(StatusMapper.map(new Status(dataMap))));
        mappingHelper.mapToResult(DEPRECATION_ASPECT_NAME, (mlModelGroup, dataMap) ->
            mlModelGroup.setDeprecation(DeprecationMapper.map(new Deprecation(dataMap))));

        return mappingHelper.getResult();
    }

    private void mapToMLModelGroupKey(MLModelGroup mlModelGroup, DataMap dataMap) {
        MLModelGroupKey mlModelGroupKey = new MLModelGroupKey(dataMap);
        mlModelGroup.setName(mlModelGroupKey.getName());
        mlModelGroup.setOrigin(FabricType.valueOf(mlModelGroupKey.getOrigin().toString()));
        DataPlatform partialPlatform = new DataPlatform();
        partialPlatform.setUrn(mlModelGroupKey.getPlatform().toString());
        mlModelGroup.setPlatform(partialPlatform);
    }

    private void mapToMLModelGroupProperties(MLModelGroup mlModelGroup, DataMap dataMap) {
        MLModelGroupProperties modelGroupProperties = new MLModelGroupProperties(dataMap);
        mlModelGroup.setProperties(MLModelGroupPropertiesMapper.map(modelGroupProperties));
        if (modelGroupProperties.getDescription() != null) {
            mlModelGroup.setDescription(modelGroupProperties.getDescription());
        }
    }
}

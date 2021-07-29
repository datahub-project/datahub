package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.common.Deprecation;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.FabricType;
import com.linkedin.datahub.graphql.generated.MLModelGroup;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.key.MLModelGroupKey;
import com.linkedin.metadata.snapshot.MLModelGroupSnapshot;
import com.linkedin.ml.metadata.MLModelGroupProperties;

import javax.annotation.Nonnull;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 */
public class MLModelGroupSnapshotMapper implements ModelMapper<MLModelGroupSnapshot, MLModelGroup> {

    public static final MLModelGroupSnapshotMapper INSTANCE = new MLModelGroupSnapshotMapper();

    public static MLModelGroup map(@Nonnull final MLModelGroupSnapshot mlModelGroup) {
        return INSTANCE.apply(mlModelGroup);
    }

    @Override
    public MLModelGroup apply(@Nonnull final MLModelGroupSnapshot mlModel) {
        final MLModelGroup result = new MLModelGroup();
        result.setUrn(mlModel.getUrn().toString());
        result.setType(EntityType.MLMODEL_GROUP);

        ModelUtils.getAspectsFromSnapshot(mlModel).forEach(aspect -> {
            if (aspect instanceof Ownership) {
                Ownership ownership = Ownership.class.cast(aspect);
                result.setOwnership(OwnershipMapper.map(ownership));
            } else if (aspect instanceof MLModelGroupKey) {
                MLModelGroupKey mlModelGroupKey = MLModelGroupKey.class.cast(aspect);
                result.setName(mlModelGroupKey.getName());
                result.setOrigin(FabricType.valueOf(mlModelGroupKey.getOrigin().toString()));
                DataPlatform partialPlatform = new DataPlatform();
                partialPlatform.setUrn(mlModelGroupKey.getPlatform().toString());
                result.setPlatform(partialPlatform);
            } else if (aspect instanceof MLModelGroupProperties) {
                MLModelGroupProperties modelGroupProperties = MLModelGroupProperties.class.cast(aspect);
                result.setProperties(MLModelGroupPropertiesMapper.map(modelGroupProperties));
                if (modelGroupProperties.getDescription() != null) {
                    result.setDescription(modelGroupProperties.getDescription());
                }
            } else if (aspect instanceof Status) {
                Status status = Status.class.cast(aspect);
                result.setStatus(StatusMapper.map(status));
            } else if (aspect instanceof Deprecation) {
                Deprecation deprecation = Deprecation.class.cast(aspect);
                result.setDeprecation(DeprecationMapper.map(deprecation));
            }
        });

        return result;
    }
}

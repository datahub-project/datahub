package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.common.Deprecation;
import com.linkedin.common.Ownership;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Status;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.MLFeature;
import com.linkedin.datahub.graphql.generated.MLFeatureDataType;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.MLFeatureSnapshot;
import com.linkedin.ml.metadata.MLFeatureProperties;
import com.linkedin.metadata.key.MLFeatureKey;

import javax.annotation.Nonnull;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 */
public class MLFeatureSnapshotMapper implements ModelMapper<MLFeatureSnapshot, MLFeature> {

    public static final MLFeatureSnapshotMapper INSTANCE = new MLFeatureSnapshotMapper();

    public static MLFeature map(@Nonnull final MLFeatureSnapshot mlFeature) {
        return INSTANCE.apply(mlFeature);
    }

    @Override
    public MLFeature apply(@Nonnull final MLFeatureSnapshot mlFeature) {
        final MLFeature result = new MLFeature();
        result.setUrn(mlFeature.getUrn().toString());
        result.setType(EntityType.MLFEATURE);
        ModelUtils.getAspectsFromSnapshot(mlFeature).forEach(aspect -> {
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

        return result;
    }
}

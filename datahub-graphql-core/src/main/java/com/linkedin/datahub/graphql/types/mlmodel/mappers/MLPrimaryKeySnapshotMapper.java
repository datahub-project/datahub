package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.common.Deprecation;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.MLPrimaryKey;
import com.linkedin.datahub.graphql.generated.MLFeatureDataType;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.MLPrimaryKeySnapshot;
import com.linkedin.ml.metadata.MLPrimaryKeyProperties;
import com.linkedin.metadata.key.MLPrimaryKeyKey;

import javax.annotation.Nonnull;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 */
public class MLPrimaryKeySnapshotMapper implements ModelMapper<MLPrimaryKeySnapshot, MLPrimaryKey> {

    public static final MLPrimaryKeySnapshotMapper INSTANCE = new MLPrimaryKeySnapshotMapper();

    public static MLPrimaryKey map(@Nonnull final MLPrimaryKeySnapshot mlPrimaryKey) {
        return INSTANCE.apply(mlPrimaryKey);
    }

    @Override
    public MLPrimaryKey apply(@Nonnull final MLPrimaryKeySnapshot mlPrimaryKeySnapshot) {
        final MLPrimaryKey result = new MLPrimaryKey();
        result.setUrn(mlPrimaryKeySnapshot.getUrn().toString());
        result.setType(EntityType.MLPRIMARY_KEY);
        ModelUtils.getAspectsFromSnapshot(mlPrimaryKeySnapshot).forEach(aspect -> {
            if (aspect instanceof Ownership) {
                Ownership ownership = Ownership.class.cast(aspect);
                result.setOwnership(OwnershipMapper.map(ownership));
            } else if (aspect instanceof MLPrimaryKeyKey) {
                MLPrimaryKeyKey mlPrimaryKeyKey = MLPrimaryKeyKey.class.cast(aspect);
                result.setName(mlPrimaryKeyKey.getName());
                result.setFeatureNamespace(mlPrimaryKeyKey.getFeatureNamespace());
            } else if (aspect instanceof MLPrimaryKeyProperties) {
                MLPrimaryKeyProperties primaryKeyProperties = MLPrimaryKeyProperties.class.cast(aspect);
                result.setPrimaryKeyProperties(MLPrimaryKeyPropertiesMapper.map(primaryKeyProperties));
                result.setDescription(primaryKeyProperties.getDescription());
                result.setDataType(MLFeatureDataType.valueOf(primaryKeyProperties.getDataType().toString()));
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

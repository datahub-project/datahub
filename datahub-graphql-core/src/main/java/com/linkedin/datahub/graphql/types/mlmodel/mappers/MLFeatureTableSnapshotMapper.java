package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.common.Deprecation;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.MLFeatureTable;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.MLFeatureTableSnapshot;
import com.linkedin.ml.metadata.MLFeatureTableProperties;
import com.linkedin.metadata.key.MLFeatureTableKey;

import javax.annotation.Nonnull;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 */
public class MLFeatureTableSnapshotMapper implements ModelMapper<MLFeatureTableSnapshot, MLFeatureTable> {

    public static final MLFeatureTableSnapshotMapper INSTANCE = new MLFeatureTableSnapshotMapper();

    public static MLFeatureTable map(@Nonnull final MLFeatureTableSnapshot mlFeatureTable) {
        return INSTANCE.apply(mlFeatureTable);
    }

    @Override
    public MLFeatureTable apply(@Nonnull final MLFeatureTableSnapshot mlFeatureTable) {
        final MLFeatureTable result = new MLFeatureTable();
        result.setUrn(mlFeatureTable.getUrn().toString());
        result.setType(EntityType.MLFEATURE_TABLE);
        ModelUtils.getAspectsFromSnapshot(mlFeatureTable).forEach(aspect -> {
            if (aspect instanceof Ownership) {
                Ownership ownership = Ownership.class.cast(aspect);
                result.setOwnership(OwnershipMapper.map(ownership));
            } else if (aspect instanceof MLFeatureTableKey) {
                MLFeatureTableKey mlFeatureTableKey = MLFeatureTableKey.class.cast(aspect);
                result.setName(mlFeatureTableKey.getName());
                DataPlatform partialPlatform = new DataPlatform();
                partialPlatform.setUrn(mlFeatureTableKey.getPlatform().toString());
                result.setPlatform(partialPlatform);
            } else if (aspect instanceof MLFeatureTableProperties) {
                MLFeatureTableProperties featureTableProperties = MLFeatureTableProperties.class.cast(aspect);
                result.setFeatureTableProperties(MLFeatureTablePropertiesMapper.map(featureTableProperties));
                result.setDescription(featureTableProperties.getDescription());
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

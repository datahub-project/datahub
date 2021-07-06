package com.linkedin.datahub.graphql.types.mlmodel.mappers;


import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.MLFeatureDataType;
import com.linkedin.datahub.graphql.generated.MLPrimaryKeyProperties;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import lombok.NonNull;

import java.util.stream.Collectors;

public class MLPrimaryKeyPropertiesMapper implements ModelMapper<com.linkedin.ml.metadata.MLPrimaryKeyProperties, MLPrimaryKeyProperties> {

    public static final MLPrimaryKeyPropertiesMapper INSTANCE = new MLPrimaryKeyPropertiesMapper();

    public static MLPrimaryKeyProperties map(@NonNull final com.linkedin.ml.metadata.MLPrimaryKeyProperties mlPrimaryKeyProperties) {
        return INSTANCE.apply(mlPrimaryKeyProperties);
    }

    @Override
    public MLPrimaryKeyProperties apply(@NonNull final com.linkedin.ml.metadata.MLPrimaryKeyProperties mlPrimaryKeyProperties) {
        final MLPrimaryKeyProperties result = new MLPrimaryKeyProperties();

        result.setDescription(mlPrimaryKeyProperties.getDescription());
        result.setDataType(MLFeatureDataType.valueOf(mlPrimaryKeyProperties.getDataType().toString()));
        if (mlPrimaryKeyProperties.getVersion() != null) {
            result.setVersion(VersionTagMapper.map(mlPrimaryKeyProperties.getVersion()));
        }
        if (mlPrimaryKeyProperties.getSources() != null) {
            result.setSources(mlPrimaryKeyProperties
                .getSources()
                .stream()
                .map(Urn::toString)
                .collect(Collectors.toList()));
        }

        return result;
    }
}

package com.linkedin.datahub.graphql.types.mlmodel.mappers;


import java.util.stream.Collectors;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.MLModelProperties;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import lombok.NonNull;

public class MLModelPropertiesMapper implements ModelMapper<com.linkedin.ml.metadata.MLModelProperties, MLModelProperties> {

    public static final MLModelPropertiesMapper INSTANCE = new MLModelPropertiesMapper();

    public static MLModelProperties map(@NonNull final com.linkedin.ml.metadata.MLModelProperties mlModelProperties) {
        return INSTANCE.apply(mlModelProperties);
    }

    @Override
    public MLModelProperties apply(@NonNull final com.linkedin.ml.metadata.MLModelProperties mlModelProperties) {
        final MLModelProperties result = new MLModelProperties();

        result.setDate(mlModelProperties.getDate());
        result.setDescription(mlModelProperties.getDescription());
        if (mlModelProperties.getVersion() != null) {
            result.setVersion(mlModelProperties.getVersion().getVersionTag());
        }
        result.setType(mlModelProperties.getType());
        if (mlModelProperties.getHyperParameters() != null) {
            result.setHyperParameters(HyperParameterMapMapper.map(mlModelProperties.getHyperParameters()));
        }
        if (mlModelProperties.getMlFeatures() != null) {
            result.setMlFeatures(mlModelProperties
                .getMlFeatures()
                .stream()
                .map(Urn::toString)
                .collect(Collectors.toList()));
        }
        result.setTags(mlModelProperties.getTags());

        return result;
    }
}

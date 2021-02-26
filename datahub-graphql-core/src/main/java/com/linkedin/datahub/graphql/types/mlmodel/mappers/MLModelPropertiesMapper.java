package com.linkedin.datahub.graphql.types.mlmodel.mappers;


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
        if(mlModelProperties.getDate() != null) {
            result.setDate(mlModelProperties.getDate());
        }
        if(mlModelProperties.getDescription() != null) {
            result.setDescription(mlModelProperties.getDescription());
        }
        if(mlModelProperties.getVersion() != null) {
            result.setVersion(mlModelProperties.getVersion());
        }
        if(mlModelProperties.getType() != null) {
            result.setType(mlModelProperties.getType());
        }
        if(mlModelProperties.getTags() != null) {
            result.setTags(mlModelProperties.getTags());
        }

        return result;
    }
}

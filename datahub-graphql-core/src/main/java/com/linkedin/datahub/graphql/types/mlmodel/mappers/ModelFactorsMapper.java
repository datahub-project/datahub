package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.generated.ModelFactors;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import lombok.NonNull;

public class ModelFactorsMapper implements ModelMapper<com.linkedin.ml.metadata.ModelFactors,ModelFactors> {

    public static final ModelFactorsMapper INSTANCE = new ModelFactorsMapper();

    public static ModelFactors map(@NonNull final com.linkedin.ml.metadata.ModelFactors modelFactors) {
        return INSTANCE.apply(modelFactors);
    }

    @Override
    public ModelFactors apply(@NonNull final com.linkedin.ml.metadata.ModelFactors modelFactors) {
        final ModelFactors result = new ModelFactors();
        result.setEnvironment(modelFactors.getEnvironment());
        result.setEvaluationFactors(modelFactors.getEvaluationFactors());
        result.setGroups(modelFactors.getGroups());
        result.setInstrumentation(modelFactors.getInstrumentation());
        result.setRelevantFactors(modelFactors.getRelevantFactors());
        return result;
    }
}

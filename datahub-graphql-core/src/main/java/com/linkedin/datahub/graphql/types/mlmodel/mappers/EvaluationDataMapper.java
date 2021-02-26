package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import java.util.Objects;
import java.util.stream.Collectors;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.BaseData;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.ml.metadata.EvaluationData;

import lombok.NonNull;

public class EvaluationDataMapper implements ModelMapper<EvaluationData, BaseData> {
    public static final EvaluationDataMapper INSTANCE = new EvaluationDataMapper();

    public static BaseData map(@NonNull final EvaluationData evaluationData) {
        return INSTANCE.apply(evaluationData);
    }

    @Override
    public BaseData apply(@NonNull final EvaluationData evaluationData) {
        final BaseData result = new BaseData();
        if(evaluationData.getDatasets() != null) {
            result.setDatasets(Objects.requireNonNull(evaluationData.getDatasets()).stream().map(Urn::getContent).collect(Collectors.toList()));
        }
        result.setMotivation(evaluationData.getMotivation());
        result.setPreProcessing(evaluationData.getPreProcessing());
        return result;
    }
}

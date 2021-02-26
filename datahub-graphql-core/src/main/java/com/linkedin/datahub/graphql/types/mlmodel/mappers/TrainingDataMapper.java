package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import java.util.Objects;
import java.util.stream.Collectors;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.BaseData;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.ml.metadata.TrainingData;

import lombok.NonNull;

public class TrainingDataMapper implements ModelMapper<TrainingData, BaseData> {
    public static final TrainingDataMapper INSTANCE = new TrainingDataMapper();

    public static BaseData map(@NonNull final TrainingData trainingData) {
        return INSTANCE.apply(trainingData);
    }

    @Override
    public BaseData apply(@NonNull final TrainingData trainingData) {
        BaseData result = new BaseData();
        if(trainingData.getDataset() != null) {
            result.setDatasets(Objects.requireNonNull(trainingData.getDatasets()).stream().map(Urn::getContent).collect(Collectors.toList()));
        }
        result.setMotivation(trainingData.getMotivation());
        result.setPreProcessing(trainingData.getPreProcessing());
        return result;
    }
}

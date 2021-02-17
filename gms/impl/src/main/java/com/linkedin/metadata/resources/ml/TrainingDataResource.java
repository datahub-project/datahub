package com.linkedin.metadata.resources.ml;

import javax.annotation.Nonnull;

import com.linkedin.ml.metadata.TrainingData;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;

import lombok.extern.slf4j.Slf4j;

/**
 * Rest.li entry point: /mlModels/{mlModelKey}/trainingData
 */
@Slf4j
@RestLiCollection(name = "trainingData", namespace = "com.linkedin.ml", parent = MLModels.class)
public class TrainingDataResource extends BaseMLModelsAspectResource<TrainingData> {
    public TrainingDataResource() {
        super(TrainingData.class);
    }

    @RestMethod.Get
    @Nonnull
    @Override
    public Task<TrainingData> get(@Nonnull Long version) {
        return super.get(version);
    }

    @RestMethod.Create
    @Nonnull
    @Override
    public Task<CreateResponse> create(@Nonnull TrainingData trainingData) {
        return super.create(trainingData);
    }
}
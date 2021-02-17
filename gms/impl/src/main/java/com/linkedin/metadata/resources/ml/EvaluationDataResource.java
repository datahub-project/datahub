package com.linkedin.metadata.resources.ml;

import javax.annotation.Nonnull;

import com.linkedin.ml.metadata.EvaluationData;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;

import lombok.extern.slf4j.Slf4j;

/**
 * Rest.li entry point: /mlModels/{mlModelKey}/evaluationData
 */
@Slf4j
@RestLiCollection(name = "evaluationData", namespace = "com.linkedin.ml", parent = MLModels.class)
public class EvaluationDataResource extends BaseMLModelsAspectResource<EvaluationData> {
    public EvaluationDataResource() {
        super(EvaluationData.class);
    }

    @RestMethod.Get
    @Nonnull
    @Override
    public Task<EvaluationData> get(@Nonnull Long version) {
        return super.get(version);
    }

    @RestMethod.Create
    @Nonnull
    @Override
    public Task<CreateResponse> create(@Nonnull EvaluationData evaluationData) {
        return super.create(evaluationData);
    }
}
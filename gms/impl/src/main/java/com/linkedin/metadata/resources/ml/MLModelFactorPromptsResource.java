package com.linkedin.metadata.resources.ml;

import javax.annotation.Nonnull;

import com.linkedin.ml.metadata.MLModelFactorPrompts;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;

import lombok.extern.slf4j.Slf4j;

/**
 * Rest.li entry point: /mlModels/{mlModelKey}/mlModelFactorPrompts
 */
@Slf4j
@RestLiCollection(name = "mlModelFactorPrompts", namespace = "com.linkedin.ml", parent = MLModels.class)
public class MLModelFactorPromptsResource extends BaseMLModelsAspectResource<MLModelFactorPrompts> {
    public MLModelFactorPromptsResource() {
        super(MLModelFactorPrompts.class);
    }

    @RestMethod.Get
    @Nonnull
    @Override
    public Task<MLModelFactorPrompts> get(@Nonnull Long version) {
        return super.get(version);
    }

    @RestMethod.Create
    @Nonnull
    @Override
    public Task<CreateResponse> create(@Nonnull MLModelFactorPrompts mlModelFactorPrompts) {
        return super.create(mlModelFactorPrompts);
    }
}
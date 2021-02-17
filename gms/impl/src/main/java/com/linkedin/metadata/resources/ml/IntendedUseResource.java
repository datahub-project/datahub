package com.linkedin.metadata.resources.ml;

import javax.annotation.Nonnull;

import com.linkedin.ml.metadata.IntendedUse;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;

import lombok.extern.slf4j.Slf4j;

/**
 * Rest.li entry point: /mlModels/{mlModelKey}/intendedUse
 */
@Slf4j
@RestLiCollection(name = "intendedUse", namespace = "com.linkedin.ml", parent = MLModels.class)
public class IntendedUseResource extends BaseMLModelsAspectResource<IntendedUse> {
    public IntendedUseResource() {
        super(IntendedUse.class);
    }

    @RestMethod.Get
    @Nonnull
    @Override
    public Task<IntendedUse> get(@Nonnull Long version) {
        return super.get(version);
    }

    @RestMethod.Create
    @Nonnull
    @Override
    public Task<CreateResponse> create(@Nonnull IntendedUse intendedUse) {
        return super.create(intendedUse);
    }
}
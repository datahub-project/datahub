package com.linkedin.metadata.resources.ml;

import javax.annotation.Nonnull;

import com.linkedin.common.Cost;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;

import lombok.extern.slf4j.Slf4j;

/**
 * Rest.li entry point: /mlModels/{mlModelKey}/cost
 */
@Slf4j
@RestLiCollection(name = "cost", namespace = "com.linkedin.ml", parent = MLModels.class)
public class CostResource extends BaseMLModelsAspectResource<Cost> {
    public CostResource() {
        super(Cost.class);
    }

    @RestMethod.Get
    @Nonnull
    @Override
    public Task<Cost> get(@Nonnull Long version) {
        return super.get(version);
    }

    @RestMethod.Create
    @Nonnull
    @Override
    public Task<CreateResponse> create(@Nonnull Cost cost) {
        return super.create(cost);
    }
}
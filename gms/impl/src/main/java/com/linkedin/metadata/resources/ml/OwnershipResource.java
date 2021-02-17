package com.linkedin.metadata.resources.ml;

import javax.annotation.Nonnull;

import com.linkedin.common.Ownership;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;

import lombok.extern.slf4j.Slf4j;

/**
 * Rest.li entry point: /mlModels/{mlModelKey}/ownership
 */
@Slf4j
@RestLiCollection(name = "ownership", namespace = "com.linkedin.ml", parent = MLModels.class)
public class OwnershipResource extends BaseMLModelsAspectResource<Ownership> {

    public OwnershipResource() {
        super(Ownership.class);
    }

    @RestMethod.Get
    @Nonnull
    @Override
    public Task<Ownership> get(@Nonnull Long version) {
        return super.get(version);
    }

    @RestMethod.Create
    @Nonnull
    @Override
    public Task<CreateResponse> create(@Nonnull Ownership ownership) {
        return super.create(ownership);
    }
}

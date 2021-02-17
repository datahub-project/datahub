package com.linkedin.metadata.resources.ml;

import javax.annotation.Nonnull;

import com.linkedin.common.InstitutionalMemory;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;

import lombok.extern.slf4j.Slf4j;

/**
 * Rest.li entry point: /mlModels/{mlModelKey}/institutionalMemory
 */
@Slf4j
@RestLiCollection(name = "institutionalMemory", namespace = "com.linkedin.ml", parent = MLModels.class)
public class InstitutionalMemoryResource extends BaseMLModelsAspectResource<InstitutionalMemory> {
    public InstitutionalMemoryResource() {
        super(InstitutionalMemory.class);
    }

    @RestMethod.Get
    @Nonnull
    @Override
    public Task<InstitutionalMemory> get(@Nonnull Long version) {
        return super.get(version);
    }

    @RestMethod.Create
    @Nonnull
    @Override
    public Task<CreateResponse> create(@Nonnull InstitutionalMemory institutionalMemory) {
        return super.create(institutionalMemory);
    }
}

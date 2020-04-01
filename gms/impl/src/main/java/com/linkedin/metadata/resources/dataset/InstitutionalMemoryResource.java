package com.linkedin.metadata.resources.dataset;

import com.linkedin.common.InstitutionalMemory;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

/**
 * Rest.li entry point: /datasets/{datasetKey}/institutionalMemory
 */
@Slf4j
@RestLiCollection(name = "institutionalMemory", namespace = "com.linkedin.dataset", parent = Datasets.class)
public class InstitutionalMemoryResource extends BaseDatasetVersionedAspectResource<InstitutionalMemory> {
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

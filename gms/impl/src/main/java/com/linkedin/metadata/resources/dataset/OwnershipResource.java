package com.linkedin.metadata.resources.dataset;

import com.linkedin.common.Ownership;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

/**
 * Rest.li entry point: /datasets/{datasetKey}/rawOwnership
 */
@Slf4j
@RestLiCollection(name = "rawOwnership", namespace = "com.linkedin.dataset", parent = Datasets.class)
public class OwnershipResource extends BaseDatasetVersionedAspectResource<Ownership> {

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

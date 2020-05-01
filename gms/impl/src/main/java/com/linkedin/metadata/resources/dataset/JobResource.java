package com.linkedin.metadata.resources.dataset;

import com.linkedin.common.Job;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

/**
 * Rest.li entry point: /datasets/{datasetKey}/job
 */
@Slf4j
@RestLiCollection(name = "job", namespace = "com.linkedin.dataset", parent = Datasets.class)
public class JobResource extends BaseDatasetVersionedAspectResource<Job>{

    public JobResource() {
        super(Job.class);
    }

    @RestMethod.Get
    @Nonnull
    @Override
    public Task<Job> get(@Nonnull Long version) {
        return super.get(version);
    }

    @RestMethod.Create
    @Nonnull
    @Override
    public Task<CreateResponse> create(@Nonnull Job job) {
        return super.create(job);
    }
}

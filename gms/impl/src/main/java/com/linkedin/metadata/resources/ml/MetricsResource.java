package com.linkedin.metadata.resources.ml;

import javax.annotation.Nonnull;

import com.linkedin.ml.metadata.Metrics;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;

import lombok.extern.slf4j.Slf4j;

/**
 * Rest.li entry point: /mlModels/{mlModelKey}/metrics
 */
@Slf4j
@RestLiCollection(name = "metrics", namespace = "com.linkedin.ml", parent = MLModels.class)
public class MetricsResource extends BaseMLModelsAspectResource<Metrics> {
    public MetricsResource() {
        super(Metrics.class);
    }

    @RestMethod.Get
    @Nonnull
    @Override
    public Task<Metrics> get(@Nonnull Long version) {
        return super.get(version);
    }

    @RestMethod.Create
    @Nonnull
    @Override
    public Task<CreateResponse> create(@Nonnull Metrics metrics) {
        return super.create(metrics);
    }
}
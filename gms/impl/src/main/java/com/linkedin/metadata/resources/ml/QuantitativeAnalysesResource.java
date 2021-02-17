package com.linkedin.metadata.resources.ml;

import javax.annotation.Nonnull;

import com.linkedin.ml.metadata.QuantitativeAnalyses;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;

import lombok.extern.slf4j.Slf4j;

/**
 * Rest.li entry point: /mlModels/{mlModelKey}/quantitativeAnalyses
 */
@Slf4j
@RestLiCollection(name = "quantitativeAnalyses", namespace = "com.linkedin.ml", parent = MLModels.class)
public class QuantitativeAnalysesResource extends BaseMLModelsAspectResource<QuantitativeAnalyses> {
    public QuantitativeAnalysesResource() {
        super(QuantitativeAnalyses.class);
    }

    @RestMethod.Get
    @Nonnull
    @Override
    public Task<QuantitativeAnalyses> get(@Nonnull Long version) {
        return super.get(version);
    }

    @RestMethod.Create
    @Nonnull
    @Override
    public Task<CreateResponse> create(@Nonnull QuantitativeAnalyses quantitativeAnalyses) {
        return super.create(quantitativeAnalyses);
    }
}
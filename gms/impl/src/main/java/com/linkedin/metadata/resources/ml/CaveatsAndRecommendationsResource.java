package com.linkedin.metadata.resources.ml;

import javax.annotation.Nonnull;

import com.linkedin.ml.metadata.CaveatsAndRecommendations;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;

import lombok.extern.slf4j.Slf4j;

/**
 * Rest.li entry point: /mlModels/{mlModelKey}/caveatsAndRecommendations
 */
@Slf4j
@RestLiCollection(name = "caveatsAndRecommendations", namespace = "com.linkedin.ml", parent = MLModels.class)
public class CaveatsAndRecommendationsResource extends BaseMLModelsAspectResource<CaveatsAndRecommendations> {
    public CaveatsAndRecommendationsResource() {
        super(CaveatsAndRecommendations.class);
    }

    @RestMethod.Get
    @Nonnull
    @Override
    public Task<CaveatsAndRecommendations> get(@Nonnull Long version) {
        return super.get(version);
    }

    @RestMethod.Create
    @Nonnull
    @Override
    public Task<CreateResponse> create(@Nonnull CaveatsAndRecommendations caveatsAndRecommendations) {
        return super.create(caveatsAndRecommendations);
    }
}
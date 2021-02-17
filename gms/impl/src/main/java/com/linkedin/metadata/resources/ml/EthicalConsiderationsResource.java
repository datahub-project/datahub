package com.linkedin.metadata.resources.ml;

import javax.annotation.Nonnull;

import com.linkedin.ml.metadata.EthicalConsiderations;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;

import lombok.extern.slf4j.Slf4j;

/**
 * Rest.li entry point: /mlModels/{mlModelKey}/ethicalConsiderations
 */
@Slf4j
@RestLiCollection(name = "ethicalConsiderations", namespace = "com.linkedin.ml", parent = MLModels.class)
public class EthicalConsiderationsResource extends BaseMLModelsAspectResource<EthicalConsiderations> {
    public EthicalConsiderationsResource() {
        super(EthicalConsiderations.class);
    }

    @RestMethod.Get
    @Nonnull
    @Override
    public Task<EthicalConsiderations> get(@Nonnull Long version) {
        return super.get(version);
    }

    @RestMethod.Create
    @Nonnull
    @Override
    public Task<CreateResponse> create(@Nonnull EthicalConsiderations ethicalConsiderations) {
        return super.create(ethicalConsiderations);
    }
}
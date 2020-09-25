package com.linkedin.metadata.resources.ml;

import javax.annotation.Nonnull;

import com.linkedin.ml.metadata.SourceCode;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;

import lombok.extern.slf4j.Slf4j;

/**
 * Rest.li entry point: /mlModels/{mlModelKey}/sourceCode
 */
@Slf4j
@RestLiCollection(name = "sourceCode", namespace = "com.linkedin.ml", parent = MLModels.class)
public class SourceCodeResource extends BaseMLModelsAspectResource<SourceCode> {
    public SourceCodeResource() {
        super(SourceCode.class);
    }

    @RestMethod.Get
    @Nonnull
    @Override
    public Task<SourceCode> get(@Nonnull Long version) {
        return super.get(version);
    }

    @RestMethod.Create
    @Nonnull
    @Override
    public Task<CreateResponse> create(@Nonnull SourceCode sourceCode) {
        return super.create(sourceCode);
    }
}
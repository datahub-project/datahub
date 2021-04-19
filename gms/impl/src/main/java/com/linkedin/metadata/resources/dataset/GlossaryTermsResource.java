package com.linkedin.metadata.resources.dataset;

import com.linkedin.common.GlossaryTerms;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;

import javax.annotation.Nonnull;


/**
 * Rest.li entry point: /datasets/{datasetKey}/glossaryTerms
 */
@RestLiCollection(name = "glossaryTerms", namespace = "com.linkedin.dataset", parent = Datasets.class)
public final class GlossaryTermsResource extends BaseDatasetVersionedAspectResource<GlossaryTerms> {

    public GlossaryTermsResource() {
        super(GlossaryTerms.class);
    }

    @Nonnull
    @Override
    @RestMethod.Get
    public Task<GlossaryTerms> get(@Nonnull Long version) {
        return super.get(version);
    }

    @Nonnull
    @Override
    @RestMethod.Create
    public Task<CreateResponse> create(@Nonnull GlossaryTerms glossaryTerms) {
        return super.create(glossaryTerms);
    }
}
package com.linkedin.metadata.resources.dataset;

import com.linkedin.schema.SchemaMetadata;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

/**
 * Rest.li entry point: /datasets/{datasetKey}/schema
 */
@Slf4j
@RestLiCollection(name = "schema", namespace = "com.linkedin.dataset", parent = Datasets.class)
public class SchemaResource extends BaseDatasetVersionedAspectResource<SchemaMetadata> {
    public SchemaResource() {
        super(SchemaMetadata.class);
    }

    @RestMethod.Get
    @Nonnull
    @Override
    public Task<SchemaMetadata> get(@Nonnull Long version) {
        return super.get(version);
    }

    @RestMethod.Create
    @Nonnull
    @Override
    public Task<CreateResponse> create(@Nonnull SchemaMetadata schemaMetadata) {
        return super.create(schemaMetadata);
    }
}

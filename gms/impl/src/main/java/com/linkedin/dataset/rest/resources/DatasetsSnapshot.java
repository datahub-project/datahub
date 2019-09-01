package com.linkedin.dataset.rest.resources;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.DatasetKey;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.restli.BaseSnapshotResource;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import com.linkedin.metadata.snapshot.SnapshotKey;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.validation.CreateOnly;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.annotations.*;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;

/**
 * Rest.li entry point: /dataset/{datasetKey}/snapshot
 */
@Slf4j
@RestLiCollection(name = "snapshot", namespace = "com.linkedin.dataset", parent = Datasets.class)
@CreateOnly({"urn"})
public class DatasetsSnapshot extends BaseSnapshotResource<DatasetUrn, DatasetSnapshot, DatasetAspect> {
    private static final String DATASET_KEY = Datasets.class.getAnnotation(RestLiCollection.class).keyName();

    public DatasetsSnapshot() {
        super(DatasetSnapshot.class, DatasetAspect.class);
    }

    @Inject
    @Named("datasetDao")
    private BaseLocalDAO localDAO;

    @Override
    @RestMethod.Create
    public Task<CreateResponse> create(@Nonnull DatasetSnapshot datasetSnapshot) {
        return super.create(datasetSnapshot);
    }

    @Override
    @RestMethod.Get
    public Task<DatasetSnapshot> get(@Nonnull ComplexResourceKey<SnapshotKey, EmptyRecord> snapshotKey) {
        return super.get(snapshotKey);
    }

    @Nonnull
    @Override
    protected BaseLocalDAO getLocalDAO() {
        return localDAO;
    }

    @Nonnull
    @Override
    protected DatasetUrn getUrn(@PathKeysParam @Nonnull PathKeys keys) {
        DatasetKey key = keys.<ComplexResourceKey<DatasetKey, EmptyRecord>>get(DATASET_KEY).getKey();
        return new DatasetUrn(key.getPlatform(), key.getName(), key.getOrigin());
    }

    @Action(name = BACKFILL_ACTION_NAME)
    @Override
    @Nonnull
    public Task<DatasetSnapshot> backfill(@ActionParam(ASPECT_NAMES_PARAM_NAME) @Nonnull String[] aspectNames) {
        return super.backfill(aspectNames);
    }
}

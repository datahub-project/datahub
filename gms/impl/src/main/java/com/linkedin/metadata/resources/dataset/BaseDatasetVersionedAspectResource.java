package com.linkedin.metadata.resources.dataset;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.DatasetKey;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.restli.BaseVersionedAspectResource;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.annotations.PathKeysParam;
import com.linkedin.restli.server.annotations.RestLiCollection;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;


public class BaseDatasetVersionedAspectResource<ASPECT extends RecordTemplate>
        extends BaseVersionedAspectResource<DatasetUrn, DatasetAspect, ASPECT> {

    private static final String DATASET_KEY = Datasets.class.getAnnotation(RestLiCollection.class).keyName();

    @Inject
    @Named("entityService")
    private EntityService _entityService;

    public BaseDatasetVersionedAspectResource(@Nonnull Class<ASPECT> aspectClass) {
        super(DatasetAspect.class, aspectClass);
    }

    @Nonnull
    @Override
    public BaseLocalDAO getLocalDAO() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    protected DatasetUrn getUrn(@PathKeysParam @Nonnull PathKeys keys) {
        DatasetKey key = keys.<ComplexResourceKey<DatasetKey, EmptyRecord>>get(DATASET_KEY).getKey();
        return new DatasetUrn(key.getPlatform(), key.getName(), key.getOrigin());
    }

    @Nonnull
    protected EntityService getEntityService() {
        return _entityService;
    }
}

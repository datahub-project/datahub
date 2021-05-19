package com.linkedin.metadata.resources.dataprocess;

import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataprocess.DataProcessResourceKey;
import com.linkedin.metadata.aspect.DataProcessAspect;
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

public class BaseDataProcessesAspectResource<ASPECT extends RecordTemplate>
        extends BaseVersionedAspectResource<DataProcessUrn, DataProcessAspect, ASPECT> {
    private static final String DATA_PROCESS_KEY = DataProcesses.class.getAnnotation(RestLiCollection.class).keyName();

    public BaseDataProcessesAspectResource(Class<ASPECT> aspectClass) {
        super(DataProcessAspect.class, aspectClass);
    }

    @Inject
    @Named("entityService")
    private EntityService _entityService;

    @Nonnull
    @Override
    protected BaseLocalDAO<DataProcessAspect, DataProcessUrn> getLocalDAO() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    protected DataProcessUrn getUrn(@PathKeysParam @Nonnull PathKeys keys) {
        DataProcessResourceKey key = keys.<ComplexResourceKey<DataProcessResourceKey, EmptyRecord>>get(DATA_PROCESS_KEY).getKey();
        return new DataProcessUrn(key.getOrchestrator(), key.getName(), key.getOrigin());
    }

    @Nonnull
    protected EntityService getEntityService() {
        return _entityService;
    }
}

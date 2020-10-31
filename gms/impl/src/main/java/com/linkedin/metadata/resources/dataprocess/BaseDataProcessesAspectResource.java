package com.linkedin.metadata.resources.dataprocess;

import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataprocess.DataProcessKey;
import com.linkedin.metadata.aspect.DataProcessAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
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
    @Named("dataProcessDAO")
    private BaseLocalDAO localDAO;

    @Nonnull
    @Override
    protected BaseLocalDAO<DataProcessAspect, DataProcessUrn> getLocalDAO() {
        return localDAO;
    }

    @Nonnull
    @Override
    protected DataProcessUrn getUrn(@PathKeysParam @Nonnull PathKeys keys) {
        DataProcessKey key = keys.<ComplexResourceKey<DataProcessKey, EmptyRecord>>get(DATA_PROCESS_KEY).getKey();
        return new DataProcessUrn(key.getOrchestrator(), key.getName(), key.getOrigin());
    }
}

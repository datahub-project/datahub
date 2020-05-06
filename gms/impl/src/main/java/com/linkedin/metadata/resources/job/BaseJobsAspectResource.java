package com.linkedin.metadata.resources.job;

import com.linkedin.common.urn.JobUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.DatasetKey;
import com.linkedin.metadata.aspect.JobAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.EbeanLocalDAO;
import com.linkedin.metadata.restli.BaseVersionedAspectResource;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.annotations.RestLiCollection;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;

public class BaseJobsAspectResource <ASPECT extends RecordTemplate>
        extends BaseVersionedAspectResource<JobUrn, JobAspect, ASPECT> {

    private static final String JOB_KEY = Jobs.class.getAnnotation(RestLiCollection.class).keyName();

    public BaseJobsAspectResource(Class<ASPECT> aspectClass) {
        super(JobAspect.class, aspectClass);
    }

    @Inject
    @Named("jobDao")
    private EbeanLocalDAO localDAO;

    @Nonnull
    @Override
    protected BaseLocalDAO<JobAspect, JobUrn> getLocalDAO() {
        return localDAO;
    }

    @Nonnull
    @Override
    protected JobUrn getUrn(@Nonnull PathKeys keys) {
        DatasetKey key = keys.<ComplexResourceKey<DatasetKey, EmptyRecord>>get(JOB_KEY).getKey();
        return new JobUrn(key.getPlatform(), key.getName(), key.getOrigin());
    }
}

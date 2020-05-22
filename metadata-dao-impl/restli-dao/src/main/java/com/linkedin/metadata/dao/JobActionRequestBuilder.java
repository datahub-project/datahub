package com.linkedin.metadata.dao;

import com.linkedin.common.urn.JobUrn;
import com.linkedin.metadata.snapshot.JobSnapshot;

public class JobActionRequestBuilder extends BaseActionRequestBuilder<JobSnapshot, JobUrn> {
    private static final String BASE_URI_TEMPLATE = "corpUsers";

    public JobActionRequestBuilder() {
        super(JobSnapshot.class, JobUrn.class, BASE_URI_TEMPLATE);
    }
}

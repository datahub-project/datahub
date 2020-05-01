package com.linkedin.metadata.resources.dataset;

import com.linkedin.common.Job;
import com.linkedin.job.JobInfo;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

/**
 * Rest.li entry point: /datasets/{datasetKey}/jobInfo
 */
@Slf4j
@RestLiCollection(name = "jobInfo", namespace = "com.linkedin.dataset", parent = Datasets.class)
public class JobInfoResource extends BaseDatasetVersionedAspectResource<JobInfo>{

	public JobInfoResource() {
		super(JobInfo.class);
	}

	@RestMethod.Get
	@Nonnull
	@Override
	public Task<JobInfo> get(@Nonnull Long version) {
		return super.get(version);
	}

	@RestMethod.Create
	@Nonnull
	@Override
	public Task<CreateResponse> create(@Nonnull JobInfo jobInfo) {
		return super.create(jobInfo);
	}
}

package com.linkedin.metadata.resources.job;

import com.linkedin.job.JobInfo;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

/**
 * Rest.li entry point: /datasets/{datasetKey}/jobInfo
 */
@Slf4j
@RestLiCollection(name = "jobInfo", namespace = "com.linkedin.job", parent = Jobs.class)
public class JobInfoResource extends BaseJobsAspectResource<JobInfo> {

	public JobInfoResource() {
		super(JobInfo.class);
	}

	@RestMethod.Create
	@Nonnull
	@Override
	public Task<CreateResponse> create(@Nonnull JobInfo jobInfo) {
		return super.create(jobInfo);
	}

	@Nonnull
	@Override
	@RestMethod.Get
	public Task<JobInfo> get(@QueryParam("version") @Optional("0") @Nonnull Long version) {
		return super.get(version);
	}
}

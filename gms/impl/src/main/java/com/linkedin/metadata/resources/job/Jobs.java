package com.linkedin.metadata.resources.job;

import com.linkedin.common.urn.JobUrn;
import com.linkedin.job.Job;
import com.linkedin.job.JobKey;
import com.linkedin.job.JobInfo;
import com.linkedin.metadata.aspect.JobAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.*;
import com.linkedin.metadata.restli.BaseSearchableEntityResource;
import com.linkedin.metadata.search.JobDocument;
import com.linkedin.metadata.snapshot.JobSnapshot;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.annotations.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.linkedin.metadata.restli.RestliConstants.*;

@RestLiCollection(name = "jobs", namespace = "com.linkedin.job", keyName = "job")
public class Jobs extends BaseSearchableEntityResource<
		// @formatter:off
		JobKey,
		Job,
		JobUrn,
		JobSnapshot,
		JobAspect,
		JobDocument> {
	// @formatter:on

	public Jobs() {
		super(JobSnapshot.class, JobAspect.class);
	}

	@Inject
	@Named("jobDao")
	private BaseLocalDAO _localDAO;

	@Inject
	@Named("jobSearchDao")
	private BaseSearchDAO _searchDAO;

//	@Inject
//	@Named("jobBrowseDao")
//	private BaseBrowseDAO _browseDAO;


//	@Nonnull
//	@Override
//	protected BaseBrowseDAO getBrowseDAO() {
//		return _browseDAO;
//	}

	@Nonnull
	@Override
	protected BaseSearchDAO<JobDocument> getSearchDAO() {
		return _searchDAO;
	}

	@Nonnull
	@Override
	protected BaseLocalDAO<JobAspect, JobUrn> getLocalDAO() {
		return _localDAO;
	}

	@Nonnull
	@Override
	protected JobUrn createUrnFromString(@Nonnull String urnString) throws Exception {
		return JobUrn.createFromString(urnString);
	}

	@Nonnull
	@Override
	protected JobUrn toUrn(@Nonnull JobKey key) {
		return new JobUrn(key.getPlatform(), key.getName(), key.getOrigin());
	}

	@Nonnull
	@Override
	protected JobKey toKey(@Nonnull JobUrn urn) {
		return new JobKey()
				.setPlatform(urn.getPlatformEntity())
				.setName(urn.getJobNameEntity())
				.setOrigin(urn.getOriginEntity());
	}

	@Nonnull
	@Override
	protected Job toValue(@Nonnull JobSnapshot jobSnapshot) {
		final Job value = new Job()
				.setPlatform(jobSnapshot.getUrn().getPlatformEntity())
				.setName(jobSnapshot.getUrn().getJobNameEntity())
				.setOrigin(jobSnapshot.getUrn().getOriginEntity())
				.setUrn(jobSnapshot.getUrn());
		ModelUtils.getAspectsFromSnapshot(jobSnapshot).forEach(aspect -> {
			if (aspect instanceof JobInfo) {
				JobInfo jobInfo = JobInfo.class.cast(aspect);
				if (jobInfo.hasInputs())
					value.setInputs(jobInfo.getInputs());
				if (jobInfo.hasOutputs())
					value.setOutputs(jobInfo.getOutputs());
			}
		});

		return value;
	}

	@Nonnull
	private JobInfo getJobInfoAspect(@Nonnull Job job) {
		final JobInfo jobInfo = new JobInfo();
		if (job.hasInputs()) {
			jobInfo.setInputs(job.getInputs());
		}
		if (job.hasOutputs()) {
			jobInfo.setOutputs(job.getOutputs());
		}
		return jobInfo;
	}

	@Nonnull
	@Override
	protected JobSnapshot toSnapshot(@Nonnull Job job, @Nonnull JobUrn urn) {
		final List<JobAspect> aspects = new ArrayList<>();
		aspects.add(ModelUtils.newAspectUnion(JobAspect.class, getJobInfoAspect(job)));
		return ModelUtils.newSnapshot(JobSnapshot.class, urn, aspects);
	}

	@RestMethod.Get
	@Override
	@Nonnull
	public Task<Job> get(@Nonnull ComplexResourceKey<JobKey, EmptyRecord> key,
	                         @QueryParam(PARAM_ASPECTS) @Optional("[]") String[] aspectNames) {
		return super.get(key, aspectNames);
	}

	@RestMethod.BatchGet
	@Override
	@Nonnull
	public Task<Map<ComplexResourceKey<JobKey, EmptyRecord>, Job>> batchGet(
			@Nonnull Set<ComplexResourceKey<JobKey, EmptyRecord>> keys,
			@QueryParam(PARAM_ASPECTS) @Optional("[]") String[] aspectNames) {
		return super.batchGet(keys, aspectNames);
	}

	@Finder(FINDER_SEARCH)
	@Override
	@Nonnull
	public Task<CollectionResult<Job, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
	                                                                @QueryParam(PARAM_ASPECTS) @Optional("[]") @Nonnull String[] aspectNames,
	                                                                @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
	                                                                @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
	                                                                @PagingContextParam @Nonnull PagingContext pagingContext) {
		return super.search(input, aspectNames, filter, sortCriterion, pagingContext);
	}

	@Action(name = ACTION_AUTOCOMPLETE)
	@Override
	@Nonnull
	public Task<AutoCompleteResult> autocomplete(@ActionParam(PARAM_QUERY) @Nonnull String query,
	                                             @ActionParam(PARAM_FIELD) @Nullable String field, @ActionParam(PARAM_FILTER) @Nullable Filter filter,
	                                             @ActionParam(PARAM_LIMIT) int limit) {
		return super.autocomplete(query, field, filter, limit);
	}

//	@Action(name = ACTION_BROWSE)
//	@Override
//	@Nonnull
//	public Task<BrowseResult> browse(@ActionParam(PARAM_PATH) @Nonnull String path,
//	                                 @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter, @ActionParam(PARAM_START) int start,
//	                                 @ActionParam(PARAM_LIMIT) int limit) {
//		return super.browse(path, filter, start, limit);
//	}
//
//	@Action(name = ACTION_GET_BROWSE_PATHS)
//	@Override
//	@Nonnull
//	public Task<StringArray> getBrowsePaths(
//			@ActionParam(value = "urn", typeref = com.linkedin.common.Urn.class) @Nonnull Urn urn) {
//		return super.getBrowsePaths(urn);
//	}

	@Action(name = ACTION_INGEST)
	@Override
	@Nonnull
	public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull JobSnapshot snapshot) {
		return super.ingest(snapshot);
	}
	@Action(name = ACTION_GET_SNAPSHOT)
	@Override
	@Nonnull
	public Task<JobSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
	                                         @ActionParam(PARAM_ASPECTS) @Optional("[]") @Nonnull String[] aspectNames) {
		return super.getSnapshot(urnString, aspectNames);
	}
	@Action(name = ACTION_BACKFILL)
	@Override
	@Nonnull
	public Task<String[]> backfill(@ActionParam(PARAM_URN) @Nonnull String urnString,
	                               @ActionParam(PARAM_ASPECTS) @Optional("[]") @Nonnull String[] aspectNames) {
		return super.backfill(urnString, aspectNames);
	}

}

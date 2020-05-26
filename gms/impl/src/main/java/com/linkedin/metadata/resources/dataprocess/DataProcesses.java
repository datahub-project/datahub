package com.linkedin.metadata.resources.dataprocess;

import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.dataprocess.DataProcess;
import com.linkedin.dataprocess.DataProcessInfo;
import com.linkedin.dataprocess.DataProcessKey;
import com.linkedin.metadata.aspect.DataProcessAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.restli.BaseSearchableEntityResource;
import com.linkedin.metadata.search.CorpUserInfoDocument;
import com.linkedin.metadata.snapshot.DataProcessSnapshot;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.annotations.*;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.linkedin.metadata.restli.RestliConstants.*;

@RestLiCollection(name = "dataProcesses", namespace = "com.linkedin.dataprocess", keyName = "dataprocess")
public class DataProcesses extends BaseSearchableEntityResource<
    // @formatter:off
    DataProcessKey,
    DataProcess,
    DataProcessUrn,
    DataProcessSnapshot,
    DataProcessAspect,
    CorpUserInfoDocument> {
    // @formatter:on


	public DataProcesses() {
		super(DataProcessSnapshot.class, DataProcessAspect.class);
	}

	@Inject
	@Named("dataProcessDAO")
	private BaseLocalDAO<DataProcessAspect, DataProcessUrn> _localDAO;

	@Inject
	@Named("dataProcessSearchDAO")
	private BaseSearchDAO _esSearchDAO;


	@Nonnull
	@Override
	protected BaseSearchDAO<CorpUserInfoDocument> getSearchDAO() {
		return _esSearchDAO;
	}

	@Nonnull
	@Override
	protected BaseLocalDAO<DataProcessAspect, DataProcessUrn> getLocalDAO() {
		return _localDAO;
	}

	@Nonnull
	@Override
	protected DataProcessUrn createUrnFromString(@Nonnull String urnString) throws Exception {
		return DataProcessUrn.createFromString(urnString);
	}

	@Nonnull
	@Override
	protected DataProcessUrn toUrn(@Nonnull DataProcessKey key) {
		return new DataProcessUrn(key.getOrchestrator(), key.getName(), key.getOrigin());
	}

	@Nonnull
	@Override
	protected DataProcessKey toKey(@Nonnull DataProcessUrn urn) {
		return new DataProcessKey()
				.setOrchestrator(urn.getDataProcessOrchestrator())
				.setName(urn.getProcessNameEntity())
				.setOrigin(urn.getOriginEntity());
	}

	@Nonnull
	@Override
	protected DataProcess toValue(@Nonnull DataProcessSnapshot processSnapshot) {
		final DataProcess value = new DataProcess()
				.setOrchestrator(processSnapshot.getUrn().getDataProcessOrchestrator())
				.setName(processSnapshot.getUrn().getProcessNameEntity())
				.setOrigin(processSnapshot.getUrn().getOriginEntity())
				.setUrn(processSnapshot.getUrn());
		ModelUtils.getAspectsFromSnapshot(processSnapshot).forEach(aspect -> {
			if (aspect instanceof DataProcessInfo) {
				DataProcessInfo processInfo = DataProcessInfo.class.cast(aspect);
				value.setDataProcessInfo(processInfo);
			}
		});

		return value;
	}

	@Nonnull
	private DataProcessInfo getDataProcessInfoAspect(@Nonnull DataProcess process) {
		final DataProcessInfo processInfo = new DataProcessInfo();
		if (process.getDataProcessInfo().hasInputs()) {
			processInfo.setInputs(process.getDataProcessInfo().getInputs());
		}
		if (process.getDataProcessInfo().hasOutputs()) {
			processInfo.setOutputs(process.getDataProcessInfo().getOutputs());
		}
		return processInfo;
	}

	@Nonnull
	@Override
	protected DataProcessSnapshot toSnapshot(@Nonnull DataProcess process, @Nonnull DataProcessUrn urn) {
		final List<DataProcessAspect> aspects = new ArrayList<>();
		aspects.add(ModelUtils.newAspectUnion(DataProcessAspect.class, getDataProcessInfoAspect(process)));
		return ModelUtils.newSnapshot(DataProcessSnapshot.class, urn, aspects);
	}

	@RestMethod.Get
	@Override
	@Nonnull
	public Task<DataProcess> get(@Nonnull ComplexResourceKey<DataProcessKey, EmptyRecord> key,
								 @QueryParam(PARAM_ASPECTS) @Optional("[]") String[] aspectNames) {
		return super.get(key, aspectNames);
	}

	@RestMethod.BatchGet
	@Override
	@Nonnull
	public Task<Map<ComplexResourceKey<DataProcessKey, EmptyRecord>, DataProcess>> batchGet(
			@Nonnull Set<ComplexResourceKey<DataProcessKey, EmptyRecord>> keys,
			@QueryParam(PARAM_ASPECTS) @Optional("[]") String[] aspectNames) {
		return super.batchGet(keys, aspectNames);
	}

	@Action(name = ACTION_INGEST)
	@Override
	@Nonnull
	public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull DataProcessSnapshot snapshot) {
		return super.ingest(snapshot);
	}
	@Action(name = ACTION_GET_SNAPSHOT)
	@Override
	@Nonnull
	public Task<DataProcessSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
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

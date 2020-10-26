package com.linkedin.metadata.resources.dataprocess;

import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.dataprocess.DataProcess;
import com.linkedin.dataprocess.DataProcessInfo;
import com.linkedin.dataprocess.DataProcessKey;
import com.linkedin.metadata.aspect.DataProcessAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BackfillResult;
import com.linkedin.metadata.restli.BaseSearchableEntityResource;
import com.linkedin.metadata.search.DataProcessDocument;
import com.linkedin.metadata.snapshot.DataProcessSnapshot;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Finder;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.PagingContextParam;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
    ComplexResourceKey<DataProcessKey, EmptyRecord>,
    DataProcess,
    DataProcessUrn,
    DataProcessSnapshot,
    DataProcessAspect,
    DataProcessDocument> {
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
    protected BaseSearchDAO<DataProcessDocument> getSearchDAO() {
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
    protected DataProcessUrn toUrn(@Nonnull ComplexResourceKey<DataProcessKey, EmptyRecord> key) {
        return new DataProcessUrn(key.getKey().getOrchestrator(), key.getKey().getName(), key.getKey().getOrigin());
    }

    @Nonnull
    @Override
    protected ComplexResourceKey<DataProcessKey, EmptyRecord> toKey(@Nonnull DataProcessUrn urn) {
        return new ComplexResourceKey<>(
            new DataProcessKey()
                .setOrchestrator(urn.getOrchestrator())
                .setName(urn.getNameEntity())
                .setOrigin(urn.getOriginEntity()),
            new EmptyRecord());
    }

    @Nonnull
    @Override
    protected DataProcess toValue(@Nonnull DataProcessSnapshot processSnapshot) {
        final DataProcess value = new DataProcess()
                .setOrchestrator(processSnapshot.getUrn().getOrchestrator())
                .setName(processSnapshot.getUrn().getNameEntity())
                .setOrigin(processSnapshot.getUrn().getOriginEntity());
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
                                 @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
        return super.get(key, aspectNames);
    }

    @RestMethod.BatchGet
    @Override
    @Nonnull
    public Task<Map<ComplexResourceKey<DataProcessKey, EmptyRecord>, DataProcess>> batchGet(
            @Nonnull Set<ComplexResourceKey<DataProcessKey, EmptyRecord>> keys,
            @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
        return super.batchGet(keys, aspectNames);
    }

    @RestMethod.GetAll
    @Nonnull
    public Task<List<DataProcess>> getAll(@PagingContextParam @Nonnull PagingContext pagingContext,
        @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
        @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
        @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion) {
        return super.getAll(pagingContext, aspectNames, filter, sortCriterion);
    }

    @Finder(FINDER_SEARCH)
    @Override
    @Nonnull
    public Task<CollectionResult<DataProcess, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
                                                                            @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
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
                                                 @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
        return super.getSnapshot(urnString, aspectNames);
    }

    @Action(name = ACTION_BACKFILL)
    @Override
    @Nonnull
    public Task<BackfillResult> backfill(@ActionParam(PARAM_URN) @Nonnull String urnString,
                                   @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
        return super.backfill(urnString, aspectNames);
    }
}

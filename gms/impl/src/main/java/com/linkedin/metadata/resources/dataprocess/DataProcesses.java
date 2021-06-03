package com.linkedin.metadata.resources.dataprocess;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataprocess.DataProcess;
import com.linkedin.dataprocess.DataProcessInfo;
import com.linkedin.dataprocess.DataProcessResourceKey;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.aspect.DataProcessAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.metadata.restli.BackfillResult;
import com.linkedin.metadata.restli.BaseSearchableEntityResource;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.metadata.search.DataProcessDocument;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.snapshot.DataProcessSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
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

import java.net.URISyntaxException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.linkedin.metadata.restli.RestliConstants.*;

/**
 * Deprecated! Use {@link EntityResource} instead.
 */
@Deprecated
@RestLiCollection(name = "dataProcesses", namespace = "com.linkedin.dataprocess", keyName = "dataprocess")
public class DataProcesses extends BaseSearchableEntityResource<
    // @formatter:off
    ComplexResourceKey<DataProcessResourceKey, EmptyRecord>,
    DataProcess,
    DataProcessUrn,
    DataProcessSnapshot,
    DataProcessAspect,
    DataProcessDocument> {
    // @formatter:on

    private static final String DEFAULT_ACTOR = "urn:li:principal:UNKNOWN";
    private final Clock _clock = Clock.systemUTC();

    public DataProcesses() {
        super(DataProcessSnapshot.class, DataProcessAspect.class);
    }

    @Inject
    @Named("entityService")
    private EntityService _entityService;

    @Inject
    @Named("searchService")
    private SearchService _searchService;

    @Nonnull
    @Override
    protected BaseSearchDAO<DataProcessDocument> getSearchDAO() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    protected BaseLocalDAO<DataProcessAspect, DataProcessUrn> getLocalDAO() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    protected DataProcessUrn createUrnFromString(@Nonnull String urnString) throws Exception {
        return DataProcessUrn.createFromString(urnString);
    }

    @Nonnull
    @Override
    protected DataProcessUrn toUrn(@Nonnull ComplexResourceKey<DataProcessResourceKey, EmptyRecord> key) {
        return new DataProcessUrn(key.getKey().getOrchestrator(), key.getKey().getName(), key.getKey().getOrigin());
    }

    @Nonnull
    @Override
    protected ComplexResourceKey<DataProcessResourceKey, EmptyRecord> toKey(@Nonnull DataProcessUrn urn) {
        return new ComplexResourceKey<>(
            new DataProcessResourceKey()
                .setOrchestrator(urn.getOrchestratorEntity())
                .setName(urn.getNameEntity())
                .setOrigin(urn.getOriginEntity()),
            new EmptyRecord());
    }

    @Nonnull
    @Override
    protected DataProcess toValue(@Nonnull DataProcessSnapshot processSnapshot) {
        final DataProcess value = new DataProcess()
                .setOrchestrator(processSnapshot.getUrn().getOrchestratorEntity())
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
    public Task<DataProcess> get(@Nonnull ComplexResourceKey<DataProcessResourceKey, EmptyRecord> key,
                                 @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
        final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
            Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
                .collect(Collectors.toList()));
        return RestliUtils.toTask(() -> {
            final Entity entity = _entityService.getEntity(new DataProcessUrn(
                key.getKey().getOrchestrator(),
                key.getKey().getName(),
                key.getKey().getOrigin()), projectedAspects);
            if (entity != null) {
                return toValue(entity.getValue().getDataProcessSnapshot());
            }
            throw RestliUtils.resourceNotFoundException();
        });
    }

    @RestMethod.BatchGet
    @Override
    @Nonnull
    public Task<Map<ComplexResourceKey<DataProcessResourceKey, EmptyRecord>, DataProcess>> batchGet(
            @Nonnull Set<ComplexResourceKey<DataProcessResourceKey, EmptyRecord>> keys,
            @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
        final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
            Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
                .collect(Collectors.toList()));
        return RestliUtils.toTask(() -> {

            final Map<ComplexResourceKey<DataProcessResourceKey, EmptyRecord>, DataProcess> entities = new HashMap<>();
            for (final ComplexResourceKey<DataProcessResourceKey, EmptyRecord> key : keys) {
                final Entity entity = _entityService.getEntity(
                    new DataProcessUrn(
                        key.getKey().getOrchestrator(),
                        key.getKey().getName(),
                        key.getKey().getOrigin()),
                    projectedAspects);
                if (entity != null) {
                    entities.put(key, toValue(entity.getValue().getDataProcessSnapshot()));
                }
            }
            return entities;
        });
    }

    @RestMethod.GetAll
    @Nonnull
    public Task<List<DataProcess>> getAll(@PagingContextParam @Nonnull PagingContext pagingContext,
        @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
        @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
        @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion) {
        final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
            Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
                .collect(Collectors.toList()));
        return RestliUtils.toTask(() -> {

            final Filter searchFilter = filter != null ? filter : QueryUtils.EMPTY_FILTER;
            final SortCriterion searchSortCriterion = sortCriterion != null ? sortCriterion
                : new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING);
            final SearchResult filterResults = _searchService.filter(
                "dataProcess",
                searchFilter,
                searchSortCriterion,
                pagingContext.getStart(),
                pagingContext.getCount());

            final Set<Urn> urns = new HashSet<>(filterResults.getEntities());
            final Map<Urn, Entity> entity = _entityService.getEntities(urns, projectedAspects);

            return new CollectionResult<>(
                entity.keySet().stream().map(urn -> toValue(entity.get(urn).getValue().getDataProcessSnapshot())).collect(
                    Collectors.toList()),
                filterResults.getNumEntities(),
                filterResults.getMetadata().setUrns(new UrnArray(urns))
            ).getElements();
        });
    }

    @Finder(FINDER_SEARCH)
    @Override
    @Nonnull
    public Task<CollectionResult<DataProcess, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
                                                                            @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
                                                                            @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
                                                                            @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
                                                                            @PagingContextParam @Nonnull PagingContext pagingContext) {
        final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
            Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
                .collect(Collectors.toList()));
        return RestliUtils.toTask(() -> {

            final SearchResult searchResult = _searchService.search(
                "dataProcess",
                input,
                filter,
                sortCriterion,
                pagingContext.getStart(),
                pagingContext.getCount());

            final Set<Urn> urns = new HashSet<>(searchResult.getEntities());
            final Map<Urn, Entity> entity = _entityService.getEntities(urns, projectedAspects);

            return new CollectionResult<>(
                entity.keySet().stream().map(urn -> toValue(entity.get(urn).getValue().getDataProcessSnapshot())).collect(
                    Collectors.toList()),
                searchResult.getNumEntities(),
                searchResult.getMetadata().setUrns(new UrnArray(urns))
            );
        });
    }

    @Action(name = ACTION_AUTOCOMPLETE)
    @Override
    @Nonnull
    public Task<AutoCompleteResult> autocomplete(@ActionParam(PARAM_QUERY) @Nonnull String query,
        @ActionParam(PARAM_FIELD) @Nullable String field, @ActionParam(PARAM_FILTER) @Nullable Filter filter,
        @ActionParam(PARAM_LIMIT) int limit) {
        return RestliUtils.toTask(() ->
            _searchService.autoComplete(
                "dataProcess",
                query,
                field,
                filter,
                limit)
        );
    }

    @Action(name = ACTION_INGEST)
    @Override
    @Nonnull
    public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull DataProcessSnapshot snapshot) {
        return RestliUtils.toTask(() -> {
            try {
                final AuditStamp auditStamp =
                    new AuditStamp().setTime(_clock.millis()).setActor(Urn.createFromString(DEFAULT_ACTOR));
                _entityService.ingestEntity(new Entity().setValue(Snapshot.create(snapshot)), auditStamp);
            } catch (URISyntaxException e) {
                throw new RuntimeException("Failed to create Audit Urn", e);
            }
            return null;
        });
    }
    @Action(name = ACTION_GET_SNAPSHOT)
    @Override
    @Nonnull
    public Task<DataProcessSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
                                                 @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
        final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
            Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
                .collect(Collectors.toList()));
        return RestliUtils.toTask(() -> {
            try {
                final Entity entity = _entityService.getEntity(
                    Urn.createFromString(urnString), projectedAspects);

                if (entity != null) {
                    return entity.getValue().getDataProcessSnapshot();
                }
                throw RestliUtils.resourceNotFoundException();
            } catch (URISyntaxException e) {
                throw new RuntimeException(String.format("Failed to convert urnString %s into an Urn", urnString));
            }
        });
    }

    @Action(name = ACTION_BACKFILL)
    @Override
    @Nonnull
    public Task<BackfillResult> backfill(@ActionParam(PARAM_URN) @Nonnull String urnString,
                                   @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
        return super.backfill(urnString, aspectNames);
    }
}

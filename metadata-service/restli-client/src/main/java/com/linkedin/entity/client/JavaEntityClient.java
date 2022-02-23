package com.linkedin.entity.client;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;

import com.google.common.collect.ImmutableSet;
import com.linkedin.aspect.GetTimeseriesAspectValuesResponse;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.aspect.EnvelopedAspectArray;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.resources.entity.AspectUtils;
import com.linkedin.metadata.resources.entity.EntityResource;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.r2.RemoteInvocationException;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.search.utils.QueryUtils.*;


@Slf4j
public class JavaEntityClient implements EntityClient {

    private final Clock _clock = Clock.systemUTC();

    private EntityService _entityService;
    private EntitySearchService _entitySearchService;
    private SearchService _searchService;
    private TimeseriesAspectService _timeseriesAspectService;

    public JavaEntityClient(@Nonnull final EntityService entityService, @Nonnull final EntitySearchService entitySearchService, @Nonnull final
        SearchService searchService, @Nonnull final TimeseriesAspectService timeseriesAspectService) {
      _entityService = entityService;
      _entitySearchService = entitySearchService;
      _searchService = searchService;
      _timeseriesAspectService = timeseriesAspectService;
    }

    @Nonnull
    public Entity get(@Nonnull final Urn urn, @Nonnull final Authentication authentication) {
      return _entityService.getEntity(urn, ImmutableSet.of());
    }

    @Nonnull
    public Map<Urn, Entity> batchGet(@Nonnull final Set<Urn> urns, @Nonnull final Authentication authentication) {
      return _entityService.getEntities(urns, ImmutableSet.of());
    }

    /**
     * Gets browse snapshot of a given path
     *
     * @param query search query
     * @param field field of the dataset
     * @param requestFilters autocomplete filters
     * @param limit max number of autocomplete results
     * @throws RemoteInvocationException
     */
    @Nonnull
    public AutoCompleteResult autoComplete(
        @Nonnull String entityType,
        @Nonnull String query,
        @Nonnull Map<String, String> requestFilters,
        @Nonnull int limit,
        @Nullable String field,
        @Nonnull final Authentication authentication) throws RemoteInvocationException {
      return _entitySearchService.autoComplete(entityType, query, field, newFilter(requestFilters), limit);
    }

    /**;
     * Gets autocomplete results
     *
     * @param query search query
     * @param requestFilters autocomplete filters
     * @param limit max number of autocomplete results
     * @throws RemoteInvocationException
     */
    @Nonnull
    public AutoCompleteResult autoComplete(
        @Nonnull String entityType,
        @Nonnull String query,
        @Nonnull Map<String, String> requestFilters,
        @Nonnull int limit,
        @Nonnull final Authentication authentication) throws RemoteInvocationException {
        return _entitySearchService.autoComplete(entityType, query, "", newFilter(requestFilters), limit);
    }

    /**
     * Gets autocomplete results
     *
     * @param entityType entity type being browse
     * @param path path being browsed
     * @param requestFilters browse filters
     * @param start start offset of first dataset
     * @param limit max number of datasets
     * @throws RemoteInvocationException
     */
    @Nonnull
    public BrowseResult browse(
        @Nonnull String entityType,
        @Nonnull String path,
        @Nullable Map<String, String> requestFilters,
        int start,
        int limit,
        @Nonnull final Authentication authentication) throws RemoteInvocationException {
        return _entitySearchService.browse(entityType, path, newFilter(requestFilters), start, limit);
    }

    @SneakyThrows
    public void update(@Nonnull final Entity entity, @Nonnull final Authentication authentication)
        throws RemoteInvocationException {
        Objects.requireNonNull(authentication, "authentication must not be null");
        AuditStamp auditStamp = new AuditStamp();
        auditStamp.setActor(Urn.createFromString(authentication.getActor().toUrnStr()));
        auditStamp.setTime(Clock.systemUTC().millis());
        _entityService.ingestEntity(entity, auditStamp);
    }

    @SneakyThrows
    public void updateWithSystemMetadata(
        @Nonnull final Entity entity,
        @Nullable final SystemMetadata systemMetadata,
        @Nonnull final Authentication authentication) throws RemoteInvocationException {
        if (systemMetadata == null) {
            update(entity, authentication);
            return;
        }

        AuditStamp auditStamp = new AuditStamp();
        auditStamp.setActor(Urn.createFromString(authentication.getActor().toUrnStr()));
        auditStamp.setTime(Clock.systemUTC().millis());

        _entityService.ingestEntity(entity, auditStamp, systemMetadata);
    }

    @SneakyThrows
    public void batchUpdate(@Nonnull final Set<Entity> entities, @Nonnull final Authentication authentication)
        throws RemoteInvocationException {
        AuditStamp auditStamp = new AuditStamp();
        auditStamp.setActor(Urn.createFromString(authentication.getActor().toUrnStr()));
        auditStamp.setTime(Clock.systemUTC().millis());

      _entityService.ingestEntities(entities.stream().collect(Collectors.toList()), auditStamp, ImmutableList.of());
    }

    /**
     * Searches for entities matching to a given query and filters
     *
     * @param input search query
     * @param requestFilters search filters
     * @param start start offset for search results
     * @param count max number of search results requested
     * @return a set of search results
     * @throws RemoteInvocationException
     */
    @Nonnull
    public SearchResult search(
        @Nonnull String entity,
        @Nonnull String input,
        @Nullable Map<String, String> requestFilters,
        int start,
        int count,
        @Nonnull final Authentication authentication)
        throws RemoteInvocationException {
        return _entitySearchService.search(entity, input, newFilter(requestFilters), null, start, count);
    }

    /**
     * Filters for entities matching to a given query and filters
     *
     * @param requestFilters search filters
     * @param start start offset for search results
     * @param count max number of search results requested
     * @return a set of list results
     * @throws RemoteInvocationException
     */
    @Nonnull
    public ListResult list(
        @Nonnull String entity,
        @Nullable Map<String, String> requestFilters,
        int start,
        int count,
        @Nonnull final Authentication authentication)
        throws RemoteInvocationException {
        return EntityResource.toListResult(
            _entitySearchService.filter(entity, newFilter(requestFilters), null, start, count));
    }

    /**
     * Searches for datasets matching to a given query and filters
     *
     * @param input search query
     * @param filter search filters
     * @param start start offset for search results
     * @param count max number of search results requested
     * @return Snapshot key
     * @throws RemoteInvocationException
     */
    @Nonnull
    public SearchResult search(
        @Nonnull String entity,
        @Nonnull String input,
        @Nullable Filter filter,
        int start,
        int count,
        @Nonnull final Authentication authentication)
        throws RemoteInvocationException {
        return _entitySearchService.search(entity, input, filter, null, start, count);
    }

    /**
     * Searches for entities matching to a given query and filters across multiple entity types
     *
     * @param entities entity types to search (if empty, searches all entities)
     * @param input search query
     * @param filter search filters
     * @param start start offset for search results
     * @param count max number of search results requested
     * @return Snapshot key
     * @throws RemoteInvocationException
     */
    @Nonnull
    public SearchResult searchAcrossEntities(
        @Nonnull List<String> entities,
        @Nonnull String input,
        @Nullable Filter filter,
        int start,
        int count,
        @Nonnull final Authentication authentication) throws RemoteInvocationException {
        return _searchService.searchAcrossEntities(entities, input, filter, null, start, count);
    }

    /**
     * Gets browse path(s) given dataset urn
     *
     * @param urn urn for the entity
     * @return list of paths given urn
     * @throws RemoteInvocationException
     */
    @Nonnull
    public StringArray getBrowsePaths(@Nonnull Urn urn, @Nonnull final Authentication authentication) throws RemoteInvocationException {
      return new StringArray(_entitySearchService.getBrowsePaths(urn.getEntityType(), urn));
    }

    public void setWritable(boolean canWrite, @Nonnull final Authentication authentication) throws RemoteInvocationException {
        _entityService.setWritable(canWrite);
    }

    @Nonnull
    public long getTotalEntityCount(@Nonnull String entityName, @Nonnull final Authentication authentication) throws RemoteInvocationException {
        return _searchService.docCount(entityName);
    }

    @Nonnull
    public Map<String, Long> batchGetTotalEntityCount(
        @Nonnull List<String> entityName,
        @Nonnull final Authentication authentication) throws RemoteInvocationException {
        return entityName.stream().collect(Collectors.toMap(Function.identity(), _searchService::docCount));
    }

    /**
     * List all urns existing for a particular Entity type.
     */
    public ListUrnsResult listUrns(@Nonnull final String entityName, final int start, final int count, @Nonnull final Authentication authentication)
        throws RemoteInvocationException {
        return _entityService.listUrns(entityName, start, count);
    }

    /**
     * Hard delete an entity with a particular urn.
     */
    public void deleteEntity(@Nonnull final Urn urn, @Nonnull final Authentication authentication) throws RemoteInvocationException {
        _entityService.deleteUrn(urn);
    }

    @Nonnull
    @Override
    public SearchResult filter(@Nonnull String entity, @Nonnull Filter filter, @Nullable SortCriterion sortCriterion,
        int start, int count, @Nonnull final Authentication authentication) throws RemoteInvocationException {
        return _entitySearchService.filter(entity, filter, sortCriterion, start, count);
    }

    @SneakyThrows
    @Override
    public VersionedAspect getAspect(@Nonnull String urn, @Nonnull String aspect, @Nonnull Long version,
        @Nonnull final Authentication authentication) throws RemoteInvocationException {
        return _entityService.getVersionedAspect(Urn.createFromString(urn), aspect, version);
    }

    @SneakyThrows
    @Override
    public VersionedAspect getAspectOrNull(@Nonnull String urn, @Nonnull String aspect, @Nonnull Long version,
        @Nonnull final Authentication authentication) throws RemoteInvocationException {
        return _entityService.getVersionedAspect(Urn.createFromString(urn), aspect, version);
    }

    @SneakyThrows
    @Override
    public List<EnvelopedAspect> getTimeseriesAspectValues(@Nonnull String urn, @Nonnull String entity,
        @Nonnull String aspect, @Nullable Long startTimeMillis, @Nullable Long endTimeMillis, @Nullable Integer limit,
        @Nonnull final Authentication authentication) throws RemoteInvocationException {
        GetTimeseriesAspectValuesResponse response = new GetTimeseriesAspectValuesResponse();
        response.setEntityName(entity);
        response.setAspectName(aspect);
        if (startTimeMillis != null) {
            response.setStartTimeMillis(startTimeMillis);
        }
        if (endTimeMillis != null) {
            response.setEndTimeMillis(endTimeMillis);
        }
        response.setValues(new EnvelopedAspectArray(
            _timeseriesAspectService.getAspectValues(Urn.createFromString(urn), entity, aspect, startTimeMillis, endTimeMillis,
                limit)));
        return response.getValues();
    }

    // TODO: Factor out ingest logic into a util that can be accessed by the java client and the resource
    @SneakyThrows
    @Override
    public String ingestProposal(@Nonnull MetadataChangeProposal metadataChangeProposal,
        @Nonnull final Authentication authentication) throws RemoteInvocationException {
        final AuditStamp auditStamp =
            new AuditStamp().setTime(_clock.millis()).setActor(Urn.createFromString(Constants.UNKNOWN_ACTOR));
        final List<MetadataChangeProposal> additionalChanges =
            AspectUtils.getAdditionalChanges(metadataChangeProposal, _entityService);

        Urn urn = _entityService.ingestProposal(metadataChangeProposal, auditStamp);
        additionalChanges.forEach(proposal -> _entityService.ingestProposal(proposal, auditStamp));
        return urn.toString();
    }

    @SneakyThrows
    @Override
    public <T extends RecordTemplate> Optional<T> getVersionedAspect(@Nonnull String urn, @Nonnull String aspect,
        @Nonnull Long version, @Nonnull Class<T> aspectClass, @Nonnull final Authentication authentication) throws RemoteInvocationException {
        VersionedAspect entity = _entityService.getVersionedAspect(Urn.createFromString(urn), aspect, version);
        if (entity != null && entity.hasAspect()) {
            DataMap rawAspect = ((DataMap) entity.data().get("aspect"));
            if (rawAspect.containsKey(aspectClass.getCanonicalName())) {
                DataMap aspectDataMap = rawAspect.getDataMap(aspectClass.getCanonicalName());
                return Optional.of(RecordUtils.toRecordTemplate(aspectClass, aspectDataMap));
            }
        }
        return Optional.empty();
    }

    @SneakyThrows
    public DataMap getRawAspect(@Nonnull String urn, @Nonnull String aspect,
        @Nonnull Long version, @Nonnull Authentication authentication) throws RemoteInvocationException {
        VersionedAspect entity = _entityService.getVersionedAspect(Urn.createFromString(urn), aspect, version);
        if (entity == null) {
            return null;
        }

        if (entity.hasAspect()) {
            DataMap rawAspect = ((DataMap) entity.data().get("aspect"));
            return rawAspect;
        }

        return null;
    }
}

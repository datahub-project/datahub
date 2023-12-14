package com.linkedin.metadata.client;

import static com.linkedin.metadata.search.utils.QueryUtils.*;
import static com.linkedin.metadata.search.utils.SearchUtils.*;

import com.datahub.authentication.Authentication;
import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.aspect.GetTimeseriesAspectValuesResponse;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.aspect.EnvelopedAspectArray;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.ebean.transactions.AspectsBatchImpl;
import com.linkedin.metadata.entity.transactions.AspectsBatch;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LineageScrollResult;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.shared.ValidationUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.parseq.retry.backoff.BackoffPolicy;
import com.linkedin.parseq.retry.backoff.ExponentialBackoff;
import com.linkedin.r2.RemoteInvocationException;
import io.opentelemetry.extension.annotations.WithSpan;
import java.net.URISyntaxException;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class JavaEntityClient implements EntityClient {
  private static final int DEFAULT_RETRY_INTERVAL = 2;
  private static final int DEFAULT_RETRY_COUNT = 3;

  private static final Set<String> NON_RETRYABLE =
      Set.of("com.linkedin.data.template.RequiredFieldNotPresentException");

  private final Clock _clock = Clock.systemUTC();

  private final EntityService _entityService;
  private final DeleteEntityService _deleteEntityService;
  private final EntitySearchService _entitySearchService;
  private final CachingEntitySearchService _cachingEntitySearchService;
  private final SearchService _searchService;
  private final LineageSearchService _lineageSearchService;
  private final TimeseriesAspectService _timeseriesAspectService;
  private final EventProducer _eventProducer;
  private final RestliEntityClient _restliEntityClient;

  @Nullable
  public EntityResponse getV2(
      @Nonnull String entityName,
      @Nonnull final Urn urn,
      @Nullable final Set<String> aspectNames,
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException, URISyntaxException {
    final Set<String> projectedAspects =
        aspectNames == null ? _entityService.getEntityAspectNames(entityName) : aspectNames;
    return _entityService.getEntityV2(entityName, urn, projectedAspects);
  }

  @Nonnull
  public Entity get(@Nonnull final Urn urn, @Nonnull final Authentication authentication) {
    return _entityService.getEntity(urn, ImmutableSet.of());
  }

  @Nonnull
  @Override
  public Map<Urn, EntityResponse> batchGetV2(
      @Nonnull String entityName,
      @Nonnull Set<Urn> urns,
      @Nullable Set<String> aspectNames,
      @Nonnull Authentication authentication)
      throws RemoteInvocationException, URISyntaxException {
    final Set<String> projectedAspects =
        aspectNames == null ? _entityService.getEntityAspectNames(entityName) : aspectNames;
    return _entityService.getEntitiesV2(entityName, urns, projectedAspects);
  }

  @Nonnull
  public Map<Urn, EntityResponse> batchGetVersionedV2(
      @Nonnull String entityName,
      @Nonnull final Set<VersionedUrn> versionedUrns,
      @Nullable final Set<String> aspectNames,
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException, URISyntaxException {
    final Set<String> projectedAspects =
        aspectNames == null ? _entityService.getEntityAspectNames(entityName) : aspectNames;
    return _entityService.getEntitiesVersionedV2(versionedUrns, projectedAspects);
  }

  @Nonnull
  public Map<Urn, Entity> batchGet(
      @Nonnull final Set<Urn> urns, @Nonnull final Authentication authentication) {
    return _entityService.getEntities(urns, ImmutableSet.of());
  }

  /**
   * Gets autocomplete results
   *
   * @param entityType the type of entity to autocomplete against
   * @param query search query
   * @param field field of the dataset to autocomplete against
   * @param requestFilters autocomplete filters
   * @param limit max number of autocomplete results
   * @throws RemoteInvocationException
   */
  @Nonnull
  public AutoCompleteResult autoComplete(
      @Nonnull String entityType,
      @Nonnull String query,
      @Nullable Filter requestFilters,
      @Nonnull int limit,
      @Nullable String field,
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    return _cachingEntitySearchService.autoComplete(
        entityType, query, field, filterOrDefaultEmptyFilter(requestFilters), limit, null);
  }

  /**
   * Gets autocomplete results
   *
   * @param entityType the type of entity to autocomplete against
   * @param query search query
   * @param requestFilters autocomplete filters
   * @param limit max number of autocomplete results
   * @throws RemoteInvocationException
   */
  @Nonnull
  public AutoCompleteResult autoComplete(
      @Nonnull String entityType,
      @Nonnull String query,
      @Nullable Filter requestFilters,
      @Nonnull int limit,
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    return _cachingEntitySearchService.autoComplete(
        entityType, query, "", filterOrDefaultEmptyFilter(requestFilters), limit, null);
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
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    return ValidationUtils.validateBrowseResult(
        _cachingEntitySearchService.browse(
            entityType, path, newFilter(requestFilters), start, limit, null),
        _entityService);
  }

  /**
   * Gets browse V2 snapshot of a given path
   *
   * @param entityName entity being browsed
   * @param path path being browsed
   * @param filter browse filter
   * @param input search query
   * @param start start offset of first group
   * @param count max number of results requested
   * @throws RemoteInvocationException
   */
  @Nonnull
  public BrowseResultV2 browseV2(
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input,
      int start,
      int count,
      @Nonnull Authentication authentication) {
    // TODO: cache browseV2 results
    return _entitySearchService.browseV2(entityName, path, filter, input, start, count);
  }

  @SneakyThrows
  @Deprecated
  public void update(@Nonnull final Entity entity, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    Objects.requireNonNull(authentication, "authentication must not be null");
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(Urn.createFromString(authentication.getActor().toUrnStr()));
    auditStamp.setTime(Clock.systemUTC().millis());
    _entityService.ingestEntity(entity, auditStamp);
  }

  @SneakyThrows
  @Deprecated
  public void updateWithSystemMetadata(
      @Nonnull final Entity entity,
      @Nullable final SystemMetadata systemMetadata,
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    if (systemMetadata == null) {
      update(entity, authentication);
      return;
    }

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(Urn.createFromString(authentication.getActor().toUrnStr()));
    auditStamp.setTime(Clock.systemUTC().millis());

    _entityService.ingestEntity(entity, auditStamp, systemMetadata);
    tryIndexRunId(
        com.datahub.util.ModelUtils.getUrnFromSnapshotUnion(entity.getValue()), systemMetadata);
  }

  @SneakyThrows
  @Deprecated
  public void batchUpdate(
      @Nonnull final Set<Entity> entities, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(Urn.createFromString(authentication.getActor().toUrnStr()));
    auditStamp.setTime(Clock.systemUTC().millis());
    _entityService.ingestEntities(
        entities.stream().collect(Collectors.toList()), auditStamp, ImmutableList.of());
  }

  /**
   * Searches for entities matching to a given query and filters
   *
   * @param input search query
   * @param requestFilters search filters
   * @param start start offset for search results
   * @param count max number of search results requested
   * @param searchFlags
   * @return a set of search results
   * @throws RemoteInvocationException
   */
  @Nonnull
  @WithSpan
  @Override
  public SearchResult search(
      @Nonnull String entity,
      @Nonnull String input,
      @Nullable Map<String, String> requestFilters,
      int start,
      int count,
      @Nonnull Authentication authentication,
      @Nullable SearchFlags searchFlags)
      throws RemoteInvocationException {

    return ValidationUtils.validateSearchResult(
        _entitySearchService.search(
            List.of(entity), input, newFilter(requestFilters), null, start, count, searchFlags),
        _entityService);
  }

  /**
   * Deprecated! Use 'filter' or 'search' instead.
   *
   * <p>Filters for entities matching to a given query and filters
   *
   * @param requestFilters search filters
   * @param start start offset for search results
   * @param count max number of search results requested
   * @return a set of list results
   * @throws RemoteInvocationException
   */
  @Deprecated
  @Nonnull
  public ListResult list(
      @Nonnull String entity,
      @Nullable Map<String, String> requestFilters,
      int start,
      int count,
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    return ValidationUtils.validateListResult(
        toListResult(
            _entitySearchService.filter(entity, newFilter(requestFilters), null, start, count)),
        _entityService);
  }

  /**
   * Searches for datasets matching to a given query and filters
   *
   * @param input search query
   * @param filter search filters
   * @param sortCriterion sort criterion
   * @param start start offset for search results
   * @param count max number of search results requested
   * @return Snapshot key
   * @throws RemoteInvocationException
   */
  @Nonnull
  @Override
  public SearchResult search(
      @Nonnull String entity,
      @Nonnull String input,
      @Nullable Filter filter,
      @Nullable SortCriterion sortCriterion,
      int start,
      int count,
      @Nonnull Authentication authentication,
      @Nullable SearchFlags searchFlags)
      throws RemoteInvocationException {
    return ValidationUtils.validateSearchResult(
        _entitySearchService.search(
            List.of(entity), input, filter, sortCriterion, start, count, searchFlags),
        _entityService);
  }

  @Nonnull
  public SearchResult searchAcrossEntities(
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter filter,
      int start,
      int count,
      @Nullable SearchFlags searchFlags,
      @Nullable SortCriterion sortCriterion,
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    return searchAcrossEntities(
        entities, input, filter, start, count, searchFlags, sortCriterion, authentication, null);
  }

  /**
   * Searches for entities matching to a given query and filters across multiple entity types
   *
   * @param entities entity types to search (if empty, searches all entities)
   * @param input search query
   * @param filter search filters
   * @param start start offset for search results
   * @param count max number of search results requested
   * @param facets list of facets we want aggregations for
   * @param sortCriterion sorting criterion
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
      @Nullable SearchFlags searchFlags,
      @Nullable SortCriterion sortCriterion,
      @Nonnull final Authentication authentication,
      @Nullable List<String> facets)
      throws RemoteInvocationException {
    final SearchFlags finalFlags =
        searchFlags != null ? searchFlags : new SearchFlags().setFulltext(true);
    return ValidationUtils.validateSearchResult(
        _searchService.searchAcrossEntities(
            entities, input, filter, sortCriterion, start, count, finalFlags, facets),
        _entityService);
  }

  @Nonnull
  @Override
  public ScrollResult scrollAcrossEntities(
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter filter,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      int count,
      @Nullable SearchFlags searchFlags,
      @Nonnull Authentication authentication)
      throws RemoteInvocationException {
    final SearchFlags finalFlags =
        searchFlags != null ? searchFlags : new SearchFlags().setFulltext(true);
    return ValidationUtils.validateScrollResult(
        _searchService.scrollAcrossEntities(
            entities, input, filter, null, scrollId, keepAlive, count, finalFlags),
        _entityService);
  }

  @Nonnull
  @Override
  public LineageSearchResult searchAcrossLineage(
      @Nonnull Urn sourceUrn,
      @Nonnull LineageDirection direction,
      @Nonnull List<String> entities,
      @Nullable String input,
      @Nullable Integer maxHops,
      @Nullable Filter filter,
      @Nullable SortCriterion sortCriterion,
      int start,
      int count,
      @Nullable SearchFlags searchFlags,
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    return ValidationUtils.validateLineageSearchResult(
        _lineageSearchService.searchAcrossLineage(
            sourceUrn,
            direction,
            entities,
            input,
            maxHops,
            filter,
            sortCriterion,
            start,
            count,
            null,
            null,
            searchFlags),
        _entityService);
  }

  @Nonnull
  @Override
  public LineageSearchResult searchAcrossLineage(
      @Nonnull Urn sourceUrn,
      @Nonnull LineageDirection direction,
      @Nonnull List<String> entities,
      @Nullable String input,
      @Nullable Integer maxHops,
      @Nullable Filter filter,
      @Nullable SortCriterion sortCriterion,
      int start,
      int count,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis,
      @Nullable SearchFlags searchFlags,
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    return ValidationUtils.validateLineageSearchResult(
        _lineageSearchService.searchAcrossLineage(
            sourceUrn,
            direction,
            entities,
            input,
            maxHops,
            filter,
            sortCriterion,
            start,
            count,
            startTimeMillis,
            endTimeMillis,
            searchFlags),
        _entityService);
  }

  @Nonnull
  @Override
  public LineageScrollResult scrollAcrossLineage(
      @Nonnull Urn sourceUrn,
      @Nonnull LineageDirection direction,
      @Nonnull List<String> entities,
      @Nullable String input,
      @Nullable Integer maxHops,
      @Nullable Filter filter,
      @Nullable SortCriterion sortCriterion,
      @Nullable String scrollId,
      @Nonnull String keepAlive,
      int count,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis,
      @Nullable SearchFlags searchFlags,
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    final SearchFlags finalFlags =
        searchFlags != null ? searchFlags : new SearchFlags().setFulltext(true).setSkipCache(true);
    return ValidationUtils.validateLineageScrollResult(
        _lineageSearchService.scrollAcrossLineage(
            sourceUrn,
            direction,
            entities,
            input,
            maxHops,
            filter,
            sortCriterion,
            scrollId,
            keepAlive,
            count,
            startTimeMillis,
            endTimeMillis,
            finalFlags),
        _entityService);
  }

  /**
   * Gets browse path(s) given dataset urn
   *
   * @param urn urn for the entity
   * @return list of paths given urn
   * @throws RemoteInvocationException
   */
  @Nonnull
  public StringArray getBrowsePaths(@Nonnull Urn urn, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    return new StringArray(_entitySearchService.getBrowsePaths(urn.getEntityType(), urn));
  }

  public void setWritable(boolean canWrite, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    _entityService.setWritable(canWrite);
  }

  @Nonnull
  public Map<String, Long> batchGetTotalEntityCount(
      @Nonnull List<String> entityNames, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    return _searchService.docCountPerEntity(entityNames);
  }

  /** List all urns existing for a particular Entity type. */
  public ListUrnsResult listUrns(
      @Nonnull final String entityName,
      final int start,
      final int count,
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    return _entityService.listUrns(entityName, start, count);
  }

  /** Hard delete an entity with a particular urn. */
  public void deleteEntity(@Nonnull final Urn urn, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    _entityService.deleteUrn(urn);
  }

  @Override
  public void deleteEntityReferences(@Nonnull Urn urn, @Nonnull Authentication authentication)
      throws RemoteInvocationException {
    withRetry(() -> _deleteEntityService.deleteReferencesTo(urn, false), "deleteEntityReferences");
  }

  @Nonnull
  @Override
  public SearchResult filter(
      @Nonnull String entity,
      @Nonnull Filter filter,
      @Nullable SortCriterion sortCriterion,
      int start,
      int count,
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    return ValidationUtils.validateSearchResult(
        _entitySearchService.filter(entity, filter, sortCriterion, start, count), _entityService);
  }

  @Override
  public boolean exists(@Nonnull Urn urn, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    return _entityService.exists(urn);
  }

  @SneakyThrows
  @Override
  public VersionedAspect getAspect(
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version,
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    return _entityService.getVersionedAspect(Urn.createFromString(urn), aspect, version);
  }

  @SneakyThrows
  @Override
  public VersionedAspect getAspectOrNull(
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version,
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    return _entityService.getVersionedAspect(Urn.createFromString(urn), aspect, version);
  }

  @SneakyThrows
  @Override
  public List<EnvelopedAspect> getTimeseriesAspectValues(
      @Nonnull String urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis,
      @Nullable Integer limit,
      @Nullable Filter filter,
      @Nullable SortCriterion sort,
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    GetTimeseriesAspectValuesResponse response = new GetTimeseriesAspectValuesResponse();
    response.setEntityName(entity);
    response.setAspectName(aspect);
    if (startTimeMillis != null) {
      response.setStartTimeMillis(startTimeMillis);
    }
    if (endTimeMillis != null) {
      response.setEndTimeMillis(endTimeMillis);
    }
    if (limit != null) {
      response.setLimit(limit);
    }
    if (filter != null) {
      response.setFilter(filter);
    }
    response.setValues(
        new EnvelopedAspectArray(
            _timeseriesAspectService.getAspectValues(
                Urn.createFromString(urn),
                entity,
                aspect,
                startTimeMillis,
                endTimeMillis,
                limit,
                filter,
                sort)));
    return response.getValues();
  }

  // TODO: Factor out ingest logic into a util that can be accessed by the java client and the
  // resource
  @Override
  public String ingestProposal(
      @Nonnull final MetadataChangeProposal metadataChangeProposal,
      @Nonnull final Authentication authentication,
      final boolean async)
      throws RemoteInvocationException {
    String actorUrnStr =
        authentication.getActor() != null
            ? authentication.getActor().toUrnStr()
            : Constants.UNKNOWN_ACTOR;
    final AuditStamp auditStamp =
        new AuditStamp().setTime(_clock.millis()).setActor(UrnUtils.getUrn(actorUrnStr));
    final List<MetadataChangeProposal> additionalChanges =
        AspectUtils.getAdditionalChanges(metadataChangeProposal, _entityService);

    Stream<MetadataChangeProposal> proposalStream =
        Stream.concat(Stream.of(metadataChangeProposal), additionalChanges.stream());
    AspectsBatch batch =
        AspectsBatchImpl.builder()
            .mcps(proposalStream.collect(Collectors.toList()), _entityService.getEntityRegistry())
            .build();

    IngestResult one =
        _entityService.ingestProposal(batch, auditStamp, async).stream().findFirst().get();

    Urn urn = one.getUrn();
    tryIndexRunId(urn, metadataChangeProposal.getSystemMetadata());
    return urn.toString();
  }

  @SneakyThrows
  @Override
  public <T extends RecordTemplate> Optional<T> getVersionedAspect(
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version,
      @Nonnull Class<T> aspectClass,
      @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    VersionedAspect entity =
        _entityService.getVersionedAspect(Urn.createFromString(urn), aspect, version);
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
  public DataMap getRawAspect(
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version,
      @Nonnull Authentication authentication)
      throws RemoteInvocationException {
    VersionedAspect entity =
        _entityService.getVersionedAspect(Urn.createFromString(urn), aspect, version);
    if (entity == null) {
      return null;
    }

    if (entity.hasAspect()) {
      DataMap rawAspect = ((DataMap) entity.data().get("aspect"));
      return rawAspect;
    }

    return null;
  }

  @Override
  public void producePlatformEvent(
      @Nonnull String name,
      @Nullable String key,
      @Nonnull PlatformEvent event,
      @Nonnull Authentication authentication)
      throws Exception {
    _eventProducer.producePlatformEvent(name, key, event);
  }

  @Override
  public void rollbackIngestion(@Nonnull String runId, @Nonnull Authentication authentication)
      throws Exception {
    _restliEntityClient.rollbackIngestion(runId, authentication);
  }

  private void tryIndexRunId(Urn entityUrn, @Nullable SystemMetadata systemMetadata) {
    if (systemMetadata != null && systemMetadata.hasRunId()) {
      _entitySearchService.appendRunId(
          entityUrn.getEntityType(), entityUrn, systemMetadata.getRunId());
    }
  }

  protected <T> T withRetry(@Nonnull final Supplier<T> block, @Nullable String counterPrefix) {
    final BackoffPolicy backoffPolicy = new ExponentialBackoff(DEFAULT_RETRY_INTERVAL);
    int attemptCount = 0;

    while (attemptCount < DEFAULT_RETRY_COUNT + 1) {
      try {
        return block.get();
      } catch (Throwable ex) {
        MetricUtils.counter(this.getClass(), buildMetricName(ex, counterPrefix)).inc();

        final boolean skipRetry =
            NON_RETRYABLE.contains(ex.getClass().getCanonicalName())
                || (ex.getCause() != null
                    && NON_RETRYABLE.contains(ex.getCause().getClass().getCanonicalName()));

        if (attemptCount == DEFAULT_RETRY_COUNT || skipRetry) {
          throw ex;
        } else {
          attemptCount = attemptCount + 1;
          try {
            Thread.sleep(backoffPolicy.nextBackoff(attemptCount, ex) * 1000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    // Should never hit this line.
    throw new IllegalStateException("No JavaEntityClient call executed.");
  }

  private String buildMetricName(Throwable throwable, @Nullable String counterPrefix) {
    StringBuilder builder = new StringBuilder();

    // deleteEntityReferences_failures
    if (counterPrefix != null) {
      builder.append(counterPrefix).append(MetricUtils.DELIMITER);
    }

    return builder
        .append("exception")
        .append(MetricUtils.DELIMITER)
        .append(throwable.getClass().getName())
        .toString();
  }
}

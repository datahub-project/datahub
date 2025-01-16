package com.linkedin.metadata.client;

import static com.linkedin.metadata.search.utils.QueryUtils.*;
import static com.linkedin.metadata.search.utils.SearchUtils.*;

import com.datahub.plugins.auth.authorization.Authorizer;
import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.linkedin.aspect.GetTimeseriesAspectValuesResponse;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.aspect.EnvelopedAspectArray;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.validation.ValidationUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.query.ListUrnsResult;
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
import com.linkedin.metadata.service.RollbackService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.parseq.retry.backoff.BackoffPolicy;
import com.linkedin.parseq.retry.backoff.ExponentialBackoff;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.extension.annotations.WithSpan;
import java.net.URISyntaxException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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
  private final EntityService<?> entityService;
  private final DeleteEntityService deleteEntityService;
  private final EntitySearchService entitySearchService;
  private final CachingEntitySearchService cachingEntitySearchService;
  private final SearchService searchService;
  private final LineageSearchService lineageSearchService;
  private final TimeseriesAspectService timeseriesAspectService;
  private final RollbackService rollbackService;
  private final EventProducer eventProducer;
  private final EntityClientConfig entityClientConfig;

  @Override
  @Nullable
  public EntityResponse getV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull final Urn urn,
      @Nullable final Set<String> aspectNames,
      @Nullable Boolean alwaysIncludeKeyAspect)
      throws RemoteInvocationException, URISyntaxException {
    final Set<String> projectedAspects =
        aspectNames == null ? opContext.getEntityAspectNames(entityName) : aspectNames;
    return entityService.getEntityV2(
        opContext,
        entityName,
        urn,
        projectedAspects,
        alwaysIncludeKeyAspect == null || alwaysIncludeKeyAspect);
  }

  @Override
  @Nonnull
  @Deprecated
  public Entity get(@Nonnull OperationContext opContext, @Nonnull final Urn urn) {
    return entityService.getEntity(opContext, urn, ImmutableSet.of(), true);
  }

  @Nonnull
  @Override
  public Map<Urn, EntityResponse> batchGetV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull Set<Urn> urns,
      @Nullable Set<String> aspectNames,
      @Nullable Boolean alwaysIncludeKeyAspect)
      throws RemoteInvocationException, URISyntaxException {
    final Set<String> projectedAspects =
        aspectNames == null ? opContext.getEntityAspectNames(entityName) : aspectNames;

    Map<Urn, EntityResponse> responseMap = new HashMap<>();

    Iterators.partition(urns.iterator(), Math.max(1, entityClientConfig.getBatchGetV2Size()))
        .forEachRemaining(
            batch -> {
              try {
                responseMap.putAll(
                    entityService.getEntitiesV2(
                        opContext,
                        entityName,
                        new HashSet<>(batch),
                        projectedAspects,
                        alwaysIncludeKeyAspect == null || alwaysIncludeKeyAspect));
              } catch (URISyntaxException e) {
                throw new RuntimeException(e);
              }
            });

    return responseMap;
  }

  @Override
  @Nonnull
  public Map<Urn, EntityResponse> batchGetVersionedV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull final Set<VersionedUrn> versionedUrns,
      @Nullable final Set<String> aspectNames) {
    final Set<String> projectedAspects =
        aspectNames == null ? opContext.getEntityAspectNames(entityName) : aspectNames;

    Map<Urn, EntityResponse> responseMap = new HashMap<>();

    Iterators.partition(
            versionedUrns.iterator(), Math.max(1, entityClientConfig.getBatchGetV2Size()))
        .forEachRemaining(
            batch -> {
              try {
                responseMap.putAll(
                    entityService.getEntitiesVersionedV2(
                        opContext, new HashSet<>(batch), projectedAspects));
              } catch (URISyntaxException e) {
                throw new RuntimeException(e);
              }
            });

    return responseMap;
  }

  @Override
  @Nonnull
  @Deprecated
  public Map<Urn, Entity> batchGet(
      @Nonnull OperationContext opContext, @Nonnull final Set<Urn> urns) {
    return entityService.getEntities(opContext, urns, ImmutableSet.of(), true);
  }

  /**
   * Gets autocomplete results
   *
   * @param entityType the type of entity to autocomplete against
   * @param query search query
   * @param field field of the dataset to autocomplete against
   * @param requestFilters autocomplete filters
   * @param limit max number of autocomplete results
   * @throws RemoteInvocationException when unable to execute request
   */
  @Override
  @Nonnull
  public AutoCompleteResult autoComplete(
      @Nonnull OperationContext opContext,
      @Nonnull String entityType,
      @Nonnull String query,
      @Nullable Filter requestFilters,
      int limit,
      @Nullable String field)
      throws RemoteInvocationException {
    return cachingEntitySearchService.autoComplete(
        opContext, entityType, query, field, filterOrDefaultEmptyFilter(requestFilters), limit);
  }

  /**
   * Gets autocomplete results
   *
   * @param entityType the type of entity to autocomplete against
   * @param query search query
   * @param requestFilters autocomplete filters
   * @param limit max number of autocomplete results
   * @throws RemoteInvocationException when unable to execute request
   */
  @Override
  @Nonnull
  public AutoCompleteResult autoComplete(
      @Nonnull OperationContext opContext,
      @Nonnull String entityType,
      @Nonnull String query,
      @Nullable Filter requestFilters,
      int limit)
      throws RemoteInvocationException {
    return cachingEntitySearchService.autoComplete(
        opContext, entityType, query, "", filterOrDefaultEmptyFilter(requestFilters), limit);
  }

  /**
   * Gets autocomplete results
   *
   * @param entityType entity type being browse
   * @param path path being browsed
   * @param requestFilters browse filters
   * @param start start offset of first dataset
   * @param limit max number of datasets
   * @throws RemoteInvocationException when unable to execute request
   */
  @Override
  public BrowseResult browse(
      @Nonnull OperationContext opContext,
      @Nonnull String entityType,
      @Nonnull String path,
      @Nullable Map<String, String> requestFilters,
      int start,
      int limit)
      throws RemoteInvocationException {
    return ValidationUtils.validateBrowseResult(
        opContext,
        cachingEntitySearchService.browse(
            opContext, entityType, path, newFilter(requestFilters), start, limit),
        entityService);
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
   */
  @Override
  @Nonnull
  public BrowseResultV2 browseV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input,
      int start,
      int count) {
    // TODO: cache browseV2 results
    return entitySearchService.browseV2(opContext, entityName, path, filter, input, start, count);
  }

  /**
   * Gets browse V2 snapshot of a given path
   *
   * @param entityNames entities being browsed
   * @param path path being browsed
   * @param filter browse filter
   * @param input search query
   * @param start start offset of first group
   * @param count max number of results requested
   */
  @Override
  @Nonnull
  public BrowseResultV2 browseV2(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input,
      int start,
      int count) {
    // TODO: cache browseV2 results
    return entitySearchService.browseV2(opContext, entityNames, path, filter, input, start, count);
  }

  @Override
  @SneakyThrows
  @Deprecated
  public void update(@Nonnull OperationContext opContext, @Nonnull final Entity entity)
      throws RemoteInvocationException {
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(
        Urn.createFromString(opContext.getSessionAuthentication().getActor().toUrnStr()));
    auditStamp.setTime(Clock.systemUTC().millis());
    entityService.ingestEntity(opContext, entity, auditStamp);
  }

  @Override
  @SneakyThrows
  @Deprecated
  public void updateWithSystemMetadata(
      @Nonnull OperationContext opContext,
      @Nonnull final Entity entity,
      @Nullable final SystemMetadata systemMetadata)
      throws RemoteInvocationException {
    if (systemMetadata == null) {
      update(opContext, entity);
      return;
    }

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(
        Urn.createFromString(opContext.getSessionAuthentication().getActor().toUrnStr()));
    auditStamp.setTime(Clock.systemUTC().millis());

    entityService.ingestEntity(opContext, entity, auditStamp, systemMetadata);
    tryIndexRunId(
        opContext,
        com.datahub.util.ModelUtils.getUrnFromSnapshotUnion(entity.getValue()),
        systemMetadata);
  }

  @SneakyThrows
  @Deprecated
  public void batchUpdate(@Nonnull OperationContext opContext, @Nonnull final Set<Entity> entities)
      throws RemoteInvocationException {
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(
        Urn.createFromString(opContext.getSessionAuthentication().getActor().toUrnStr()));
    auditStamp.setTime(Clock.systemUTC().millis());
    entityService.ingestEntities(
        opContext, new ArrayList<>(entities), auditStamp, ImmutableList.of());
  }

  /**
   * Searches for entities matching to a given query and filters
   *
   * @param input search query
   * @param requestFilters search filters
   * @param start start offset for search results
   * @param count max number of search results requested
   * @return a set of search results
   * @throws RemoteInvocationException when unable to execute request
   */
  @WithSpan
  @Override
  public SearchResult search(
      @Nonnull OperationContext opContext,
      @Nonnull String entity,
      @Nonnull String input,
      @Nullable Map<String, String> requestFilters,
      int start,
      int count)
      throws RemoteInvocationException {

    return ValidationUtils.validateSearchResult(
        opContext,
        entitySearchService.search(
            opContext,
            List.of(entity),
            input,
            newFilter(requestFilters),
            Collections.emptyList(),
            start,
            count),
        entityService);
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
   * @throws RemoteInvocationException when unable to execute request
   */
  @Override
  @Deprecated
  public ListResult list(
      @Nonnull OperationContext opContext,
      @Nonnull String entity,
      @Nullable Map<String, String> requestFilters,
      int start,
      int count)
      throws RemoteInvocationException {
    return ValidationUtils.validateListResult(
        opContext,
        toListResult(
            entitySearchService.filter(
                opContext.withSearchFlags(flags -> flags.setFulltext(false)),
                entity,
                newFilter(requestFilters),
                Collections.emptyList(),
                start,
                count)),
        entityService);
  }

  /**
   * Searches for datasets matching to a given query and filters
   *
   * @param input search query
   * @param filter search filters
   * @param sortCriteria sort criteria
   * @param start start offset for search results
   * @param count max number of search results requested
   * @return Snapshot key
   * @throws RemoteInvocationException when unable to execute request
   */
  @Override
  public SearchResult search(
      @Nonnull OperationContext opContext,
      @Nonnull String entity,
      @Nonnull String input,
      @Nullable Filter filter,
      List<SortCriterion> sortCriteria,
      int start,
      int count)
      throws RemoteInvocationException {
    return ValidationUtils.validateSearchResult(
        opContext,
        entitySearchService.search(
            opContext, List.of(entity), input, filter, sortCriteria, start, count),
        entityService);
  }

  @Override
  @Nonnull
  public SearchResult searchAcrossEntities(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter filter,
      int start,
      int count,
      List<SortCriterion> sortCriteria)
      throws RemoteInvocationException {
    return searchAcrossEntities(
        opContext, entities, input, filter, start, count, sortCriteria, null);
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
   * @param sortCriteria sorting criteria
   * @return Snapshot key
   * @throws RemoteInvocationException when unable to execute request
   */
  @Override
  public SearchResult searchAcrossEntities(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter filter,
      int start,
      int count,
      List<SortCriterion> sortCriteria,
      @Nullable List<String> facets)
      throws RemoteInvocationException {

    return ValidationUtils.validateSearchResult(
        opContext,
        searchService.searchAcrossEntities(
            opContext.withSearchFlags(flags -> flags.setFulltext(true)),
            entities,
            input,
            filter,
            sortCriteria,
            start,
            count,
            facets),
        entityService);
  }

  @Nonnull
  @Override
  public ScrollResult scrollAcrossEntities(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter filter,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      int count)
      throws RemoteInvocationException {

    return ValidationUtils.validateScrollResult(
        opContext,
        searchService.scrollAcrossEntities(
            opContext.withSearchFlags(flags -> flags.setFulltext(true)),
            entities,
            input,
            filter,
            null,
            scrollId,
            keepAlive,
            count),
        entityService);
  }

  @Override
  public LineageSearchResult searchAcrossLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn sourceUrn,
      @Nonnull LineageDirection direction,
      @Nonnull List<String> entities,
      @Nullable String input,
      @Nullable Integer maxHops,
      @Nullable Filter filter,
      List<SortCriterion> sortCriteria,
      int start,
      int count)
      throws RemoteInvocationException {
    return ValidationUtils.validateLineageSearchResult(
        opContext,
        lineageSearchService.searchAcrossLineage(
            opContext,
            sourceUrn,
            direction,
            entities,
            input,
            maxHops,
            filter,
            sortCriteria,
            start,
            count),
        entityService);
  }

  @Nonnull
  @Override
  public LineageScrollResult scrollAcrossLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn sourceUrn,
      @Nonnull LineageDirection direction,
      @Nonnull List<String> entities,
      @Nullable String input,
      @Nullable Integer maxHops,
      @Nullable Filter filter,
      List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nonnull String keepAlive,
      int count)
      throws RemoteInvocationException {

    return ValidationUtils.validateLineageScrollResult(
        opContext,
        lineageSearchService.scrollAcrossLineage(
            opContext
                .withSearchFlags(flags -> flags.setFulltext(true).setSkipCache(true))
                .withLineageFlags(flags -> flags),
            sourceUrn,
            direction,
            entities,
            input,
            maxHops,
            filter,
            sortCriteria,
            scrollId,
            keepAlive,
            count),
        entityService);
  }

  /**
   * Gets browse path(s) given dataset urn
   *
   * @param urn urn for the entity
   * @return list of paths given urn
   * @throws RemoteInvocationException when unable to execute request
   */
  @Override
  @Nonnull
  public StringArray getBrowsePaths(@Nonnull OperationContext opContext, @Nonnull Urn urn)
      throws RemoteInvocationException {
    return new StringArray(entitySearchService.getBrowsePaths(opContext, urn.getEntityType(), urn));
  }

  @Override
  public void setWritable(@Nonnull OperationContext opContext, boolean canWrite)
      throws RemoteInvocationException {
    entityService.setWritable(canWrite);
  }

  @Override
  @Nonnull
  public Map<String, Long> batchGetTotalEntityCount(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nullable Filter filter)
      throws RemoteInvocationException {
    return searchService.docCountPerEntity(opContext, entityNames, filter);
  }

  /** List all urns existing for a particular Entity type. */
  @Override
  public ListUrnsResult listUrns(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityName,
      final int start,
      final int count)
      throws RemoteInvocationException {
    return entityService.listUrns(opContext, entityName, start, count);
  }

  /** Hard delete an entity with a particular urn. */
  @Override
  public void deleteEntity(@Nonnull OperationContext opContext, @Nonnull final Urn urn)
      throws RemoteInvocationException {
    entityService.deleteUrn(opContext, urn);
  }

  @Override
  public void deleteEntityReferences(@Nonnull OperationContext opContext, @Nonnull Urn urn)
      throws RemoteInvocationException {
    withRetry(
        () -> deleteEntityService.deleteReferencesTo(opContext, urn, false),
        "deleteEntityReferences");
  }

  @Override
  public SearchResult filter(
      @Nonnull OperationContext opContext,
      @Nonnull String entity,
      @Nonnull Filter filter,
      List<SortCriterion> sortCriteria,
      int start,
      int count)
      throws RemoteInvocationException {
    return ValidationUtils.validateSearchResult(
        opContext,
        entitySearchService.filter(
            opContext.withSearchFlags(flags -> flags.setFulltext(true)),
            entity,
            filter,
            sortCriteria,
            start,
            count),
        entityService);
  }

  @Override
  public boolean exists(@Nonnull OperationContext opContext, @Nonnull Urn urn)
      throws RemoteInvocationException {
    return entityService.exists(opContext, urn, true);
  }

  @Override
  public boolean exists(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull Boolean includeSoftDelete)
      throws RemoteInvocationException {
    return entityService.exists(opContext, urn, includeSoftDelete);
  }

  @SneakyThrows
  @Override
  @Deprecated
  public VersionedAspect getAspect(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version)
      throws RemoteInvocationException {
    return entityService.getVersionedAspect(opContext, Urn.createFromString(urn), aspect, version);
  }

  @SneakyThrows
  @Override
  @Deprecated
  public VersionedAspect getAspectOrNull(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version)
      throws RemoteInvocationException {
    return entityService.getVersionedAspect(opContext, Urn.createFromString(urn), aspect, version);
  }

  @SneakyThrows
  @Override
  public List<EnvelopedAspect> getTimeseriesAspectValues(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis,
      @Nullable Integer limit,
      @Nullable Filter filter,
      @Nullable SortCriterion sort)
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
            timeseriesAspectService.getAspectValues(
                opContext,
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

  @Override
  @Nonnull
  public List<String> batchIngestProposals(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<MetadataChangeProposal> metadataChangeProposals,
      boolean async) {
    String actorUrnStr =
        opContext.getSessionAuthentication().getActor() != null
            ? opContext.getSessionAuthentication().getActor().toUrnStr()
            : Constants.UNKNOWN_ACTOR;
    final AuditStamp auditStamp = AuditStampUtils.createAuditStamp(actorUrnStr);

    List<String> updatedUrns = new ArrayList<>();
    Iterators.partition(
            metadataChangeProposals.iterator(),
            Math.max(1, entityClientConfig.getBatchIngestSize()))
        .forEachRemaining(
            batch -> {
              AspectsBatch aspectsBatch =
                  AspectsBatchImpl.builder()
                      .mcps(
                          batch,
                          auditStamp,
                          opContext.getRetrieverContext(),
                          opContext.getValidationContext().isAlternateValidation())
                      .build();

              List<IngestResult> results =
                  entityService.ingestProposal(opContext, aspectsBatch, async);
              entitySearchService.appendRunId(opContext, results);

              Map<Pair<Urn, String>, List<IngestResult>> resultMap =
                  results.stream()
                      .collect(
                          Collectors.groupingBy(
                              result ->
                                  Pair.of(
                                      result.getRequest().getUrn(),
                                      result.getRequest().getAspectName())));

              // Preserve ordering
              updatedUrns.addAll(
                  aspectsBatch.getItems().stream()
                      .map(
                          requestItem -> {
                            // Urns generated
                            List<Urn> urnsForRequest =
                                resultMap
                                    .getOrDefault(
                                        Pair.of(requestItem.getUrn(), requestItem.getAspectName()),
                                        List.of())
                                    .stream()
                                    .map(IngestResult::getUrn)
                                    .filter(Objects::nonNull)
                                    .distinct()
                                    .collect(Collectors.toList());

                            // Update runIds
                            urnsForRequest.forEach(
                                urn ->
                                    tryIndexRunId(opContext, urn, requestItem.getSystemMetadata()));

                            return urnsForRequest.isEmpty()
                                ? null
                                : urnsForRequest.get(0).toString();
                          })
                      .collect(Collectors.toList()));
            });
    return updatedUrns;
  }

  @SneakyThrows
  @Override
  @Deprecated
  public <T extends RecordTemplate> Optional<T> getVersionedAspect(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version,
      @Nonnull Class<T> aspectClass)
      throws RemoteInvocationException {
    VersionedAspect entity =
        entityService.getVersionedAspect(opContext, Urn.createFromString(urn), aspect, version);
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
  @Deprecated
  public DataMap getRawAspect(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version)
      throws RemoteInvocationException {
    VersionedAspect entity =
        entityService.getVersionedAspect(opContext, Urn.createFromString(urn), aspect, version);
    if (entity == null) {
      return null;
    }

    if (entity.hasAspect()) {
      return ((DataMap) entity.data().get("aspect"));
    }

    return null;
  }

  @Override
  public void producePlatformEvent(
      @Nonnull OperationContext opContext,
      @Nonnull String name,
      @Nullable String key,
      @Nonnull PlatformEvent event)
      throws Exception {
    eventProducer.producePlatformEvent(name, key, event);
  }

  @Override
  public void rollbackIngestion(
      @Nonnull OperationContext opContext, @Nonnull String runId, @Nonnull Authorizer authorizer)
      throws Exception {
    rollbackService.rollbackIngestion(opContext, runId, false, true, authorizer);
  }

  private void tryIndexRunId(
      @Nonnull OperationContext opContext, Urn entityUrn, @Nullable SystemMetadata systemMetadata) {
    if (systemMetadata != null && systemMetadata.hasRunId()) {
      entitySearchService.appendRunId(opContext, entityUrn, systemMetadata.getRunId());
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

package com.linkedin.entity.client;

import static com.linkedin.metadata.Constants.RESTLI_SUCCESS;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.datahub.plugins.auth.authorization.Authorizer;
import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.client.BaseClient;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.AspectsDoGetTimeseriesAspectValuesRequestBuilder;
import com.linkedin.entity.AspectsDoIngestProposalBatchRequestBuilder;
import com.linkedin.entity.AspectsGetRequestBuilder;
import com.linkedin.entity.AspectsRequestBuilders;
import com.linkedin.entity.EntitiesBatchGetRequestBuilder;
import com.linkedin.entity.EntitiesDoAutocompleteRequestBuilder;
import com.linkedin.entity.EntitiesDoBatchGetTotalEntityCountRequestBuilder;
import com.linkedin.entity.EntitiesDoBatchIngestRequestBuilder;
import com.linkedin.entity.EntitiesDoBrowseRequestBuilder;
import com.linkedin.entity.EntitiesDoDeleteReferencesRequestBuilder;
import com.linkedin.entity.EntitiesDoDeleteRequestBuilder;
import com.linkedin.entity.EntitiesDoExistsRequestBuilder;
import com.linkedin.entity.EntitiesDoFilterRequestBuilder;
import com.linkedin.entity.EntitiesDoGetBrowsePathsRequestBuilder;
import com.linkedin.entity.EntitiesDoIngestRequestBuilder;
import com.linkedin.entity.EntitiesDoListRequestBuilder;
import com.linkedin.entity.EntitiesDoListUrnsRequestBuilder;
import com.linkedin.entity.EntitiesDoScrollAcrossEntitiesRequestBuilder;
import com.linkedin.entity.EntitiesDoScrollAcrossLineageRequestBuilder;
import com.linkedin.entity.EntitiesDoSearchAcrossEntitiesRequestBuilder;
import com.linkedin.entity.EntitiesDoSearchAcrossLineageRequestBuilder;
import com.linkedin.entity.EntitiesDoSearchRequestBuilder;
import com.linkedin.entity.EntitiesDoSetWritableRequestBuilder;
import com.linkedin.entity.EntitiesRequestBuilders;
import com.linkedin.entity.EntitiesV2BatchGetRequestBuilder;
import com.linkedin.entity.EntitiesV2GetRequestBuilder;
import com.linkedin.entity.EntitiesV2RequestBuilders;
import com.linkedin.entity.EntitiesVersionedV2BatchGetRequestBuilder;
import com.linkedin.entity.EntitiesVersionedV2RequestBuilders;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityArray;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.RunsDoRollbackRequestBuilder;
import com.linkedin.entity.RunsRequestBuilders;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortCriterionArray;
import com.linkedin.metadata.search.LineageScrollResult;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.MetadataChangeProposalArray;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.platform.PlatformDoProducePlatformEventRequestBuilder;
import com.linkedin.platform.PlatformRequestBuilders;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.common.HttpStatus;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.http.MethodNotSupportedException;
import org.opensearch.core.common.util.CollectionUtils;

@Slf4j
public class RestliEntityClient extends BaseClient implements EntityClient {

  private static final EntitiesRequestBuilders ENTITIES_REQUEST_BUILDERS =
      new EntitiesRequestBuilders();
  private static final EntitiesV2RequestBuilders ENTITIES_V2_REQUEST_BUILDERS =
      new EntitiesV2RequestBuilders();
  private static final EntitiesVersionedV2RequestBuilders ENTITIES_VERSIONED_V2_REQUEST_BUILDERS =
      new EntitiesVersionedV2RequestBuilders();
  private static final AspectsRequestBuilders ASPECTS_REQUEST_BUILDERS =
      new AspectsRequestBuilders();
  private static final PlatformRequestBuilders PLATFORM_REQUEST_BUILDERS =
      new PlatformRequestBuilders();
  private static final RunsRequestBuilders RUNS_REQUEST_BUILDERS = new RunsRequestBuilders();

  private final ExecutorService batchGetV2Pool;
  private final ExecutorService batchIngestPool;

  public RestliEntityClient(
      @Nonnull final Client restliClient, EntityClientConfig entityClientConfig) {
    super(restliClient, entityClientConfig);
    this.batchGetV2Pool =
        new ThreadPoolExecutor(
            entityClientConfig.getBatchGetV2Concurrency(), // core threads
            entityClientConfig.getBatchGetV2Concurrency(), // max threads
            entityClientConfig.getBatchGetV2KeepAlive(),
            TimeUnit.SECONDS, // thread keep-alive time
            new ArrayBlockingQueue<>(
                entityClientConfig.getBatchGetV2QueueSize()), // fixed size queue
            new ThreadPoolExecutor.CallerRunsPolicy());
    this.batchIngestPool =
        new ThreadPoolExecutor(
            entityClientConfig.getBatchIngestConcurrency(), // core threads
            entityClientConfig.getBatchIngestConcurrency(), // max threads
            entityClientConfig.getBatchIngestKeepAlive(),
            TimeUnit.SECONDS, // thread keep-alive time
            new ArrayBlockingQueue<>(
                entityClientConfig.getBatchIngestQueueSize()), // fixed size queue
            new ThreadPoolExecutor.CallerRunsPolicy());
  }

  @Override
  @Nullable
  public EntityResponse getV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull final Urn urn,
      @Nullable final Set<String> aspectNames,
      @Nullable Boolean alwaysIncludeKeyAspect)
      throws RemoteInvocationException, URISyntaxException {
    final EntitiesV2GetRequestBuilder requestBuilder =
        ENTITIES_V2_REQUEST_BUILDERS
            .get()
            .aspectsParam(aspectNames)
            .id(urn.toString())
            .alwaysIncludeKeyAspectParam(alwaysIncludeKeyAspect);
    return sendClientRequest(requestBuilder, opContext.getSessionAuthentication()).getEntity();
  }

  @Override
  @Nonnull
  public Entity get(@Nonnull OperationContext opContext, @Nonnull final Urn urn)
      throws RemoteInvocationException {
    return sendClientRequest(
            ENTITIES_REQUEST_BUILDERS.get().id(urn.toString()),
            opContext.getSessionAuthentication())
        .getEntity();
  }

  /**
   * Legacy! Use {#batchGetV2} instead, as this method leverages Snapshot models, and will not work
   * for fetching entities + aspects added by Entity Registry configuration.
   *
   * <p>Batch get a set of {@link Entity} objects by urn.
   *
   * @param urns the urns of the entities to batch get
   * @throws RemoteInvocationException when unable to execute request
   */
  @Override
  @Nonnull
  public Map<Urn, Entity> batchGet(
      @Nonnull OperationContext opContext, @Nonnull final Set<Urn> urns)
      throws RemoteInvocationException {

    final Integer batchSize = 25;
    final AtomicInteger index = new AtomicInteger(0);

    final Collection<List<Urn>> entityUrnBatches =
        urns.stream()
            .collect(Collectors.groupingBy(x -> index.getAndIncrement() / batchSize))
            .values();

    final Map<Urn, Entity> response = new HashMap<>();

    for (List<Urn> urnsInBatch : entityUrnBatches) {
      EntitiesBatchGetRequestBuilder batchGetRequestBuilder =
          ENTITIES_REQUEST_BUILDERS
              .batchGet()
              .ids(urnsInBatch.stream().map(Urn::toString).collect(Collectors.toSet()));
      final Map<Urn, Entity> batchResponse =
          sendClientRequest(batchGetRequestBuilder, opContext.getSessionAuthentication())
              .getEntity()
              .getResults()
              .entrySet()
              .stream()
              .collect(
                  Collectors.toMap(
                      entry -> {
                        try {
                          return Urn.createFromString(entry.getKey());
                        } catch (URISyntaxException e) {
                          throw new RuntimeException(
                              String.format(
                                  "Failed to create Urn from key string %s", entry.getKey()));
                        }
                      },
                      entry -> entry.getValue().getEntity()));
      response.putAll(batchResponse);
    }
    return response;
  }

  /**
   * Batch get a set of aspects for multiple entities.
   *
   * @param opContext operation's context
   * @param entityName the entity type to fetch
   * @param urns the urns of the entities to batch get
   * @param aspectNames the aspect names to batch get
   * @throws RemoteInvocationException when unable to execute request
   */
  @Override
  @Nonnull
  public Map<Urn, EntityResponse> batchGetV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull final Set<Urn> urns,
      @Nullable final Set<String> aspectNames,
      @Nullable Boolean alwaysIncludeKeyAspect)
      throws RemoteInvocationException, URISyntaxException {

    Map<Urn, EntityResponse> responseMap = new HashMap<>();

    Iterable<List<Urn>> iterable =
        () -> Iterators.partition(urns.iterator(), entityClientConfig.getBatchGetV2Size());
    List<Future<Map<Urn, EntityResponse>>> futures =
        StreamSupport.stream(iterable.spliterator(), false)
            .map(
                batch ->
                    batchGetV2Pool.submit(
                        () -> {
                          try {
                            log.debug("Executing batchGetV2 with batch size: {}", batch.size());
                            final EntitiesV2BatchGetRequestBuilder requestBuilder =
                                ENTITIES_V2_REQUEST_BUILDERS
                                    .batchGet()
                                    .aspectsParam(aspectNames)
                                    .alwaysIncludeKeyAspectParam(alwaysIncludeKeyAspect)
                                    .ids(
                                        batch.stream()
                                            .map(Urn::toString)
                                            .collect(Collectors.toList()));

                            return sendClientRequest(
                                    requestBuilder, opContext.getSessionAuthentication())
                                .getEntity()
                                .getResults()
                                .entrySet()
                                .stream()
                                .collect(
                                    Collectors.toMap(
                                        entry -> UrnUtils.getUrn(entry.getKey()),
                                        entry -> entry.getValue().getEntity()));
                          } catch (RemoteInvocationException e) {
                            throw new RuntimeException(e);
                          }
                        }))
            .collect(Collectors.toList());

    futures.forEach(
        result -> {
          try {
            responseMap.putAll(result.get());
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        });

    return responseMap;
  }

  /**
   * Batch get a set of versioned aspects for a single entity.
   *
   * @param entityName the entity type to fetch
   * @param versionedUrns the urns of the entities to batch get
   * @param aspectNames the aspect names to batch get
   * @throws RemoteInvocationException when unable to execute request
   */
  @Override
  @Nonnull
  public Map<Urn, EntityResponse> batchGetVersionedV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull final Set<VersionedUrn> versionedUrns,
      @Nullable final Set<String> aspectNames) {

    Map<Urn, EntityResponse> responseMap = new HashMap<>();

    Iterable<List<VersionedUrn>> iterable =
        () -> Iterators.partition(versionedUrns.iterator(), entityClientConfig.getBatchGetV2Size());
    List<Future<Map<Urn, EntityResponse>>> futures =
        StreamSupport.stream(iterable.spliterator(), false)
            .map(
                batch ->
                    batchGetV2Pool.submit(
                        () -> {
                          try {
                            log.debug(
                                "Executing batchGetVersionedV2 with batch size: {}", batch.size());
                            final EntitiesVersionedV2BatchGetRequestBuilder requestBuilder =
                                ENTITIES_VERSIONED_V2_REQUEST_BUILDERS
                                    .batchGet()
                                    .aspectsParam(aspectNames)
                                    .entityTypeParam(entityName)
                                    .ids(
                                        batch.stream()
                                            .map(
                                                versionedUrn ->
                                                    com.linkedin.common.urn.VersionedUrn.of(
                                                        versionedUrn.getUrn().toString(),
                                                        versionedUrn.getVersionStamp()))
                                            .collect(Collectors.toSet()));

                            return sendClientRequest(
                                    requestBuilder, opContext.getSessionAuthentication())
                                .getEntity()
                                .getResults()
                                .entrySet()
                                .stream()
                                .collect(
                                    Collectors.toMap(
                                        entry -> UrnUtils.getUrn(entry.getKey().getUrn()),
                                        entry -> entry.getValue().getEntity()));
                          } catch (RemoteInvocationException e) {
                            throw new RuntimeException(e);
                          }
                        }))
            .collect(Collectors.toList());

    futures.forEach(
        result -> {
          try {
            responseMap.putAll(result.get());
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        });

    return responseMap;
  }

  /**
   * Autocomplete a search query for a particular field of an entity.
   *
   * @param entityType the entity type to autocomplete against, e.g. 'dataset'
   * @param query search query
   * @param field field of the dataset to autocomplete against, e.g. 'name'
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
    EntitiesDoAutocompleteRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS
            .actionAutocomplete()
            .entityParam(entityType)
            .queryParam(query)
            .fieldParam(field)
            .filterParam(filterOrDefaultEmptyFilter(requestFilters))
            .limitParam(limit);
    return sendClientRequest(requestBuilder, opContext.getAuthentication()).getEntity();
  }

  /**
   * Autocomplete a search query for a particular entity type.
   *
   * @param entityType the entity type to autocomplete against, e.g. 'dataset'
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
    EntitiesDoAutocompleteRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS
            .actionAutocomplete()
            .entityParam(entityType)
            .queryParam(query)
            .filterParam(filterOrDefaultEmptyFilter(requestFilters))
            .limitParam(limit);
    return sendClientRequest(requestBuilder, opContext.getAuthentication()).getEntity();
  }

  /**
   * Gets browse snapshot of a given path
   *
   * @param entityType entity type being browse
   * @param path path being browsed
   * @param requestFilters browse filters
   * @param start start offset of first dataset
   * @param limit max number of datasets
   * @throws RemoteInvocationException when unable to execute request
   */
  @Override
  @Nonnull
  public BrowseResult browse(
      @Nonnull OperationContext opContext,
      @Nonnull String entityType,
      @Nonnull String path,
      @Nullable Map<String, String> requestFilters,
      int start,
      int limit)
      throws RemoteInvocationException {
    EntitiesDoBrowseRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS
            .actionBrowse()
            .pathParam(path)
            .entityParam(entityType)
            .startParam(start)
            .limitParam(limit);
    if (requestFilters != null) {
      requestBuilder.filterParam(newFilter(requestFilters));
    }
    return sendClientRequest(requestBuilder, opContext.getAuthentication()).getEntity();
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
    throw new NotImplementedException("BrowseV2 is not implemented in Restli yet");
  }

  @Nonnull
  @Override
  public BrowseResultV2 browseV2(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input,
      int start,
      int count)
      throws RemoteInvocationException {
    throw new NotImplementedException("BrowseV2 is not implemented in Restli yet");
  }

  @Override
  public void update(@Nonnull OperationContext opContext, @Nonnull final Entity entity)
      throws RemoteInvocationException {
    EntitiesDoIngestRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS.actionIngest().entityParam(entity);
    sendClientRequest(requestBuilder, opContext.getSessionAuthentication());
  }

  @Override
  public void updateWithSystemMetadata(
      @Nonnull OperationContext opContext,
      @Nonnull final Entity entity,
      @Nullable final SystemMetadata systemMetadata)
      throws RemoteInvocationException {
    if (systemMetadata == null) {
      update(opContext, entity);
      return;
    }

    EntitiesDoIngestRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS
            .actionIngest()
            .entityParam(entity)
            .systemMetadataParam(systemMetadata);

    sendClientRequest(requestBuilder, opContext.getSessionAuthentication());
  }

  @Override
  public void batchUpdate(@Nonnull OperationContext opContext, @Nonnull final Set<Entity> entities)
      throws RemoteInvocationException {
    EntitiesDoBatchIngestRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS.actionBatchIngest().entitiesParam(new EntityArray(entities));

    sendClientRequest(requestBuilder, opContext.getSessionAuthentication());
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
  @Nonnull
  @Override
  public SearchResult search(
      @Nonnull OperationContext opContext,
      @Nonnull String entity,
      @Nonnull String input,
      @Nullable Map<String, String> requestFilters,
      int start,
      int count)
      throws RemoteInvocationException {

    SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();

    final EntitiesDoSearchRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS
            .actionSearch()
            .entityParam(entity)
            .inputParam(input)
            .filterParam(newFilter(requestFilters))
            .startParam(start)
            .fulltextParam(searchFlags != null ? searchFlags.isFulltext() : null)
            .countParam(count);
    requestBuilder.searchFlagsParam(opContext.getSearchContext().getSearchFlags());

    return sendClientRequest(requestBuilder, opContext.getAuthentication()).getEntity();
  }

  /**
   * Filters for entities matching to a given query and filters
   *
   * @param requestFilters search filters
   * @param start start offset for search results
   * @param count max number of search results requested
   * @return a set of list results
   * @throws RemoteInvocationException when unable to execute request
   */
  @Override
  @Nonnull
  public ListResult list(
      @Nonnull OperationContext opContext,
      @Nonnull String entity,
      @Nullable Map<String, String> requestFilters,
      int start,
      int count)
      throws RemoteInvocationException {
    final EntitiesDoListRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS
            .actionList()
            .entityParam(entity)
            .filterParam(newFilter(requestFilters))
            .startParam(start)
            .countParam(count);

    return sendClientRequest(requestBuilder, opContext.getAuthentication()).getEntity();
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
  @Nonnull
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

    SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();
    final EntitiesDoSearchRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS
            .actionSearch()
            .entityParam(entity)
            .inputParam(input)
            .startParam(start)
            .countParam(count);

    if (filter != null) {
      requestBuilder.filterParam(filter);
    }

    if (!CollectionUtils.isEmpty(sortCriteria)) {
      requestBuilder.sortParam(sortCriteria.get(0));
      requestBuilder.sortCriteriaParam(new SortCriterionArray(sortCriteria));
    }

    if (searchFlags != null) {
      requestBuilder.searchFlagsParam(searchFlags);
      if (searchFlags.isFulltext() != null) {
        requestBuilder.fulltextParam(searchFlags.isFulltext());
      }
    }

    return sendClientRequest(requestBuilder, opContext.getAuthentication()).getEntity();
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
   * @return Snapshot key
   * @throws RemoteInvocationException when unable to execute request
   */
  @Override
  @Nonnull
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

    SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();
    final EntitiesDoSearchAcrossEntitiesRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS
            .actionSearchAcrossEntities()
            .inputParam(input)
            .startParam(start)
            .countParam(count);

    if (entities != null) {
      requestBuilder.entitiesParam(new StringArray(entities));
    }
    if (filter != null) {
      requestBuilder.filterParam(filter);
    }
    if (searchFlags != null) {
      requestBuilder.searchFlagsParam(searchFlags);
    }

    if (!CollectionUtils.isEmpty(sortCriteria)) {
      requestBuilder.sortParam(sortCriteria.get(0));
      requestBuilder.sortCriteriaParam(new SortCriterionArray(sortCriteria));
    }

    return sendClientRequest(requestBuilder, opContext.getAuthentication()).getEntity();
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
    final SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();
    final EntitiesDoScrollAcrossEntitiesRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS.actionScrollAcrossEntities().inputParam(input).countParam(count);

    if (entities != null) {
      requestBuilder.entitiesParam(new StringArray(entities));
    }
    if (filter != null) {
      requestBuilder.filterParam(filter);
    }
    if (scrollId != null) {
      requestBuilder.scrollIdParam(scrollId);
    }
    if (searchFlags != null) {
      requestBuilder.searchFlagsParam(searchFlags);
    }
    if (keepAlive != null) {
      requestBuilder.keepAliveParam(keepAlive);
    }

    return sendClientRequest(requestBuilder, opContext.getAuthentication()).getEntity();
  }

  @Nonnull
  @Override
  public LineageSearchResult searchAcrossLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn sourceUrn,
      @Nonnull LineageDirection direction,
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Integer maxHops,
      @Nullable Filter filter,
      List<SortCriterion> sortCriteria,
      int start,
      int count)
      throws RemoteInvocationException {

    final EntitiesDoSearchAcrossLineageRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS
            .actionSearchAcrossLineage()
            .urnParam(sourceUrn.toString())
            .directionParam(direction.name())
            .inputParam(input)
            .startParam(start)
            .countParam(count);

    if (entities != null) {
      requestBuilder.entitiesParam(new StringArray(entities));
    }
    if (filter != null) {
      requestBuilder.filterParam(filter);
    }
    LineageFlags lineageFlags = opContext.getSearchContext().getLineageFlags();
    if (lineageFlags.getStartTimeMillis() != null) {
      requestBuilder.startTimeMillisParam(lineageFlags.getStartTimeMillis());
    }
    if (lineageFlags.getEndTimeMillis() != null) {
      requestBuilder.endTimeMillisParam(lineageFlags.getEndTimeMillis());
    }

    if (!CollectionUtils.isEmpty(sortCriteria)) {
      requestBuilder.sortParam(sortCriteria.get(0));
      requestBuilder.sortCriteriaParam(new SortCriterionArray(sortCriteria));
    }

    requestBuilder.searchFlagsParam(opContext.getSearchContext().getSearchFlags());

    return sendClientRequest(requestBuilder, opContext.getAuthentication()).getEntity();
  }

  @Override
  @Nonnull
  public LineageScrollResult scrollAcrossLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn sourceUrn,
      @Nonnull LineageDirection direction,
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Integer maxHops,
      @Nullable Filter filter,
      List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nonnull String keepAlive,
      int count)
      throws RemoteInvocationException {
    final EntitiesDoScrollAcrossLineageRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS
            .actionScrollAcrossLineage()
            .urnParam(sourceUrn.toString())
            .directionParam(direction.name())
            .inputParam(input)
            .countParam(count)
            .keepAliveParam(keepAlive);

    if (entities != null) {
      requestBuilder.entitiesParam(new StringArray(entities));
    }
    if (filter != null) {
      requestBuilder.filterParam(filter);
    }
    if (scrollId != null) {
      requestBuilder.scrollIdParam(scrollId);
    }
    LineageFlags lineageFlags = opContext.getSearchContext().getLineageFlags();
    if (lineageFlags.getStartTimeMillis() != null) {
      requestBuilder.startTimeMillisParam(lineageFlags.getStartTimeMillis());
    }
    if (lineageFlags.getEndTimeMillis() != null) {
      requestBuilder.endTimeMillisParam(lineageFlags.getEndTimeMillis());
    }

    if (!CollectionUtils.isEmpty(sortCriteria)) {
      requestBuilder.sortParam(sortCriteria.get(0));
      requestBuilder.sortCriteriaParam(new SortCriterionArray(sortCriteria));
    }

    requestBuilder.searchFlagsParam(opContext.getSearchContext().getSearchFlags());

    return sendClientRequest(requestBuilder, opContext.getAuthentication()).getEntity();
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
    EntitiesDoGetBrowsePathsRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS.actionGetBrowsePaths().urnParam(urn);
    return sendClientRequest(requestBuilder, opContext.getSessionAuthentication()).getEntity();
  }

  @Override
  public void setWritable(@Nonnull OperationContext opContext, boolean canWrite)
      throws RemoteInvocationException {
    EntitiesDoSetWritableRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS.actionSetWritable().valueParam(canWrite);
    sendClientRequest(requestBuilder, opContext.getSessionAuthentication());
  }

  @Override
  @Nonnull
  public Map<String, Long> batchGetTotalEntityCount(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityName,
      @Nullable Filter filter)
      throws RemoteInvocationException {
    if (filter != null) {
      throw new UnsupportedOperationException("Filter not yet supported in restli-client.");
    }

    EntitiesDoBatchGetTotalEntityCountRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS
            .actionBatchGetTotalEntityCount()
            .entitiesParam(new StringArray(entityName));
    return sendClientRequest(requestBuilder, opContext.getAuthentication()).getEntity();
  }

  /** List all urns existing for a particular Entity type. */
  @Override
  public ListUrnsResult listUrns(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityName,
      final int start,
      final int count)
      throws RemoteInvocationException {
    EntitiesDoListUrnsRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS
            .actionListUrns()
            .entityParam(entityName)
            .startParam(start)
            .countParam(count);
    return sendClientRequest(requestBuilder, opContext.getSessionAuthentication()).getEntity();
  }

  /** Hard delete an entity with a particular urn. */
  @Override
  public void deleteEntity(@Nonnull OperationContext opContext, @Nonnull final Urn urn)
      throws RemoteInvocationException {
    EntitiesDoDeleteRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS.actionDelete().urnParam(urn.toString());
    sendClientRequest(requestBuilder, opContext.getSessionAuthentication());
  }

  /** Delete all references to a particular entity. */
  @Override
  public void deleteEntityReferences(@Nonnull OperationContext opContext, @Nonnull Urn urn)
      throws RemoteInvocationException {
    EntitiesDoDeleteReferencesRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS.actionDeleteReferences().urnParam(urn.toString());
    sendClientRequest(requestBuilder, opContext.getSessionAuthentication());
  }

  @Nonnull
  @Override
  public SearchResult filter(
      @Nonnull OperationContext opContext,
      @Nonnull String entity,
      @Nonnull Filter filter,
      List<SortCriterion> sortCriteria,
      int start,
      int count)
      throws RemoteInvocationException {
    EntitiesDoFilterRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS
            .actionFilter()
            .entityParam(entity)
            .filterParam(filter)
            .startParam(start)
            .countParam(count);
    if (!CollectionUtils.isEmpty(sortCriteria)) {
      requestBuilder.sortParam(sortCriteria.get(0));
      requestBuilder.sortCriteriaParam(new SortCriterionArray(sortCriteria));
    }
    return sendClientRequest(requestBuilder, opContext.getAuthentication()).getEntity();
  }

  @Override
  public boolean exists(@Nonnull OperationContext opContext, @Nonnull Urn urn)
      throws RemoteInvocationException {
    return exists(opContext, urn, true);
  }

  @Override
  public boolean exists(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull Boolean includeSoftDeleted)
      throws RemoteInvocationException {
    EntitiesDoExistsRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS
            .actionExists()
            .urnParam(urn.toString())
            .includeSoftDeleteParam(includeSoftDeleted);
    return sendClientRequest(requestBuilder, opContext.getSessionAuthentication()).getEntity();
  }

  /**
   * Gets aspect at version for an entity
   *
   * @param urn urn for the entity
   * @return list of paths given urn
   * @throws RemoteInvocationException on remote request error.
   */
  @Override
  @Nonnull
  public VersionedAspect getAspect(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version)
      throws RemoteInvocationException {

    AspectsGetRequestBuilder requestBuilder =
        ASPECTS_REQUEST_BUILDERS.get().id(urn).aspectParam(aspect).versionParam(version);

    return sendClientRequest(requestBuilder, opContext.getSessionAuthentication()).getEntity();
  }

  /**
   * Gets aspect at version for an entity, or null if one doesn't exist.
   *
   * @param urn urn for the entity
   * @return list of paths given urn
   * @throws RemoteInvocationException on remote request error.
   */
  @Override
  @Nullable
  public VersionedAspect getAspectOrNull(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version)
      throws RemoteInvocationException {

    AspectsGetRequestBuilder requestBuilder =
        ASPECTS_REQUEST_BUILDERS.get().id(urn).aspectParam(aspect).versionParam(version);
    try {
      return sendClientRequest(requestBuilder, opContext.getSessionAuthentication()).getEntity();
    } catch (RestLiResponseException e) {
      if (e.getStatus() == HttpStatus.S_404_NOT_FOUND.getCode()) {
        // Then the aspect was not found. Return null.
        return null;
      }
      throw e;
    }
  }

  /**
   * Retrieve instances of a particular aspect.
   *
   * @param urn urn for the entity.
   * @param entity the name of the entity.
   * @param aspect the name of the aspect.
   * @param startTimeMillis the earliest desired event time of the aspect value in milliseconds.
   * @param endTimeMillis the latest desired event time of the aspect value in milliseconds.
   * @param limit the maximum number of desired aspect values.
   * @return the list of EnvelopedAspect values satisfying the input parameters.
   * @throws RemoteInvocationException on remote request error.
   */
  @Override
  @Nonnull
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

    AspectsDoGetTimeseriesAspectValuesRequestBuilder requestBuilder =
        ASPECTS_REQUEST_BUILDERS
            .actionGetTimeseriesAspectValues()
            .urnParam(urn)
            .entityParam(entity)
            .aspectParam(aspect);

    if (startTimeMillis != null) {
      requestBuilder.startTimeMillisParam(startTimeMillis);
    }

    if (endTimeMillis != null) {
      requestBuilder.endTimeMillisParam(endTimeMillis);
    }

    if (limit != null) {
      requestBuilder.limitParam(limit);
    }

    if (filter != null) {
      requestBuilder.filterParam(filter);
    }

    if (sort != null) {
      requestBuilder.sortParam(sort);
    }

    return sendClientRequest(requestBuilder, opContext.getSessionAuthentication())
        .getEntity()
        .getValues();
  }

  @Nonnull
  @Override
  public List<String> batchIngestProposals(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<MetadataChangeProposal> metadataChangeProposals,
      boolean async)
      throws RemoteInvocationException {

    List<String> response = new ArrayList<>();

    Iterable<List<MetadataChangeProposal>> iterable =
        () ->
            Iterators.partition(
                metadataChangeProposals.iterator(), entityClientConfig.getBatchIngestSize());
    List<Future<List<String>>> futures =
        StreamSupport.stream(iterable.spliterator(), false)
            .map(
                batch ->
                    batchIngestPool.submit(
                        () -> {
                          try {
                            log.debug(
                                "Executing batchIngestProposals with batch size: {}", batch.size());
                            final AspectsDoIngestProposalBatchRequestBuilder requestBuilder =
                                ASPECTS_REQUEST_BUILDERS
                                    .actionIngestProposalBatch()
                                    .proposalsParam(
                                        new MetadataChangeProposalArray(metadataChangeProposals))
                                    .asyncParam(String.valueOf(async));
                            String result =
                                sendClientRequest(
                                        requestBuilder, opContext.getSessionAuthentication())
                                    .getEntity();

                            if (RESTLI_SUCCESS.equals(result)) {
                              return batch.stream()
                                  .map(
                                      mcp -> {
                                        if (mcp.getEntityUrn() != null) {
                                          return mcp.getEntityUrn().toString();
                                        } else {
                                          EntitySpec entitySpec =
                                              opContext
                                                  .getEntityRegistry()
                                                  .getEntitySpec(mcp.getEntityType());
                                          return EntityKeyUtils.getUrnFromProposal(
                                                  mcp, entitySpec.getKeyAspectSpec())
                                              .toString();
                                        }
                                      })
                                  .collect(Collectors.toList());
                            }
                            return Collections.<String>emptyList();
                          } catch (RemoteInvocationException e) {
                            throw new RuntimeException(e);
                          }
                        }))
            .collect(Collectors.toList());

    futures.forEach(
        result -> {
          try {
            response.addAll(result.get());
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        });

    return response;
  }

  @Override
  public <T extends RecordTemplate> Optional<T> getVersionedAspect(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version,
      @Nonnull Class<T> aspectClass)
      throws RemoteInvocationException {

    AspectsGetRequestBuilder requestBuilder =
        ASPECTS_REQUEST_BUILDERS.get().id(urn).aspectParam(aspect).versionParam(version);

    try {
      VersionedAspect entity =
          sendClientRequest(requestBuilder, opContext.getSessionAuthentication()).getEntity();
      if (entity.hasAspect()) {
        DataMap rawAspect = ((DataMap) entity.data().get("aspect"));
        if (rawAspect.containsKey(aspectClass.getCanonicalName())) {
          DataMap aspectDataMap = rawAspect.getDataMap(aspectClass.getCanonicalName());
          return Optional.of(RecordUtils.toRecordTemplate(aspectClass, aspectDataMap));
        }
      }
    } catch (RestLiResponseException e) {
      if (e.getStatus() == 404) {
        log.debug("Could not find aspect {} for entity {}", aspect, urn);
        return Optional.empty();
      } else {
        // re-throw other exceptions
        throw e;
      }
    }

    return Optional.empty();
  }

  @SneakyThrows
  @Override
  public DataMap getRawAspect(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version)
      throws RemoteInvocationException {
    throw new MethodNotSupportedException("Method not supported");
  }

  @Override
  public void producePlatformEvent(
      @Nonnull OperationContext opContext,
      @Nonnull String name,
      @Nullable String key,
      @Nonnull PlatformEvent event)
      throws Exception {
    final PlatformDoProducePlatformEventRequestBuilder requestBuilder =
        PLATFORM_REQUEST_BUILDERS.actionProducePlatformEvent().nameParam(name).eventParam(event);
    if (key != null) {
      requestBuilder.keyParam(key);
    }
    sendClientRequest(requestBuilder, opContext.getSessionAuthentication());
  }

  @Override
  public void rollbackIngestion(
      @Nonnull OperationContext opContext, @Nonnull String runId, @Nonnull Authorizer authorizer)
      throws Exception {
    final RunsDoRollbackRequestBuilder requestBuilder =
        RUNS_REQUEST_BUILDERS.actionRollback().runIdParam(runId).dryRunParam(false);
    sendClientRequest(requestBuilder, opContext.getSessionAuthentication());
  }

  // TODO: Refactor QueryUtils inside of metadata-io to extract these methods into a single shared
  // library location.
  // Creates new Filter from a map of Criteria by removing null-valued Criteria and using EQUAL
  // condition (default).
  @Nonnull
  public static Filter newFilter(@Nullable Map<String, String> params) {
    if (params == null) {
      return new Filter().setOr(new ConjunctiveCriterionArray());
    }
    CriterionArray criteria =
        params.entrySet().stream()
            .filter(e -> Objects.nonNull(e.getValue()))
            .map(e -> buildCriterion(e.getKey(), Condition.EQUAL, e.getValue()))
            .collect(Collectors.toCollection(CriterionArray::new));
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                ImmutableList.of(new ConjunctiveCriterion().setAnd(criteria))));
  }

  @Nonnull
  public static Filter filterOrDefaultEmptyFilter(@Nullable Filter filter) {
    return filter != null ? filter : new Filter().setOr(new ConjunctiveCriterionArray());
  }
}

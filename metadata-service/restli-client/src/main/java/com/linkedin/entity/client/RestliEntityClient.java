package com.linkedin.entity.client;

import com.datahub.authentication.Authentication;
import com.datahub.util.RecordUtils;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.client.BaseClient;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.AspectsDoGetTimeseriesAspectValuesRequestBuilder;
import com.linkedin.entity.AspectsDoIngestProposalRequestBuilder;
import com.linkedin.entity.AspectsGetRequestBuilder;
import com.linkedin.entity.AspectsRequestBuilders;
import com.linkedin.entity.EntitiesBatchGetRequestBuilder;
import com.linkedin.entity.EntitiesDoAutocompleteRequestBuilder;
import com.linkedin.entity.EntitiesDoBatchGetTotalEntityCountRequestBuilder;
import com.linkedin.entity.EntitiesDoBatchIngestRequestBuilder;
import com.linkedin.entity.EntitiesDoBrowseRequestBuilder;
import com.linkedin.entity.EntitiesDoDeleteRequestBuilder;
import com.linkedin.entity.EntitiesDoFilterRequestBuilder;
import com.linkedin.entity.EntitiesDoGetBrowsePathsRequestBuilder;
import com.linkedin.entity.EntitiesDoIngestRequestBuilder;
import com.linkedin.entity.EntitiesDoListRequestBuilder;
import com.linkedin.entity.EntitiesDoListUrnsRequestBuilder;
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
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.platform.PlatformDoProducePlatformEventRequestBuilder;
import com.linkedin.platform.PlatformRequestBuilders;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.common.HttpStatus;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.mail.MethodNotSupportedException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;


@Slf4j
public class RestliEntityClient extends BaseClient implements EntityClient {

  private static final EntitiesRequestBuilders ENTITIES_REQUEST_BUILDERS = new EntitiesRequestBuilders();
  private static final EntitiesV2RequestBuilders ENTITIES_V2_REQUEST_BUILDERS = new EntitiesV2RequestBuilders();
  private static final EntitiesVersionedV2RequestBuilders ENTITIES_VERSIONED_V2_REQUEST_BUILDERS =
      new EntitiesVersionedV2RequestBuilders();
  private static final AspectsRequestBuilders ASPECTS_REQUEST_BUILDERS = new AspectsRequestBuilders();
  private static final PlatformRequestBuilders PLATFORM_REQUEST_BUILDERS = new PlatformRequestBuilders();

  public RestliEntityClient(@Nonnull final Client restliClient) {
    super(restliClient);
  }

  @Nullable
  public EntityResponse getV2(@Nonnull String entityName, @Nonnull final Urn urn,
      @Nullable final Set<String> aspectNames, @Nonnull final Authentication authentication)
      throws RemoteInvocationException, URISyntaxException {
    final EntitiesV2GetRequestBuilder requestBuilder = ENTITIES_V2_REQUEST_BUILDERS.get()
        .aspectsParam(aspectNames)
        .id(urn.toString());
    return sendClientRequest(requestBuilder, authentication).getEntity();
  }

  @Nonnull
  public Entity get(@Nonnull final Urn urn, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    return sendClientRequest(ENTITIES_REQUEST_BUILDERS.get().id(urn.toString()), authentication).getEntity();
  }

  /**
   * Legacy! Use {#batchGetV2} instead, as this method leverages Snapshot models, and will not work
   * for fetching entities + aspects added by Entity Registry configuration. 
   *
   * Batch get a set of {@link Entity} objects by urn.  
   *
   * @param urns the urns of the entities to batch get
   * @param authentication the authentication to include in the request to the Metadata Service
   * @throws RemoteInvocationException
   */
  @Nonnull
  public Map<Urn, Entity> batchGet(@Nonnull final Set<Urn> urns, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {

    final Integer batchSize = 25;
    final AtomicInteger index = new AtomicInteger(0);

    final Collection<List<Urn>> entityUrnBatches =
        urns.stream().collect(Collectors.groupingBy(x -> index.getAndIncrement() / batchSize)).values();

    final Map<Urn, Entity> response = new HashMap<>();

    for (List<Urn> urnsInBatch : entityUrnBatches) {
      EntitiesBatchGetRequestBuilder batchGetRequestBuilder =
          ENTITIES_REQUEST_BUILDERS.batchGet().ids(urnsInBatch.stream().map(Urn::toString).collect(Collectors.toSet()));
      final Map<Urn, Entity> batchResponse = sendClientRequest(batchGetRequestBuilder, authentication).getEntity()
          .getResults()
          .entrySet()
          .stream()
          .collect(Collectors.toMap(entry -> {
            try {
              return Urn.createFromString(entry.getKey());
            } catch (URISyntaxException e) {
              throw new RuntimeException(String.format("Failed to create Urn from key string %s", entry.getKey()));
            }
          }, entry -> entry.getValue().getEntity()));
      response.putAll(batchResponse);
    }
    return response;
  }

  /**
   * Batch get a set of aspects for a single entity. 
   *
   * @param entityName the entity type to fetch 
   * @param urns the urns of the entities to batch get
   * @param aspectNames the aspect names to batch get
   * @param authentication the authentication to include in the request to the Metadata Service
   * @throws RemoteInvocationException
   */
  @Nonnull
  public Map<Urn, EntityResponse> batchGetV2(@Nonnull String entityName, @Nonnull final Set<Urn> urns,
      @Nullable final Set<String> aspectNames, @Nonnull final Authentication authentication) throws RemoteInvocationException, URISyntaxException {

    final EntitiesV2BatchGetRequestBuilder requestBuilder = ENTITIES_V2_REQUEST_BUILDERS.batchGet()
        .aspectsParam(aspectNames)
        .ids(urns.stream().map(Urn::toString).collect(Collectors.toList()));

    return sendClientRequest(requestBuilder, authentication).getEntity()
        .getResults()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(entry -> {
          try {
            return Urn.createFromString(entry.getKey());
          } catch (URISyntaxException e) {
            throw new RuntimeException(
                String.format("Failed to bind urn string with value %s into urn", entry.getKey()));
          }
        }, entry -> entry.getValue().getEntity()));
  }

  /**
   * Batch get a set of versioned aspects for a single entity.
   *
   * @param entityName the entity type to fetch
   * @param versionedUrns the urns of the entities to batch get
   * @param aspectNames the aspect names to batch get
   * @param authentication the authentication to include in the request to the Metadata Service
   * @throws RemoteInvocationException
   */
  @Nonnull
  public Map<Urn, EntityResponse> batchGetVersionedV2(
      @Nonnull String entityName,
      @Nonnull final Set<VersionedUrn> versionedUrns,
      @Nullable final Set<String> aspectNames,
      @Nonnull final Authentication authentication) throws RemoteInvocationException, URISyntaxException {

    final EntitiesVersionedV2BatchGetRequestBuilder requestBuilder = ENTITIES_VERSIONED_V2_REQUEST_BUILDERS.batchGet()
        .aspectsParam(aspectNames)
        .entityTypeParam(entityName)
        .ids(versionedUrns.stream()
            .map(versionedUrn -> com.linkedin.common.urn.VersionedUrn.of(versionedUrn.getUrn().toString(), versionedUrn.getVersionStamp()))
            .collect(Collectors.toSet()));

    return sendClientRequest(requestBuilder, authentication).getEntity()
        .getResults()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(entry ->
            UrnUtils.getUrn(entry.getKey().getUrn()), entry -> entry.getValue().getEntity()));
  }

  /**
   * Autocomplete a search query for a particular field of an entity. 
   *
   * @param entityType the entity type to autocomplete against, e.g. 'dataset' 
   * @param query search query
   * @param field field of the dataset
   * @param requestFilters autocomplete filters
   * @param limit max number of autocomplete results
   * @param field the field to autocomplete against, e.g. 'name'
   * @throws RemoteInvocationException
   */
  @Nonnull
  public AutoCompleteResult autoComplete(@Nonnull String entityType, @Nonnull String query,
      @Nonnull Map<String, String> requestFilters, @Nonnull int limit, @Nullable String field,
      @Nonnull final Authentication authentication) throws RemoteInvocationException {
    EntitiesDoAutocompleteRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS.actionAutocomplete()
        .entityParam(entityType)
        .queryParam(query)
        .fieldParam(field)
        .filterParam(newFilter(requestFilters))
        .limitParam(limit);
    return sendClientRequest(requestBuilder, authentication).getEntity();
  }

  /**
   * Autocomplete a search query for a particular entity type. 
   *
   * @param entityType the entity type to autocomplete against, e.g. 'dataset' 
   * @param query search query
   * @param requestFilters autocomplete filters
   * @param limit max number of autocomplete results
   * @throws RemoteInvocationException
   */
  @Nonnull
  public AutoCompleteResult autoComplete(@Nonnull String entityType, @Nonnull String query,
      @Nonnull Map<String, String> requestFilters, @Nonnull int limit, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    EntitiesDoAutocompleteRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS.actionAutocomplete()
        .entityParam(entityType)
        .queryParam(query)
        .filterParam(newFilter(requestFilters))
        .limitParam(limit);
    return sendClientRequest(requestBuilder, authentication).getEntity();
  }

  /**
   * Gets browse snapshot of a given path
   *
   * @param entityType entity type being browse
   * @param path path being browsed
   * @param requestFilters browse filters
   * @param start start offset of first dataset
   * @param limit max number of datasets
   * @throws RemoteInvocationException
   */
  @Nonnull
  public BrowseResult browse(@Nonnull String entityType, @Nonnull String path,
      @Nullable Map<String, String> requestFilters, int start, int limit, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    EntitiesDoBrowseRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS.actionBrowse()
        .pathParam(path)
        .entityParam(entityType)
        .startParam(start)
        .limitParam(limit);
    if (requestFilters != null) {
      requestBuilder.filterParam(newFilter(requestFilters));
    }
    return sendClientRequest(requestBuilder, authentication).getEntity();
  }

  public void update(@Nonnull final Entity entity, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    EntitiesDoIngestRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS.actionIngest().entityParam(entity);
    sendClientRequest(requestBuilder, authentication);
  }

  public void updateWithSystemMetadata(@Nonnull final Entity entity, @Nullable final SystemMetadata systemMetadata,
      @Nonnull final Authentication authentication) throws RemoteInvocationException {
    if (systemMetadata == null) {
      update(entity, authentication);
      return;
    }

    EntitiesDoIngestRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS.actionIngest().entityParam(entity).systemMetadataParam(systemMetadata);

    sendClientRequest(requestBuilder, authentication);
  }

  public void batchUpdate(@Nonnull final Set<Entity> entities, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    EntitiesDoBatchIngestRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS.actionBatchIngest().entitiesParam(new EntityArray(entities));

    sendClientRequest(requestBuilder, authentication);
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
  public SearchResult search(@Nonnull String entity, @Nonnull String input,
      @Nullable Map<String, String> requestFilters, int start, int count, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {

    final EntitiesDoSearchRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS.actionSearch()
        .entityParam(entity)
        .inputParam(input)
        .filterParam(newFilter(requestFilters))
        .startParam(start)
        .countParam(count);

    return sendClientRequest(requestBuilder, authentication).getEntity();
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
  public ListResult list(@Nonnull String entity, @Nullable Map<String, String> requestFilters, int start, int count,
      @Nonnull final Authentication authentication) throws RemoteInvocationException {
    final EntitiesDoListRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS.actionList()
        .entityParam(entity)
        .filterParam(newFilter(requestFilters))
        .startParam(start)
        .countParam(count);

    return sendClientRequest(requestBuilder, authentication).getEntity();
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
  public SearchResult search(@Nonnull String entity, @Nonnull String input, @Nullable Filter filter,
      SortCriterion sortCriterion, int start, int count, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {

    final EntitiesDoSearchRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS.actionSearch()
        .entityParam(entity)
        .inputParam(input)
        .startParam(start)
        .countParam(count);

    if (filter != null) {
      requestBuilder.filterParam(filter);
    }

    if (sortCriterion != null) {
      requestBuilder.sortParam(sortCriterion);
    }

    return sendClientRequest(requestBuilder, authentication).getEntity();
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
  public SearchResult searchAcrossEntities(@Nonnull List<String> entities, @Nonnull String input,
      @Nullable Filter filter, int start, int count, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {

    final EntitiesDoSearchAcrossEntitiesRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS.actionSearchAcrossEntities().inputParam(input).startParam(start).countParam(count);

    if (entities != null) {
      requestBuilder.entitiesParam(new StringArray(entities));
    }
    if (filter != null) {
      requestBuilder.filterParam(filter);
    }

    return sendClientRequest(requestBuilder, authentication).getEntity();
  }

  @Nonnull
  @Override
  public LineageSearchResult searchAcrossLineage(@Nonnull Urn sourceUrn, @Nonnull LineageDirection direction,
      @Nonnull List<String> entities, @Nonnull String input, @Nullable Filter filter,
      @Nullable SortCriterion sortCriterion, int start, int count, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {

    final EntitiesDoSearchAcrossLineageRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS.actionSearchAcrossLineage()
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

    return sendClientRequest(requestBuilder, authentication).getEntity();
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
    EntitiesDoGetBrowsePathsRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS.actionGetBrowsePaths().urnParam(urn);
    return sendClientRequest(requestBuilder, authentication).getEntity();
  }

  public void setWritable(boolean canWrite, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    EntitiesDoSetWritableRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS.actionSetWritable().valueParam(canWrite);
    sendClientRequest(requestBuilder, authentication);
  }

  @Nonnull
  public Map<String, Long> batchGetTotalEntityCount(@Nonnull List<String> entityName,
      @Nonnull final Authentication authentication) throws RemoteInvocationException {
    EntitiesDoBatchGetTotalEntityCountRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS.actionBatchGetTotalEntityCount().entitiesParam(new StringArray(entityName));
    return sendClientRequest(requestBuilder, authentication).getEntity();
  }

  /**
   * List all urns existing for a particular Entity type.
   */
  public ListUrnsResult listUrns(@Nonnull final String entityName, final int start, final int count,
      @Nonnull final Authentication authentication) throws RemoteInvocationException {
    EntitiesDoListUrnsRequestBuilder requestBuilder =
        ENTITIES_REQUEST_BUILDERS.actionListUrns().entityParam(entityName).startParam(start).countParam(count);
    return sendClientRequest(requestBuilder, authentication).getEntity();
  }

  /**
   * Hard delete an entity with a particular urn.
   */
  public void deleteEntity(@Nonnull final Urn urn, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    EntitiesDoDeleteRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS.actionDelete().urnParam(urn.toString());
    sendClientRequest(requestBuilder, authentication);
  }

  @Nonnull
  @Override
  public SearchResult filter(@Nonnull String entity, @Nonnull Filter filter, @Nullable SortCriterion sortCriterion,
      int start, int count, @Nonnull final Authentication authentication) throws RemoteInvocationException {
    EntitiesDoFilterRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS.actionFilter()
        .entityParam(entity)
        .filterParam(filter)
        .startParam(start)
        .countParam(count);
    if (sortCriterion != null) {
      requestBuilder.sortParam(sortCriterion);
    }
    return sendClientRequest(requestBuilder, authentication).getEntity();
  }

  /**
   * Gets aspect at version for an entity
   *
   * @param urn urn for the entity
   * @return list of paths given urn
   * @throws RemoteInvocationException on remote request error.
   */
  @Nonnull
  public VersionedAspect getAspect(@Nonnull String urn, @Nonnull String aspect, @Nonnull Long version,
      @Nonnull final Authentication authentication) throws RemoteInvocationException {

    AspectsGetRequestBuilder requestBuilder =
        ASPECTS_REQUEST_BUILDERS.get().id(urn).aspectParam(aspect).versionParam(version);

    return sendClientRequest(requestBuilder, authentication).getEntity();
  }

  /**
   * Gets aspect at version for an entity, or null if one doesn't exist.
   *
   * @param urn urn for the entity
   * @return list of paths given urn
   * @throws RemoteInvocationException on remote request error.
   */
  @Nullable
  public VersionedAspect getAspectOrNull(@Nonnull String urn, @Nonnull String aspect, @Nonnull Long version,
      @Nonnull final Authentication authentication) throws RemoteInvocationException {

    AspectsGetRequestBuilder requestBuilder =
        ASPECTS_REQUEST_BUILDERS.get().id(urn).aspectParam(aspect).versionParam(version);
    try {
      return sendClientRequest(requestBuilder, authentication).getEntity();
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
   * @param authentication the actor associated with the request [internal]
   * @return the list of EnvelopedAspect values satisfying the input parameters.
   * @throws RemoteInvocationException on remote request error.
   */
  @Nonnull
  public List<EnvelopedAspect> getTimeseriesAspectValues(@Nonnull String urn, @Nonnull String entity,
      @Nonnull String aspect, @Nullable Long startTimeMillis, @Nullable Long endTimeMillis, @Nullable Integer limit,
      @Nonnull Boolean getLatestValue, @Nullable Filter filter, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {

    AspectsDoGetTimeseriesAspectValuesRequestBuilder requestBuilder =
        ASPECTS_REQUEST_BUILDERS.actionGetTimeseriesAspectValues()
            .urnParam(urn)
            .entityParam(entity)
            .aspectParam(aspect)
            .latestValueParam(getLatestValue);

    if (startTimeMillis != null) {
      requestBuilder.startTimeMillisParam(startTimeMillis);
    }

    if (endTimeMillis != null) {
      requestBuilder.endTimeMillisParam(endTimeMillis);
    }

    if (limit != null) {
      requestBuilder.limitParam(limit);
    }

    if (getLatestValue != null) {
      requestBuilder.latestValueParam(getLatestValue);
    }

    if (filter != null) {
      requestBuilder.filterParam(filter);
    }

    return sendClientRequest(requestBuilder, authentication).getEntity().getValues();
  }

  /**
   * Ingest a MetadataChangeProposal event.
   * @return
   */
  public String ingestProposal(@Nonnull final MetadataChangeProposal metadataChangeProposal,
      @Nonnull final Authentication authentication) throws RemoteInvocationException {
    final AspectsDoIngestProposalRequestBuilder requestBuilder =
        ASPECTS_REQUEST_BUILDERS.actionIngestProposal().proposalParam(metadataChangeProposal);
    return sendClientRequest(requestBuilder, authentication).getEntity();
  }

  public <T extends RecordTemplate> Optional<T> getVersionedAspect(@Nonnull String urn, @Nonnull String aspect,
      @Nonnull Long version, @Nonnull Class<T> aspectClass, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {

    AspectsGetRequestBuilder requestBuilder =
        ASPECTS_REQUEST_BUILDERS.get().id(urn).aspectParam(aspect).versionParam(version);

    try {
      VersionedAspect entity = sendClientRequest(requestBuilder, authentication).getEntity();
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
  public DataMap getRawAspect(@Nonnull String urn, @Nonnull String aspect, @Nonnull Long version,
      @Nonnull Authentication authentication) throws RemoteInvocationException {
    throw new MethodNotSupportedException();
  }

  @Override
  public void producePlatformEvent(@Nonnull String name, @Nullable String key, @Nonnull PlatformEvent event, @Nonnull final Authentication authentication)
      throws Exception {
    final PlatformDoProducePlatformEventRequestBuilder requestBuilder =
        PLATFORM_REQUEST_BUILDERS.actionProducePlatformEvent()
          .nameParam(name)
          .eventParam(event);
    if (key != null) {
      requestBuilder.keyParam(key);
    }
    sendClientRequest(requestBuilder, authentication);
  }
}

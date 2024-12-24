package com.linkedin.entity.client;

import static com.linkedin.metadata.utils.GenericRecordUtils.entityResponseToAspectMap;
import static com.linkedin.metadata.utils.GenericRecordUtils.entityResponseToSystemAspectMap;

import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.LineageScrollResult;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

// Consider renaming this to datahub client.
public interface EntityClient {

  /**
   * This version follows the legacy behavior of returning key aspects regardless of whether they
   * exist
   *
   * @param opContext operation context
   * @param entityName entity type
   * @param urn urn id for the entity
   * @param aspectNames set of aspects
   * @return requested entity/aspects
   */
  @Deprecated
  @Nullable
  default EntityResponse getV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull final Urn urn,
      @Nullable final Set<String> aspectNames)
      throws RemoteInvocationException, URISyntaxException {
    return getV2(opContext, entityName, urn, aspectNames, true);
  }

  @Nullable
  EntityResponse getV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull final Urn urn,
      @Nullable final Set<String> aspectNames,
      @Nullable Boolean alwaysIncludeKeyAspect)
      throws RemoteInvocationException, URISyntaxException;

  @Nonnull
  @Deprecated
  Entity get(@Nonnull OperationContext opContext, @Nonnull final Urn urn)
      throws RemoteInvocationException;

  /**
   * This version follows the legacy behavior of returning key aspects regardless of whether they
   * exist
   *
   * @param opContext operation context
   * @param entityName entity type
   * @param urns urn ids for the entities
   * @param aspectNames set of aspects
   * @return requested entity/aspects
   */
  @Deprecated
  @Nonnull
  default Map<Urn, EntityResponse> batchGetV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull final Set<Urn> urns,
      @Nullable final Set<String> aspectNames)
      throws RemoteInvocationException, URISyntaxException {
    return batchGetV2(opContext, entityName, urns, aspectNames, true);
  }

  @Nonnull
  Map<Urn, EntityResponse> batchGetV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull final Set<Urn> urns,
      @Nullable final Set<String> aspectNames,
      @Nullable Boolean alwaysIncludeKeyAspect)
      throws RemoteInvocationException, URISyntaxException;

  @Nonnull
  Map<Urn, EntityResponse> batchGetVersionedV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull final Set<VersionedUrn> versionedUrns,
      @Nullable final Set<String> aspectNames)
      throws RemoteInvocationException, URISyntaxException;

  @Nonnull
  @Deprecated
  Map<Urn, Entity> batchGet(@Nonnull OperationContext opContext, @Nonnull final Set<Urn> urns)
      throws RemoteInvocationException;

  /**
   * Gets browse snapshot of a given path
   *
   * @param query search query
   * @param field field of the dataset
   * @param requestFilters autocomplete filters
   * @param limit max number of autocomplete results
   * @throws RemoteInvocationException when unable to execute request
   */
  @Nonnull
  AutoCompleteResult autoComplete(
      @Nonnull OperationContext opContext,
      @Nonnull String entityType,
      @Nonnull String query,
      @Nullable Filter requestFilters,
      int limit,
      @Nullable String field)
      throws RemoteInvocationException;

  /**
   * Gets browse snapshot of a given path
   *
   * @param query search query
   * @param requestFilters autocomplete filters
   * @param limit max number of autocomplete results
   * @throws RemoteInvocationException when unable to execute request
   */
  @Nonnull
  AutoCompleteResult autoComplete(
      @Nonnull OperationContext opContext,
      @Nonnull String entityType,
      @Nonnull String query,
      @Nullable Filter requestFilters,
      int limit)
      throws RemoteInvocationException;

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
  BrowseResult browse(
      @Nonnull OperationContext opContext,
      @Nonnull String entityType,
      @Nonnull String path,
      @Nullable Map<String, String> requestFilters,
      int start,
      int limit)
      throws RemoteInvocationException;

  /**
   * Gets browse snapshot of a given path
   *
   * @param entityName entity being browsed
   * @param path path being browsed
   * @param filter browse filter
   * @param input search query
   * @param start start offset of first group
   * @param count max number of results requested
   * @throws RemoteInvocationException when unable to execute request
   */
  @Nonnull
  BrowseResultV2 browseV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input,
      int start,
      int count)
      throws RemoteInvocationException;

  /**
   * Gets browse snapshot of a given path
   *
   * @param entityNames entities being browsed
   * @param path path being browsed
   * @param filter browse filter
   * @param input search query
   * @param start start offset of first group
   * @param count max number of results requested
   * @throws RemoteInvocationException when unable to execute request
   */
  @Nonnull
  BrowseResultV2 browseV2(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input,
      int start,
      int count)
      throws RemoteInvocationException;

  @Deprecated
  void update(@Nonnull OperationContext opContext, @Nonnull final Entity entity)
      throws RemoteInvocationException;

  @Deprecated
  void updateWithSystemMetadata(
      @Nonnull OperationContext opContext,
      @Nonnull final Entity entity,
      @Nullable final SystemMetadata systemMetadata)
      throws RemoteInvocationException;

  @Deprecated
  void batchUpdate(@Nonnull OperationContext opContext, @Nonnull final Set<Entity> entities)
      throws RemoteInvocationException;

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
  SearchResult search(
      @Nonnull OperationContext opContext,
      @Nonnull String entity,
      @Nonnull String input,
      @Nullable Map<String, String> requestFilters,
      int start,
      int count)
      throws RemoteInvocationException;

  /**
   * Filters for entities matching to a given query and filters
   *
   * <p>TODO: This no longer has any usages, can we deprecate/remove?
   *
   * @param requestFilters search filters
   * @param start start offset for search results
   * @param count max number of search results requested
   * @return a set of list results
   * @throws RemoteInvocationException when unable to execute request
   */
  ListResult list(
      @Nonnull OperationContext opContext,
      @Nonnull String entity,
      @Nullable Map<String, String> requestFilters,
      int start,
      int count)
      throws RemoteInvocationException;

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
  SearchResult search(
      @Nonnull OperationContext opContext,
      @Nonnull String entity,
      @Nonnull String input,
      @Nullable Filter filter,
      List<SortCriterion> sortCriteria,
      int start,
      int count)
      throws RemoteInvocationException;

  /**
   * Searches for entities matching to a given query and filters across multiple entity types
   *
   * @param entities entity types to search (if empty, searches all entities)
   * @param input search query
   * @param filter search filters
   * @param start start offset for search results
   * @param count max number of search results requested
   * @return Snapshot key
   * @throws RemoteInvocationException when unable to execute request
   */
  @Nonnull
  SearchResult searchAcrossEntities(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter filter,
      int start,
      int count,
      List<SortCriterion> sortCriteria)
      throws RemoteInvocationException;

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
  SearchResult searchAcrossEntities(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter filter,
      int start,
      int count,
      List<SortCriterion> sortCriteria,
      List<String> facets)
      throws RemoteInvocationException;

  /**
   * Searches for entities matching to a given query and filters across multiple entity types
   *
   * @param entities entity types to search (if empty, searches all entities)
   * @param input search query
   * @param filter search filters
   * @param scrollId opaque scroll ID indicating offset
   * @param keepAlive string representation of time to keep point in time alive, ex: 5m
   * @param count max number of search results requested
   * @return Snapshot key
   * @throws RemoteInvocationException when unable to execute request
   */
  @Nonnull
  ScrollResult scrollAcrossEntities(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter filter,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      int count)
      throws RemoteInvocationException;

  /**
   * Gets a list of documents that match given search request that is related to the input entity
   *
   * @param sourceUrn Urn of the source entity
   * @param direction Direction of the relationship
   * @param entities list of entities to search (If empty, searches across all entities)
   * @param input the search input text
   * @param maxHops the max number of hops away to search for. If null, searches all hops.
   * @param filter the request map with fields and values as filters to be applied to search hits
   * @param sortCriteria list of {@link SortCriterion} to be applied to search results
   * @param start index to start the search from
   * @param count the number of search hits to return
   * @return a {@link SearchResult} that contains a list of matched documents and related search
   *     result metadata
   */
  LineageSearchResult searchAcrossLineage(
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
      throws RemoteInvocationException;

  /**
   * Gets a list of documents that match given search request that is related to the input entity
   *
   * @param sourceUrn Urn of the source entity
   * @param direction Direction of the relationship
   * @param entities list of entities to search (If empty, searches across all entities)
   * @param input the search input text
   * @param maxHops the max number of hops away to search for. If null, searches all hops.
   * @param filter the request map with fields and values as filters to be applied to search hits
   * @param sortCriteria list of {@link SortCriterion} to be applied to search results
   * @param scrollId opaque scroll ID indicating offset
   * @param keepAlive string representation of time to keep point in time alive, ex: 5m
   * @param count the number of search hits to return of roundtrips for UI visualizations.
   * @return a {@link SearchResult} that contains a list of matched documents and related search
   *     result metadata
   */
  @Nonnull
  LineageScrollResult scrollAcrossLineage(
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
      throws RemoteInvocationException;

  /**
   * Gets browse path(s) given dataset urn
   *
   * @param urn urn for the entity
   * @return list of paths given urn
   * @throws RemoteInvocationException when unable to execute request
   */
  @Nonnull
  StringArray getBrowsePaths(@Nonnull OperationContext opContext, @Nonnull Urn urn)
      throws RemoteInvocationException;

  void setWritable(@Nonnull OperationContext opContext, boolean canWrite)
      throws RemoteInvocationException;

  @Nonnull
  Map<String, Long> batchGetTotalEntityCount(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityName,
      @Nullable Filter filter)
      throws RemoteInvocationException;

  default Map<String, Long> batchGetTotalEntityCount(
      @Nonnull OperationContext opContext, @Nonnull List<String> entityName)
      throws RemoteInvocationException {
    return batchGetTotalEntityCount(opContext, entityName, null);
  }

  /** List all urns existing for a particular Entity type. */
  ListUrnsResult listUrns(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityName,
      final int start,
      final int count)
      throws RemoteInvocationException;

  /** Hard delete an entity with a particular urn. */
  void deleteEntity(@Nonnull OperationContext opContext, @Nonnull final Urn urn)
      throws RemoteInvocationException;

  /** Delete all references to an entity with a particular urn. */
  void deleteEntityReferences(@Nonnull OperationContext opContext, @Nonnull final Urn urn)
      throws RemoteInvocationException;

  /**
   * Filters entities based on a particular Filter and Sort criterion
   *
   * @param entity filter entity
   * @param filter search filters
   * @param sortCriteria sort criteria
   * @param start start offset for search results
   * @param count max number of search results requested
   * @return a set of {@link SearchResult}s
   * @throws RemoteInvocationException when unable to execute request
   */
  SearchResult filter(
      @Nonnull OperationContext opContext,
      @Nonnull String entity,
      @Nonnull Filter filter,
      List<SortCriterion> sortCriteria,
      int start,
      int count)
      throws RemoteInvocationException;

  /**
   * Checks whether an entity with a given urn exists
   *
   * @param urn the urn of the entity
   * @return true if an entity exists, i.e. there are > 0 aspects in the DB for the entity. This
   *     means that the entity has not been hard-deleted.
   * @throws RemoteInvocationException when unable to execute request
   */
  boolean exists(@Nonnull OperationContext opContext, @Nonnull Urn urn)
      throws RemoteInvocationException;

  /**
   * Checks whether an entity with a given urn exists
   *
   * @param urn the urn of the entity
   * @param includeSoftDelete whether to consider soft deletion.
   * @return true if an entity exists, i.e. there are > 0 aspects in the DB for the entity. This
   *     means that the entity has not been hard-deleted when includeSoftDelete is true. Else will
   *     check for value of the status aspect
   * @throws RemoteInvocationException when unable to execute request
   */
  boolean exists(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull Boolean includeSoftDelete)
      throws RemoteInvocationException;

  @Nullable
  @Deprecated
  VersionedAspect getAspect(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version)
      throws RemoteInvocationException;

  @Nullable
  @Deprecated
  VersionedAspect getAspectOrNull(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version)
      throws RemoteInvocationException;

  default List<EnvelopedAspect> getTimeseriesAspectValues(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis,
      @Nullable Integer limit,
      @Nullable Filter filter)
      throws RemoteInvocationException {
    return getTimeseriesAspectValues(
        opContext, urn, entity, aspect, startTimeMillis, endTimeMillis, limit, filter, null);
  }

  List<EnvelopedAspect> getTimeseriesAspectValues(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis,
      @Nullable Integer limit,
      @Nullable Filter filter,
      @Nullable SortCriterion sort)
      throws RemoteInvocationException;

  @Deprecated
  default String ingestProposal(
      @Nonnull OperationContext opContext,
      @Nonnull final MetadataChangeProposal metadataChangeProposal)
      throws RemoteInvocationException {
    return ingestProposal(opContext, metadataChangeProposal, false);
  }

  /**
   * Ingest a MetadataChangeProposal event.
   *
   * @return the urn string ingested
   */
  @Nullable
  default String ingestProposal(
      @Nonnull OperationContext opContext,
      @Nonnull final MetadataChangeProposal metadataChangeProposal,
      final boolean async)
      throws RemoteInvocationException {
    return batchIngestProposals(opContext, List.of(metadataChangeProposal), async).stream()
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(null);
  }

  @Deprecated
  default List<String> batchIngestProposals(
      @Nonnull OperationContext opContext,
      @Nonnull final Collection<MetadataChangeProposal> metadataChangeProposals)
      throws RemoteInvocationException {
    return batchIngestProposals(opContext, metadataChangeProposals, false);
  }

  /**
   * Ingest a list of proposals in a batch.
   *
   * @param opContext operation context
   * @param metadataChangeProposals list of proposals
   * @param async async or sync ingestion path
   * @return ingested urns
   */
  @Nonnull
  List<String> batchIngestProposals(
      @Nonnull OperationContext opContext,
      @Nonnull final Collection<MetadataChangeProposal> metadataChangeProposals,
      final boolean async)
      throws RemoteInvocationException;

  @Deprecated
  <T extends RecordTemplate> Optional<T> getVersionedAspect(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version,
      @Nonnull Class<T> aspectClass)
      throws RemoteInvocationException;

  @Deprecated
  DataMap getRawAspect(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version)
      throws RemoteInvocationException;

  void producePlatformEvent(
      @Nonnull OperationContext opContext,
      @Nonnull String name,
      @Nullable String key,
      @Nonnull PlatformEvent event)
      throws Exception;

  void rollbackIngestion(
      @Nonnull OperationContext opContext, @Nonnull String runId, @Nonnull Authorizer authorizer)
      throws Exception;

  @Nullable
  default Aspect getLatestAspectObject(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nullable Boolean alwaysIncludeKeyAspect)
      throws RemoteInvocationException, URISyntaxException {
    return getLatestAspects(opContext, Set.of(urn), Set.of(aspectName), alwaysIncludeKeyAspect)
        .getOrDefault(urn, Map.of())
        .get(aspectName);
  }

  @Nonnull
  default Map<Urn, Map<String, Aspect>> getLatestAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> urns,
      @Nonnull Set<String> aspectNames,
      @Nullable Boolean alwaysIncludeKeyAspect)
      throws RemoteInvocationException, URISyntaxException {
    String entityName = urns.stream().findFirst().map(Urn::getEntityType).get();
    return entityResponseToAspectMap(
        batchGetV2(opContext, entityName, urns, aspectNames, alwaysIncludeKeyAspect));
  }

  @Nonnull
  default Map<Urn, Map<String, SystemAspect>> getLatestSystemAspect(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> urns,
      @Nonnull Set<String> aspectNames,
      @Nullable Boolean alwaysIncludeKeyAspect)
      throws RemoteInvocationException, URISyntaxException {
    String entityName = urns.stream().findFirst().map(Urn::getEntityType).get();
    return entityResponseToSystemAspectMap(
        batchGetV2(opContext, entityName, urns, aspectNames, alwaysIncludeKeyAspect),
        opContext.getEntityRegistry());
  }
}

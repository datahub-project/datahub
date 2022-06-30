package com.linkedin.entity.client;

import com.datahub.authentication.Authentication;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Entity;
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
import com.linkedin.r2.RemoteInvocationException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

// Consider renaming this to datahub client.
public interface EntityClient {

  @Nullable
  public EntityResponse getV2(
      @Nonnull String entityName,
      @Nonnull final Urn urn,
      @Nullable final Set<String> aspectNames,
      @Nonnull final Authentication authentication) throws RemoteInvocationException, URISyntaxException;

  @Nonnull
  @Deprecated
  public Entity get(@Nonnull final Urn urn, @Nonnull final Authentication authentication)
      throws RemoteInvocationException;

  @Nonnull
  public Map<Urn, EntityResponse> batchGetV2(
      @Nonnull String entityName,
      @Nonnull final Set<Urn> urns,
      @Nullable final Set<String> aspectNames,
      @Nonnull final Authentication authentication) throws RemoteInvocationException, URISyntaxException;

  @Nonnull
  Map<Urn, EntityResponse> batchGetVersionedV2(
      @Nonnull String entityName,
      @Nonnull final Set<VersionedUrn> versionedUrns,
      @Nullable final Set<String> aspectNames,
      @Nonnull final Authentication authentication) throws RemoteInvocationException, URISyntaxException;

  @Nonnull
  @Deprecated
  public Map<Urn, Entity> batchGet(@Nonnull final Set<Urn> urns, @Nonnull final Authentication authentication)
      throws RemoteInvocationException;

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
  public AutoCompleteResult autoComplete(@Nonnull String entityType, @Nonnull String query,
      @Nonnull Map<String, String> requestFilters, @Nonnull int limit, @Nullable String field,
      @Nonnull Authentication authentication) throws RemoteInvocationException;

  /**
   * Gets browse snapshot of a given path
   *
   * @param query search query
   * @param requestFilters autocomplete filters
   * @param limit max number of autocomplete results
   * @throws RemoteInvocationException
   */
  @Nonnull
  public AutoCompleteResult autoComplete(@Nonnull String entityType, @Nonnull String query,
      @Nonnull Map<String, String> requestFilters, @Nonnull int limit, @Nonnull Authentication authentication)
      throws RemoteInvocationException;

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
      @Nullable Map<String, String> requestFilters, int start, int limit, @Nonnull Authentication authentication)
      throws RemoteInvocationException;

  @Deprecated
  public void update(@Nonnull final Entity entity, @Nonnull final Authentication authentication)
      throws RemoteInvocationException;

  @Deprecated
  public void updateWithSystemMetadata(@Nonnull final Entity entity, @Nullable final SystemMetadata systemMetadata,
      @Nonnull final Authentication authentication) throws RemoteInvocationException;

  @Deprecated
  public void batchUpdate(@Nonnull final Set<Entity> entities, @Nonnull final Authentication authentication)
      throws RemoteInvocationException;

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
      @Nullable Map<String, String> requestFilters, int start, int count, @Nonnull Authentication authentication)
      throws RemoteInvocationException;

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
      @Nonnull Authentication authentication) throws RemoteInvocationException;

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
      SortCriterion sortCriterion, int start, int count, @Nonnull Authentication authentication) throws RemoteInvocationException;

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
      @Nullable Filter filter, int start, int count, @Nonnull Authentication authentication)
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
   * @param sortCriterion {@link SortCriterion} to be applied to search results
   * @param start index to start the search from
   * @param count the number of search hits to return
   * @return a {@link SearchResult} that contains a list of matched documents and related search result metadata
   */
  @Nonnull
  public LineageSearchResult searchAcrossLineage(@Nonnull Urn sourceUrn, @Nonnull LineageDirection direction,
      @Nonnull List<String> entities, @Nonnull String input, @Nullable Integer maxHops, @Nullable Filter filter,
      @Nullable SortCriterion sortCriterion, int start, int count, @Nonnull final Authentication authentication)
      throws RemoteInvocationException;

  /**
   * Gets browse path(s) given dataset urn
   *
   * @param urn urn for the entity
   * @return list of paths given urn
   * @throws RemoteInvocationException
   */
  @Nonnull
  public StringArray getBrowsePaths(@Nonnull Urn urn, @Nonnull Authentication authentication)
      throws RemoteInvocationException;

  public void setWritable(boolean canWrite, @Nonnull Authentication authentication) throws RemoteInvocationException;

  @Nonnull
  public Map<String, Long> batchGetTotalEntityCount(@Nonnull List<String> entityName,
      @Nonnull Authentication authentication) throws RemoteInvocationException;

  /**
   * List all urns existing for a particular Entity type.
   */
  public ListUrnsResult listUrns(@Nonnull final String entityName, final int start, final int count,
      @Nonnull final Authentication authentication) throws RemoteInvocationException;

  /**
   * Hard delete an entity with a particular urn.
   */
  public void deleteEntity(@Nonnull final Urn urn, @Nonnull final Authentication authentication)
      throws RemoteInvocationException;

  /**
   * Delete all references to an entity with a particular urn.
   */
  public void deleteEntityReferences(@Nonnull final Urn urn, @Nonnull final Authentication authentication)
      throws RemoteInvocationException;

  /**
   * Filters entities based on a particular Filter and Sort criterion
   *
   * @param entity filter entity
   * @param filter search filters
   * @param sortCriterion sort criterion
   * @param start start offset for search results
   * @param count max number of search results requested
   * @return a set of {@link SearchResult}s
   * @throws RemoteInvocationException
   */
  @Nonnull
  public SearchResult filter(@Nonnull String entity, @Nonnull Filter filter, @Nullable SortCriterion sortCriterion,
      int start, int count, @Nonnull Authentication authentication) throws RemoteInvocationException;

  /**
   * Checks whether an entity with a given urn exists
   *
   * @param urn the urn of the entity
   * @return true if an entity exists, i.e. there are > 0 aspects in the DB for the entity. This means that the entity
   * has not been hard-deleted.
   * @throws RemoteInvocationException
   */
  @Nonnull
  public boolean exists(@Nonnull Urn urn, @Nonnull Authentication authentication) throws RemoteInvocationException;

  @Nullable
  @Deprecated
  public VersionedAspect getAspect(@Nonnull String urn, @Nonnull String aspect, @Nonnull Long version,
      @Nonnull Authentication authentication) throws RemoteInvocationException;

  @Nullable
  @Deprecated
  public VersionedAspect getAspectOrNull(@Nonnull String urn, @Nonnull String aspect, @Nonnull Long version,
      @Nonnull Authentication authentication) throws RemoteInvocationException;

  public List<EnvelopedAspect> getTimeseriesAspectValues(@Nonnull String urn, @Nonnull String entity,
      @Nonnull String aspect, @Nullable Long startTimeMillis, @Nullable Long endTimeMillis, @Nullable Integer limit,
      @Nonnull Boolean getLatestValue, @Nullable Filter filter, @Nonnull Authentication authentication)
      throws RemoteInvocationException;

  public String ingestProposal(@Nonnull final MetadataChangeProposal metadataChangeProposal,
      @Nonnull final Authentication authentication) throws RemoteInvocationException;

  default String wrappedIngestProposal(@Nonnull MetadataChangeProposal metadataChangeProposal,
      @Nonnull final Authentication authentication) {
    try {
      return ingestProposal(metadataChangeProposal, authentication);
    } catch (RemoteInvocationException e) {
      throw new RuntimeException(e);
    }
  }

  default List<String> batchIngestProposals(@Nonnull final Collection<MetadataChangeProposal> metadataChangeProposals,
      @Nonnull final Authentication authentication) throws RemoteInvocationException {
    return metadataChangeProposals.stream()
        .map(proposal -> wrappedIngestProposal(proposal, authentication))
        .collect(Collectors.toList());
  }

  @Nonnull
  @Deprecated
  public <T extends RecordTemplate> Optional<T> getVersionedAspect(@Nonnull String urn, @Nonnull String aspect,
      @Nonnull Long version, @Nonnull Class<T> aspectClass, @Nonnull Authentication authentication)
      throws RemoteInvocationException;

  @Deprecated
  public DataMap getRawAspect(@Nonnull String urn, @Nonnull String aspect, @Nonnull Long version,
      @Nonnull Authentication authentication) throws RemoteInvocationException;

  public void producePlatformEvent(@Nonnull String name, @Nullable String key, @Nonnull PlatformEvent event,
      @Nonnull Authentication authentication) throws Exception;
}

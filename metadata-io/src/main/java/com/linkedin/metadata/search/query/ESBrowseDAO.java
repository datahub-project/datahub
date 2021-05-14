package com.linkedin.metadata.search.query;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.Filter;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class ESBrowseDAO {
  /**
   * Gets a list of groups/entities that match given browse request.
   *
   * @param entityName type of entity to query
   * @param path the path to be browsed
   * @param requestParams the request map with fields and values as filters
   * @param from index of the first entity located in path
   * @param size the max number of entities contained in the response
   * @return a {@link BrowseResult} that contains a list of groups/entities
   */
  public BrowseResult browse(@Nonnull String entityName, @Nonnull String path, @Nullable Filter requestParams, int from, int size) {
    return new BrowseResult();
  }

  /**
   * Gets a list of paths for a given urn.
   *
   * @param entityName type of entity to query
   * @param urn urn of the entity
   * @return all paths related to a given urn
   */
  @Nonnull
  public List<String> getBrowsePaths(@Nonnull String entityName, @Nonnull Urn urn) {
    return ImmutableList.of();
  }
}

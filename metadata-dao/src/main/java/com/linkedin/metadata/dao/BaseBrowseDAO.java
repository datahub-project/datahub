package com.linkedin.metadata.dao;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.Filter;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * A base class for all Browse DAOs.
 *
 * A browse DAO is a standardized interface to browse metadata.
 * See http://go/gma for more details.
 */
public abstract class BaseBrowseDAO {

  /**
   * Gets a list of groups/entities that match given browse request
   *
   * @param path the path to be browsed
   * @param requestParams the request map with fields and values as filters
   * @param from index of the first entity located in path
   * @param size the max number of entities contained in the response
   * @return a {@link BrowseResult} that contains a list of groups/entities
   */
  @Nonnull
  public abstract BrowseResult browse(@Nonnull String path, @Nullable Filter requestParams, int from, int size);

  /**
   * Gets a list of paths for a given urn
   *
   * @param urn urn of the entity
   * @return all paths related to a given urn
   */
  @Nonnull
  public abstract List<String> getBrowsePaths(@Nonnull Urn urn);
}

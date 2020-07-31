package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Base client that all entities supporting browse as well as search should implement in their respective restli MPs
 * @param <VALUE> the client's value type
 * @param <URN> urn type of the entity
 */
public abstract class BaseBrowsableClient<VALUE extends RecordTemplate, URN extends Urn> extends BaseSearchableClient<VALUE> {

  public BaseBrowsableClient(@Nonnull Client restliClient) {
    super(restliClient);
  }

  /**
   * Browse method that the client extending this class must implement. Returns {@link BrowseResult} containing list of groups/entities
   * that match given browse request
   *
   * @param inputPath Path to be browsed
   * @param requestFilters Request map with fields and values to be applied as filters to the browse query
   * @param from Start index of the first entity located in path
   * @param size The max number of entities contained in the response
   * @return {@link BrowseResult} containing list of groups/entities
   * @throws RemoteInvocationException when the rest.li request fails
   */
  @Nonnull
  public abstract BrowseResult browse(@Nonnull String inputPath, @Nullable Map<String, String> requestFilters, int from, int size)
      throws RemoteInvocationException;

  /**
   * Returns a list of paths for a given urn
   *
   * @param urn Urn of the entity
   * @return all paths that are related to the urn
   * @throws RemoteInvocationException when the rest.li request fails
   */
  @Nonnull
  public StringArray getBrowsePaths(@Nonnull URN urn) throws RemoteInvocationException {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

}
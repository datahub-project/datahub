package com.linkedin.datahub.graphql.types.aspect;

import com.linkedin.datahub.graphql.AspectLoadKey;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Aspect;
import graphql.execution.DataFetcherResult;
import java.util.List;
import javax.annotation.Nonnull;


public class AspectType {
  /**
   * Retrieves an list of entities given a list of urn strings. The list returned is expected to
   * be of same length of the list of urns, where nulls are provided in place of an entity object if an entity cannot be found.
   * @param keys to retrieve
   * @param context the {@link QueryContext} corresponding to the request.
   */
  public List<DataFetcherResult<Aspect>> batchLoad(@Nonnull List<AspectLoadKey> keys, @Nonnull QueryContext context) throws Exception {
    return null;
  }
}

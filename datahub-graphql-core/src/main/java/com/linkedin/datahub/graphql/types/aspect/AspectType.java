package com.linkedin.datahub.graphql.types.aspect;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Aspects;
import com.linkedin.datahub.graphql.types.LoadableType;
import java.util.List;
import javax.annotation.Nonnull;


public class AspectType implements LoadableType {

  /**
   * Returns generated GraphQL class associated with the type
   */
  @Override
  public Class objectClass() {
    return Aspects.class;
  }

  /**
   * Retrieves an list of entities given a list of urn strings. The list returned is expected to
   * be of same length of the list of urns, where nulls are provided in place of an entity object if an entity cannot be found.
   *  @param urns to retrieve
   * @param context the {@link QueryContext} corresponding to the request.
   */
  @Override
  public List batchLoad(@Nonnull List urns, @Nonnull QueryContext context) throws Exception {
    return null;
  }
}

package com.linkedin.datahub.graphql.types.entity;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.metadata.entity.EntityService;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * {@link LoadableType} that resolves whether a batch of entity urns exist. Backed by {@link
 * EntityService#exists}, which already accepts a set of urns, so multiple {@code exists} fields in
 * a single request are coalesced into one existence check by the DataLoader.
 */
public class EntityExistsType implements LoadableType<Boolean, String> {

  private final EntityService<?> _entityService;

  public EntityExistsType(final EntityService<?> entityService) {
    _entityService = entityService;
  }

  @Override
  public Class<Boolean> objectClass() {
    return Boolean.class;
  }

  @Override
  public String name() {
    return EntityExistsType.class.getSimpleName();
  }

  @Override
  public List<DataFetcherResult<Boolean>> batchLoad(
      @Nonnull final List<String> keys, @Nonnull final QueryContext context) throws Exception {
    final List<Urn> entityUrns = new ArrayList<>();
    for (String key : keys) {
      entityUrns.add(Urn.createFromString(key));
    }

    final Set<Urn> existing =
        _entityService.exists(context.getOperationContext(), Set.copyOf(entityUrns));
    return entityUrns.stream()
        .map(
            entityUrn ->
                DataFetcherResult.<Boolean>newResult().data(existing.contains(entityUrn)).build())
        .toList();
  }
}

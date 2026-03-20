package com.linkedin.datahub.graphql.types.entitytype;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.metadata.entity.EntityService;
import graphql.execution.DataFetcherResult;
import java.util.*;
import javax.annotation.Nonnull;

public class EntityExistsType implements LoadableType<Boolean, String> {
  private final EntityService _entityService;

  public EntityExistsType(EntityService entityService) {
    _entityService = entityService;
  }

  @Override
  public Class<Boolean> objectClass() {
    return Boolean.class;
  }

  @Override
  public String name() {
    return "ENTITY_EXISTS";
  }

  @Override
  public List<DataFetcherResult<Boolean>> batchLoad(
      @Nonnull List<String> urnStrs, @Nonnull QueryContext context) {

    try {
      // Convert string URNs to Urn objects (parse once, maintain order)
      final List<Urn> parsedUrns = new ArrayList<>();
      final Set<Urn> urnSet = new HashSet<>();
      for (final String urnStr : urnStrs) {
        Urn urn = Urn.createFromString(urnStr);
        parsedUrns.add(urn);
        urnSet.add(urn);
      }

      // Batch load existence status for all URNs in one call
      final Set<Urn> existingUrns = _entityService.exists(context.getOperationContext(), urnSet);

      List<DataFetcherResult<Boolean>> res1 =
          parsedUrns.stream()
              .map(
                  urn ->
                      DataFetcherResult.<Boolean>newResult()
                          .data(existingUrns.contains(urn))
                          .build())
              .toList();
      return res1;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to batch load entity existence for urns: %s", urnStrs), e);
    }
  }
}

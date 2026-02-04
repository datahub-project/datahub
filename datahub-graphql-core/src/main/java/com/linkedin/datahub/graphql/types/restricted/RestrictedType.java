package com.linkedin.datahub.graphql.types.restricted;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.Restricted;
import com.linkedin.datahub.graphql.types.EntityType;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * GraphQL type for Restricted entities. Restricted entities are placeholders for entities that the
 * user doesn't have permission to view. They preserve the encrypted URN and show as "Restricted" in
 * the UI.
 *
 * <p>Unlike other entity types, we don't need to fetch from the database - we simply return
 * Restricted entities with the original encrypted URNs. The restricted URN was created specifically
 * because the user doesn't have permission to view the underlying entity.
 */
public class RestrictedType implements EntityType<Restricted, String> {

  @Override
  public com.linkedin.datahub.graphql.generated.EntityType type() {
    return com.linkedin.datahub.graphql.generated.EntityType.RESTRICTED;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<Restricted> objectClass() {
    return Restricted.class;
  }

  /**
   * Override batchLoad to skip authorization checks. Restricted entities are already placeholders
   * for entities the user doesn't have permission to view - no additional auth check needed.
   */
  @Override
  public List<DataFetcherResult<Restricted>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    return batchLoadWithoutAuthorization(urns, context);
  }

  /**
   * Batch load restricted entities.
   *
   * <p>We preserve the original encrypted URN (don't re-encrypt) to ensure consistency between the
   * URN used in lineage relationships and the URN returned when fetching the entity.
   */
  @Override
  public List<DataFetcherResult<Restricted>> batchLoadWithoutAuthorization(
      @Nonnull List<String> urns, @Nonnull QueryContext context) {
    final List<Urn> restrictedUrns =
        urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    // Build results, preserving the ORIGINAL restricted URN (don't re-encrypt)
    final List<DataFetcherResult<Restricted>> results = new ArrayList<>();
    for (Urn originalRestrictedUrn : restrictedUrns) {
      Restricted restricted = new Restricted();
      restricted.setUrn(originalRestrictedUrn.toString());
      restricted.setType(com.linkedin.datahub.graphql.generated.EntityType.RESTRICTED);
      results.add(DataFetcherResult.<Restricted>newResult().data(restricted).build());
    }
    return results;
  }
}

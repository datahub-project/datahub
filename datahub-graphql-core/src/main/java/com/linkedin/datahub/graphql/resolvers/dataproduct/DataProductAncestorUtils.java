package com.linkedin.datahub.graphql.resolvers.dataproduct;

import com.linkedin.common.urn.Urn;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Shared helpers for walking the Data Product parent chain. Used by parent-list resolvers and move
 * cycle guards.
 */
public final class DataProductAncestorUtils {

  public static final int MAX_DEPTH = 20;

  private static final Set<String> ASPECTS_TO_FETCH =
      Collections.singleton(Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME);

  private DataProductAncestorUtils() {}

  /**
   * Walks the parentDataProduct chain starting from {@code sourceUrn}. Returns ancestors in
   * nearest-first order (immediate parent first, root last). Guards against cycles with a visited
   * set and a depth cap.
   */
  @Nonnull
  public static List<Urn> walkParentChain(
      @Nonnull final OperationContext opContext,
      @Nonnull final EntityClient entityClient,
      @Nonnull final Urn sourceUrn)
      throws Exception {
    final List<Urn> chain = new ArrayList<>();
    final Set<Urn> visited = new HashSet<>();
    visited.add(sourceUrn);

    Urn current = resolveParent(opContext, entityClient, sourceUrn);
    while (current != null && !visited.contains(current) && chain.size() < MAX_DEPTH) {
      chain.add(current);
      visited.add(current);
      current = resolveParent(opContext, entityClient, current);
    }
    return chain;
  }

  @Nullable
  public static Urn resolveParent(
      @Nonnull final OperationContext opContext,
      @Nonnull final EntityClient entityClient,
      @Nonnull final Urn urn)
      throws Exception {
    final Map<Urn, EntityResponse> responses =
        entityClient.batchGetV2(
            opContext,
            Constants.DATA_PRODUCT_ENTITY_NAME,
            Collections.singleton(urn),
            ASPECTS_TO_FETCH,
            false);
    final EntityResponse response = responses.get(urn);
    if (response == null) {
      return null;
    }
    final EnvelopedAspect enveloped =
        response.getAspects().get(Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
    if (enveloped == null) {
      return null;
    }
    final DataProductProperties props = new DataProductProperties(enveloped.getValue().data());
    return props.hasParentDataProduct() ? props.getParentDataProduct() : null;
  }
}

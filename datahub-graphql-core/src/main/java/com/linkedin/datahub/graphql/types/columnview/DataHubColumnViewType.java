package com.linkedin.datahub.graphql.types.columnview;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubColumnView;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.resolvers.columnview.ColumnViewUtils;
import com.linkedin.datahub.graphql.util.AspectUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.view.DataHubColumnViewInfo;
import graphql.execution.DataFetcherResult;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

/** GraphQL entity type for {@code dataHubColumnView}. Mirrors {@code DataHubViewType}. */
@RequiredArgsConstructor
public class DataHubColumnViewType
    implements com.linkedin.datahub.graphql.types.EntityType<DataHubColumnView, String> {
  public static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(DATAHUB_COLUMN_VIEW_INFO_ASPECT_NAME);
  private final EntityClient _entityClient;

  @Override
  public EntityType type() {
    return EntityType.DATAHUB_COLUMN_VIEW;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<DataHubColumnView> objectClass() {
    return DataHubColumnView.class;
  }

  @Override
  public List<DataFetcherResult<DataHubColumnView>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> viewUrns = urns.stream().map(this::getUrn).collect(Collectors.toList());

    try {
      Set<String> aspectsToResolve =
          AspectUtils.getOptimizedAspects(
              context, name(), ASPECTS_TO_FETCH, DATAHUB_COLUMN_VIEW_KEY_ASPECT_NAME);
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              DATAHUB_COLUMN_VIEW_ENTITY_NAME,
              viewUrns.stream()
                  .filter(urn -> canView(context.getOperationContext(), urn))
                  .collect(Collectors.toSet()),
              aspectsToResolve);

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : viewUrns) {
        final EntityResponse response = entities.getOrDefault(urn, null);
        // PERSONAL views resolve only for their creator (or global-view managers): a guessed urn
        // must not leak someone else's definition. Not readable reads as not found.
        gmsResults.add(response != null && isReadable(response, context) ? response : null);
      }
      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<DataHubColumnView>newResult()
                          .data(DataHubColumnViewMapper.map(context, gmsResult))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Column Views", e);
    }
  }

  private static boolean isReadable(
      @Nonnull final EntityResponse response, @Nonnull final QueryContext context) {
    final EnvelopedAspect aspect = response.getAspects().get(DATAHUB_COLUMN_VIEW_INFO_ASPECT_NAME);
    if (aspect == null) {
      return true; // key-only response; nothing to protect
    }
    return ColumnViewUtils.canReadColumnView(
        new DataHubColumnViewInfo(aspect.getValue().data()), context);
  }

  private Urn getUrn(final String urnStr) {
    try {
      return Urn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("Failed to convert urn string %s into Urn", urnStr));
    }
  }
}

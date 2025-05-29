package com.linkedin.datahub.graphql.types.ingestion;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * IngestionSourceType provides a way to load {@link IngestionSource} objects from their URNs. It
 * leverages the {@link EntityClient} to retrieve the entities from the GMS.
 */
@Slf4j
@RequiredArgsConstructor
public class IngestionSourceType
    implements com.linkedin.datahub.graphql.types.LoadableType<IngestionSource, String> {
  public static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(
          Constants.INGESTION_SOURCE_ENTITY_NAME,
          Constants.INGESTION_INFO_ASPECT_NAME,
          Constants.INGESTION_SOURCE_KEY_ASPECT_NAME,
          Constants.OWNERSHIP_ASPECT_NAME);

  /**
   * Returns the class of the object that this type loads.
   *
   * @return the class of the object that this type loads.
   */
  @Override
  public Class<IngestionSource> objectClass() {
    return IngestionSource.class;
  }

  /**
   * Loads a batch of {@link IngestionSource} objects from their URNs.
   *
   * @param urns a list of URNs to load.
   * @param context the query context.
   * @return a list of {@link DataFetcherResult} objects containing the loaded {@link
   *     IngestionSource} objects.
   * @throws Exception if an error occurs while loading the entities.
   */
  @Override
  public List<DataFetcherResult<IngestionSource>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> ingestionSourceUrns =
        urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              Constants.INGESTION_SOURCE_ENTITY_NAME,
              new HashSet<>(ingestionSourceUrns),
              ASPECTS_TO_FETCH);

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : ingestionSourceUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<IngestionSource>newResult()
                          .data(IngestionSourceMapper.map(context, gmsResult))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Ingestion sources", e);
    }
  }

  private final EntityClient _entityClient;
}

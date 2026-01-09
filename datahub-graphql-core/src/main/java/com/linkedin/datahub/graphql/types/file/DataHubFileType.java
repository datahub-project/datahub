package com.linkedin.datahub.graphql.types.file;

import static com.linkedin.metadata.Constants.DATAHUB_FILE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATAHUB_FILE_INFO_ASPECT_NAME;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubFile;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DataHubFileType
    implements com.linkedin.datahub.graphql.types.EntityType<DataHubFile, String> {
  public static final Set<String> ASPECTS_TO_FETCH = ImmutableSet.of(DATAHUB_FILE_INFO_ASPECT_NAME);
  private final EntityClient _entityClient;

  @Override
  public EntityType type() {
    return EntityType.DATAHUB_FILE;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<DataHubFile> objectClass() {
    return DataHubFile.class;
  }

  @Override
  public List<DataFetcherResult<DataHubFile>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> fileUrns = urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              DATAHUB_FILE_ENTITY_NAME,
              new HashSet<>(fileUrns),
              ASPECTS_TO_FETCH);

      final List<EntityResponse> gmsResults = new ArrayList<>(urns.size());
      for (Urn urn : fileUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<DataHubFile>newResult()
                          .data(DataHubFileMapper.map(context, gmsResult))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load DataHub Files", e);
    }
  }
}

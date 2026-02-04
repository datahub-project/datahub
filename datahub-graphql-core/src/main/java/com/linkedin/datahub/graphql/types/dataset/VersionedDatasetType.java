package com.linkedin.datahub.graphql.types.dataset;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.VersionedDataset;
import com.linkedin.datahub.graphql.types.dataset.mappers.VersionedDatasetMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class VersionedDatasetType
    implements com.linkedin.datahub.graphql.types.EntityType<VersionedDataset, VersionedUrn> {

  private static final Set<String> ASPECTS_TO_RESOLVE =
      ImmutableSet.of(
          DATASET_KEY_ASPECT_NAME,
          DATASET_PROPERTIES_ASPECT_NAME,
          EDITABLE_DATASET_PROPERTIES_ASPECT_NAME,
          DATASET_DEPRECATION_ASPECT_NAME, // This aspect is deprecated.
          DEPRECATION_ASPECT_NAME,
          DATASET_UPSTREAM_LINEAGE_ASPECT_NAME,
          UPSTREAM_LINEAGE_ASPECT_NAME,
          EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
          VIEW_PROPERTIES_ASPECT_NAME,
          OWNERSHIP_ASPECT_NAME,
          INSTITUTIONAL_MEMORY_ASPECT_NAME,
          GLOBAL_TAGS_ASPECT_NAME,
          GLOSSARY_TERMS_ASPECT_NAME,
          STATUS_ASPECT_NAME,
          CONTAINER_ASPECT_NAME,
          DOMAINS_ASPECT_NAME,
          SCHEMA_METADATA_ASPECT_NAME,
          SIBLINGS_ASPECT_NAME,
          DATA_PRODUCTS_ASPECT_NAME);

  private static final Set<String> FACET_FIELDS = ImmutableSet.of("origin", "platform");
  private static final String ENTITY_NAME = "dataset";

  private final EntityClient _entityClient;
  @Nullable private final RestrictedService _restrictedService;

  public VersionedDatasetType(final EntityClient entityClient) {
    this(entityClient, null);
  }

  public VersionedDatasetType(
      final EntityClient entityClient, @Nullable final RestrictedService restrictedService) {
    _entityClient = entityClient;
    _restrictedService = restrictedService;
  }

  @Override
  public Class<VersionedDataset> objectClass() {
    return VersionedDataset.class;
  }

  @Override
  public EntityType type() {
    return EntityType.DATASET;
  }

  @Override
  public Function<Entity, VersionedUrn> getKeyProvider() {
    return entity -> new VersionedUrn().setUrn(UrnUtils.getUrn(entity.getUrn()));
  }

  @Override
  public Function<VersionedUrn, Urn> getKeyToUrn() {
    return VersionedUrn::getUrn;
  }

  @Override
  public RestrictedService getRestrictedService() {
    return _restrictedService;
  }

  @Override
  public List<DataFetcherResult<VersionedDataset>> batchLoadWithoutAuthorization(
      @Nonnull final List<VersionedUrn> versionedUrns, @Nonnull final QueryContext context) {
    try {
      final Map<Urn, EntityResponse> datasetMap =
          _entityClient.batchGetVersionedV2(
              context.getOperationContext(),
              Constants.DATASET_ENTITY_NAME,
              Set.copyOf(versionedUrns),
              ASPECTS_TO_RESOLVE);

      return versionedUrns.stream()
          .map(
              versionedUrn -> {
                EntityResponse response = datasetMap.get(versionedUrn.getUrn());
                if (response == null) {
                  return null;
                }
                return DataFetcherResult.<VersionedDataset>newResult()
                    .data(VersionedDatasetMapper.map(context, response))
                    .build();
              })
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Datasets", e);
    }
  }
}

package com.linkedin.datahub.graphql.types.repository;

import static com.linkedin.metadata.Constants.BROWSE_PATHS_V2_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERMS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.REPOSITORY_ENTITY_NAME;
import static com.linkedin.metadata.Constants.REPOSITORY_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.REPOSITORY_LINEAGE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.REPOSITORY_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.REPOSITORY_SOURCE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SUB_TYPES_ASPECT_NAME;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Repository;
import com.linkedin.datahub.graphql.types.repository.mappers.RepositoryMapper;
import com.linkedin.datahub.graphql.util.AspectUtils;
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

/**
 * GraphQL type for the Repository entity: a source-code repository (GitHub, GitLab, internal SCM).
 * Needed wherever a Repository is returned as an {@link Entity}, for example as a lineage node
 * linked to a pipeline or task through repositoryLineage.
 */
@RequiredArgsConstructor
public class RepositoryType
    implements com.linkedin.datahub.graphql.types.EntityType<Repository, String> {

  public static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(
          REPOSITORY_PROPERTIES_ASPECT_NAME,
          REPOSITORY_SOURCE_ASPECT_NAME,
          REPOSITORY_LINEAGE_ASPECT_NAME,
          SUB_TYPES_ASPECT_NAME,
          DATA_PLATFORM_INSTANCE_ASPECT_NAME,
          OWNERSHIP_ASPECT_NAME,
          STATUS_ASPECT_NAME,
          GLOBAL_TAGS_ASPECT_NAME,
          GLOSSARY_TERMS_ASPECT_NAME,
          DOMAINS_ASPECT_NAME,
          INSTITUTIONAL_MEMORY_ASPECT_NAME,
          STRUCTURED_PROPERTIES_ASPECT_NAME,
          BROWSE_PATHS_V2_ASPECT_NAME);

  private final EntityClient _entityClient;

  @Override
  public EntityType type() {
    return EntityType.REPOSITORY;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<Repository> objectClass() {
    return Repository.class;
  }

  @Override
  public List<DataFetcherResult<Repository>> batchLoad(
      @Nonnull final List<String> urns, @Nonnull final QueryContext context) throws Exception {
    final List<Urn> repositoryUrns =
        urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());
    try {
      final Set<String> aspectsToResolve =
          AspectUtils.getOptimizedAspects(
              context, name(), ASPECTS_TO_FETCH, REPOSITORY_KEY_ASPECT_NAME);
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              REPOSITORY_ENTITY_NAME,
              new HashSet<>(repositoryUrns),
              aspectsToResolve);

      final List<EntityResponse> gmsResults = new ArrayList<>(urns.size());
      for (Urn urn : repositoryUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<Repository>newResult()
                          .data(RepositoryMapper.map(context, gmsResult))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Repositories", e);
    }
  }
}

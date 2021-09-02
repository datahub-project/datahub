package com.linkedin.datahub.graphql.resolvers.mutate;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.LabelUpdateInput;
import com.linkedin.datahub.graphql.types.EntityType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


public class AddLabelResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private static final Logger _logger = LoggerFactory.getLogger(MutableTypeResolver.class.getName());
  private final Map<com.linkedin.datahub.graphql.generated.EntityType, EntityType<?>> _typeToEntity;

  public AddLabelResolver(List<EntityType<?>> entityTypes) {
    _typeToEntity = entityTypes.stream().collect(Collectors.toMap(
        EntityType::type,
        entity -> entity
    ));
  }

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final LabelUpdateInput input = bindArgument(environment.getArgument("input"), LabelUpdateInput.class);
    Urn targetUrn = Urn.createFromString(input.getTargetUrn());
    Urn labelUrn = Urn.createFromString(input.getLabelUrn());
    EntityType<?> entityType = _typeToEntity.get(targetUrn.getEntityType());
    Entity existingEntity = entityType.load(input.getTargetUrn(), environment.getContext()).getData();

    String methodName = LabelUtils.entityTypeToMethod.get(labelUrn.getEntityType());

    existingEntity.getClass().getMethod(methodName, null);

    return CompletableFuture.supplyAsync(() -> {
      try {
        _logger.debug(String.format("Adding Label. input: %s", input));
        return true;
      } catch (Exception e) {
        _logger.error(String.format("Failed to perform update against input %s", input.toString()) + " " + e.getMessage());
        throw new RuntimeException(String.format("Failed to perform update against input %s", input.toString()), e);
      }
    });
  }
}

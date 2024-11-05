package com.linkedin.datahub.graphql.resolvers.dimension;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateDimensionNameInput;
import com.linkedin.datahub.graphql.generated.DimensionNameEntity;
import com.linkedin.datahub.graphql.generated.DimensionNameInfo;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.metadata.service.DimensionTypeService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CreateDimensionNameResolver
    implements DataFetcher<CompletableFuture<DimensionNameEntity>> {

  private final DimensionTypeService _dimensionTypeService;

  @Override
  public CompletableFuture<DimensionNameEntity> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final CreateDimensionNameInput input =
        bindArgument(environment.getArgument("input"), CreateDimensionNameInput.class);

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            final Urn urn =
                _dimensionTypeService.createDimensionName(
                    context.getOperationContext(),
                    input.getName(),
                    input.getDescription(),
                    System.currentTimeMillis());
            return createDimensionName(urn, input);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        });
  }

  private DimensionNameEntity createDimensionName(
      @Nonnull final Urn urn, @Nonnull final CreateDimensionNameInput input) {
    return DimensionNameEntity.builder()
        .setUrn(urn.toString())
        .setType(EntityType.DIMENSION_NAME)
        .setInfo(new DimensionNameInfo(input.getName(), input.getDescription(), null, null))
        .build();
  }
}

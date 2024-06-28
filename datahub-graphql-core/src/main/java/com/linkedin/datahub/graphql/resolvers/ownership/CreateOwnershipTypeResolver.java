package com.linkedin.datahub.graphql.resolvers.ownership;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateOwnershipTypeInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.OwnershipTypeEntity;
import com.linkedin.datahub.graphql.generated.OwnershipTypeInfo;
import com.linkedin.metadata.service.OwnershipTypeService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CreateOwnershipTypeResolver
    implements DataFetcher<CompletableFuture<OwnershipTypeEntity>> {

  private final OwnershipTypeService _ownershipTypeService;

  @Override
  public CompletableFuture<OwnershipTypeEntity> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final CreateOwnershipTypeInput input =
        bindArgument(environment.getArgument("input"), CreateOwnershipTypeInput.class);

    if (!AuthorizationUtils.canManageOwnershipTypes(context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Urn urn =
                _ownershipTypeService.createOwnershipType(
                    context.getOperationContext(),
                    input.getName(),
                    input.getDescription(),
                    System.currentTimeMillis());
            return createOwnershipType(urn, input);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private OwnershipTypeEntity createOwnershipType(
      @Nonnull final Urn urn, @Nonnull final CreateOwnershipTypeInput input) {
    return OwnershipTypeEntity.builder()
        .setUrn(urn.toString())
        .setType(EntityType.CUSTOM_OWNERSHIP_TYPE)
        .setInfo(new OwnershipTypeInfo(input.getName(), input.getDescription(), null, null))
        .build();
  }
}

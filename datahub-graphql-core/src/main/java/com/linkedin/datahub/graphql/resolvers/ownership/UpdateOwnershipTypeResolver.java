package com.linkedin.datahub.graphql.resolvers.ownership;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.OwnershipTypeEntity;
import com.linkedin.datahub.graphql.generated.UpdateOwnershipTypeInput;
import com.linkedin.datahub.graphql.types.ownership.OwnershipTypeMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.service.OwnershipTypeService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UpdateOwnershipTypeResolver
    implements DataFetcher<CompletableFuture<OwnershipTypeEntity>> {

  private final OwnershipTypeService _ownershipTypeService;

  @Override
  public CompletableFuture<OwnershipTypeEntity> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final String urnStr = environment.getArgument("urn");
    final UpdateOwnershipTypeInput input =
        bindArgument(environment.getArgument("input"), UpdateOwnershipTypeInput.class);
    final Urn urn = Urn.createFromString(urnStr);

    if (!AuthorizationUtils.canManageOwnershipTypes(context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            _ownershipTypeService.updateOwnershipType(
                context.getOperationContext(),
                urn,
                input.getName(),
                input.getDescription(),
                System.currentTimeMillis());
            log.info(String.format("Successfully updated Ownership Type %s with urn", urn));
            return getOwnershipType(context, urn);
          } catch (AuthorizationException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform update against View with urn %s", urn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private OwnershipTypeEntity getOwnershipType(
      @Nullable QueryContext context, @Nonnull final Urn urn) {
    final EntityResponse maybeResponse =
        _ownershipTypeService.getOwnershipTypeEntityResponse(context.getOperationContext(), urn);
    // If there is no response, there is a problem.
    if (maybeResponse == null) {
      throw new RuntimeException(
          String.format(
              "Failed to perform update to Ownership Type with urn %s. Failed to find Ownership Type in GMS.",
              urn));
    }
    return OwnershipTypeMapper.map(context, maybeResponse);
  }
}

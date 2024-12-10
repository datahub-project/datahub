package com.linkedin.datahub.graphql.resolvers.dimension;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DimensionNameEntity;
import com.linkedin.datahub.graphql.generated.UpdateDimensionNameInput;
import com.linkedin.datahub.graphql.types.dimension.DimensionNameMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.service.DimensionTypeService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UpdateDimensionNameResolver
    implements DataFetcher<CompletableFuture<DimensionNameEntity>> {

  private final DimensionTypeService _dimensionTypeService;

  @Override
  public CompletableFuture<DimensionNameEntity> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final String urnStr = environment.getArgument("urn");
    final UpdateDimensionNameInput input =
        bindArgument(environment.getArgument("input"), UpdateDimensionNameInput.class);
    final Urn urn = Urn.createFromString(urnStr);

    // if (!AuthorizationUtils.canManageOwnershipTypes(context)) {
    //   throw new AuthorizationException(
    //       "Unauthorized to perform this action. Please contact your DataHub administrator.");
    // }

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            _dimensionTypeService.updateDimensionName(
                context.getOperationContext(),
                urn,
                input.getName(),
                input.getDescription(),
                System.currentTimeMillis());
            log.info(String.format("Successfully updated Dimension Name %s with urn", urn));
            return getDimensionName(
                context, urn, context.getOperationContext().getSessionAuthentication());
          } catch (AuthorizationException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform update against View with urn %s", urn), e);
          }
        });
  }

  private DimensionNameEntity getDimensionName(
      @Nullable QueryContext context,
      @Nonnull final Urn urn,
      @Nonnull final Authentication authentication) {
    final EntityResponse maybeResponse =
        _dimensionTypeService.getDimensionTypeEntityResponse(context.getOperationContext(), urn);
    // If there is no response, there is a problem.
    if (maybeResponse == null) {
      throw new RuntimeException(
          String.format(
              "Failed to perform update to Ownership Type with urn %s. Failed to find Ownership Type in GMS.",
              urn));
    }
    return DimensionNameMapper.map(context, maybeResponse);
  }
}

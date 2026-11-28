package com.linkedin.datahub.graphql.resolvers.customfabrictype;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.buildMetadataChangeProposalWithUrn;
import static com.linkedin.metadata.Constants.CUSTOM_FABRIC_TYPE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CUSTOM_FABRIC_TYPE_INFO_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.CustomFabricType;
import com.linkedin.datahub.graphql.generated.UpdateCustomFabricTypeInput;
import com.linkedin.datahub.graphql.types.customfabrictype.CustomFabricTypeMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.fabrictype.CustomFabricTypeInfo;
import com.linkedin.metadata.authorization.ApiOperation;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

/** Edits the display name or description of a custom environment; fields left null are kept. */
@RequiredArgsConstructor
public class UpdateCustomFabricTypeResolver
    implements DataFetcher<CompletableFuture<CustomFabricType>> {

  @Nonnull private final EntityClient entityClient;

  @Override
  public CompletableFuture<CustomFabricType> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateCustomFabricTypeInput input =
        bindArgument(environment.getArgument("input"), UpdateCustomFabricTypeInput.class);
    final Urn urn = UrnUtils.getUrn(input.getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // Authorize first so a caller without the privilege learns nothing about the URN's type.
          if (!AuthorizationUtils.canManageCustomFabricType(context, ApiOperation.UPDATE, urn)) {
            throw new AuthorizationException(
                "Unauthorized to edit environments. Please contact your DataHub administrator.");
          }
          if (!CUSTOM_FABRIC_TYPE_ENTITY_NAME.equals(urn.getEntityType())) {
            throw new DataHubGraphQLException(
                String.format("%s is not an environment.", urn),
                DataHubGraphQLErrorCode.BAD_REQUEST);
          }
          try {
            EntityResponse existing =
                entityClient.getV2(
                    context.getOperationContext(),
                    CUSTOM_FABRIC_TYPE_ENTITY_NAME,
                    urn,
                    Set.of(CUSTOM_FABRIC_TYPE_INFO_ASPECT_NAME));
            if (existing == null) {
              throw new DataHubGraphQLException(
                  String.format("%s does not exist.", urn), DataHubGraphQLErrorCode.NOT_FOUND);
            }
            CustomFabricTypeInfo info =
                existing.getAspects().containsKey(CUSTOM_FABRIC_TYPE_INFO_ASPECT_NAME)
                    ? new CustomFabricTypeInfo(
                        existing
                            .getAspects()
                            .get(CUSTOM_FABRIC_TYPE_INFO_ASPECT_NAME)
                            .getValue()
                            .data())
                    : new CustomFabricTypeInfo();
            if (input.getDisplayName() != null) {
              info.setDisplayName(input.getDisplayName());
            }
            if (input.getDescription() != null) {
              info.setDescription(input.getDescription());
            }
            entityClient.ingestProposal(
                context.getOperationContext(),
                buildMetadataChangeProposalWithUrn(urn, CUSTOM_FABRIC_TYPE_INFO_ASPECT_NAME, info),
                false);
            CustomFabricType result = CustomFabricTypeMapper.fromUrn(urn);
            result.setProperties(
                CustomFabricTypeMapper.properties(info.getDisplayName(), info.getDescription()));
            return result;
          } catch (DataHubGraphQLException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to update environment %s", urn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}

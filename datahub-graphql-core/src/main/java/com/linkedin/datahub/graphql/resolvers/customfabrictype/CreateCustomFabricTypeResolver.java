package com.linkedin.datahub.graphql.resolvers.customfabrictype;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.buildMetadataChangeProposalWithUrn;
import static com.linkedin.metadata.Constants.CUSTOM_FABRIC_TYPE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CUSTOM_FABRIC_TYPE_INFO_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.CreateCustomFabricTypeInput;
import com.linkedin.datahub.graphql.generated.CustomFabricType;
import com.linkedin.datahub.graphql.types.customfabrictype.CustomFabricTypeMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.fabrictype.CustomFabricTypeInfo;
import com.linkedin.metadata.authorization.ApiOperation;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

/**
 * Registers a custom environment. Whether the id is allowed, and whether custom environments are
 * enabled, is checked by CustomFabricTypeKeyValidator when the entity is written, for every API.
 */
@RequiredArgsConstructor
public class CreateCustomFabricTypeResolver
    implements DataFetcher<CompletableFuture<CustomFabricType>> {

  @Nonnull private final EntityClient entityClient;

  @Override
  public CompletableFuture<CustomFabricType> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final CreateCustomFabricTypeInput input =
        bindArgument(environment.getArgument("input"), CreateCustomFabricTypeInput.class);
    // createFromTuple doesn't parse, so a malformed id reaches the key validator and gets its
    // message rather than a URN syntax error.
    final Urn urn = Urn.createFromTuple(CUSTOM_FABRIC_TYPE_ENTITY_NAME, input.getId());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canManageCustomFabricType(context, ApiOperation.CREATE, urn)) {
            throw new AuthorizationException(
                "Unauthorized to create environments. Please contact your DataHub administrator.");
          }
          try {
            if (entityClient.exists(context.getOperationContext(), urn)) {
              throw new DataHubGraphQLException(
                  String.format("Environment %s already exists.", input.getId()),
                  DataHubGraphQLErrorCode.CONFLICT);
            }
            CustomFabricTypeInfo info =
                new CustomFabricTypeInfo()
                    .setDisplayName(input.getDisplayName(), SetMode.IGNORE_NULL)
                    .setDescription(input.getDescription(), SetMode.IGNORE_NULL);
            entityClient.ingestProposal(
                context.getOperationContext(),
                buildMetadataChangeProposalWithUrn(urn, CUSTOM_FABRIC_TYPE_INFO_ASPECT_NAME, info),
                false);
            CustomFabricType result = CustomFabricTypeMapper.fromUrn(urn);
            result.setProperties(
                CustomFabricTypeMapper.properties(input.getDisplayName(), input.getDescription()));
            return result;
          } catch (DataHubGraphQLException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to create environment %s: %s", input.getId(), e.getMessage()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}

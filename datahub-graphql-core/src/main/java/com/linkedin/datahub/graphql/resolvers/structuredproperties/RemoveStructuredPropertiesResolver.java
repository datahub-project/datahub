package com.linkedin.datahub.graphql.resolvers.structuredproperties;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.RemoveStructuredPropertiesInput;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.patch.builder.StructuredPropertiesPatchBuilder;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.structured.StructuredProperties;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

public class RemoveStructuredPropertiesResolver
    implements DataFetcher<
        CompletableFuture<com.linkedin.datahub.graphql.generated.StructuredProperties>> {

  private final EntityClient _entityClient;

  public RemoveStructuredPropertiesResolver(@Nonnull final EntityClient entityClient) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public CompletableFuture<com.linkedin.datahub.graphql.generated.StructuredProperties> get(
      final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    final RemoveStructuredPropertiesInput input =
        bindArgument(environment.getArgument("input"), RemoveStructuredPropertiesInput.class);
    final Urn assetUrn = UrnUtils.getUrn(input.getAssetUrn());

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            // check authorization first
            if (!AuthorizationUtils.canEditProperties(assetUrn, context)) {
              throw new AuthorizationException(
                  String.format(
                      "Not authorized to update properties on the gives urn %s", assetUrn));
            }

            if (!_entityClient.exists(context.getOperationContext(), assetUrn)) {
              throw new RuntimeException(
                  String.format("Asset with provided urn %s does not exist", assetUrn));
            }

            StructuredPropertiesPatchBuilder patchBuilder =
                new StructuredPropertiesPatchBuilder().urn(assetUrn);

            input
                .getStructuredPropertyUrns()
                .forEach(
                    propertyUrn -> {
                      patchBuilder.removeProperty(UrnUtils.getUrn(propertyUrn));
                    });

            // ingest change proposal
            final MetadataChangeProposal structuredPropertiesProposal = patchBuilder.build();

            _entityClient.ingestProposal(
                context.getOperationContext(), structuredPropertiesProposal, false);

            EntityResponse response =
                _entityClient.getV2(
                    context.getOperationContext(),
                    assetUrn.getEntityType(),
                    assetUrn,
                    ImmutableSet.of(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME));

            if (response == null
                || response.getAspects().get(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME) == null) {
              throw new RuntimeException(
                  String.format("Failed to fetch structured properties from entity %s", assetUrn));
            }

            StructuredProperties structuredProperties =
                new StructuredProperties(
                    response
                        .getAspects()
                        .get(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME)
                        .getValue()
                        .data());

            return StructuredPropertiesMapper.map(context, structuredProperties, assetUrn);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        });
  }
}

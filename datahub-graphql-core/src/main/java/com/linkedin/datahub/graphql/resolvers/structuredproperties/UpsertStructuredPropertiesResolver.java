package com.linkedin.datahub.graphql.resolvers.structuredproperties;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;

import com.datahub.authentication.Authentication;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.PropertyValueInput;
import com.linkedin.datahub.graphql.generated.UpsertStructuredPropertiesInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.StructuredPropertyUtils;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.aspect.patch.builder.StructuredPropertiesPatchBuilder;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import graphql.com.google.common.collect.ImmutableSet;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ReadPreference;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class UpsertStructuredPropertiesResolver
    implements DataFetcher<
        CompletableFuture<com.linkedin.datahub.graphql.generated.StructuredProperties>> {

  private final EntityClient _entityClient;

  public UpsertStructuredPropertiesResolver(@Nonnull final EntityClient entityClient) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public CompletableFuture<com.linkedin.datahub.graphql.generated.StructuredProperties> get(
      final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();

    final UpsertStructuredPropertiesInput input =
        bindArgument(environment.getArgument("input"), UpsertStructuredPropertiesInput.class);
    final Urn assetUrn = UrnUtils.getUrn(input.getAssetUrn());
    Map<String, List<PropertyValueInput>> updateMap = new HashMap<>();
    // create a map of updates from our input
    input
        .getStructuredPropertyInputParams()
        .forEach(param -> updateMap.put(param.getStructuredPropertyUrn(), param.getValues()));

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            // check authorization first
            final List<Urn> propertyUrns =
                updateMap.keySet().stream().map(UrnUtils::getUrn).collect(Collectors.toList());
            if (!AuthorizationUtils.canEditStructuredProperties(assetUrn, context, propertyUrns)) {
              throw new AuthorizationException(
                  String.format(
                      "Not authorized to update properties on the given urn %s", assetUrn));
            }

            final AuditStamp auditStamp =
                AuditStampUtils.createAuditStamp(authentication.getActor().toUrnStr());

            // schemaField entities often don't exist, create it if upserting on a schema field
            if (!assetUrn.getEntityType().equals(SCHEMA_FIELD_ENTITY_NAME)
                && !_entityClient.exists(
                    context.getOperationContext().withReadPreference(ReadPreference.PRIMARY),
                    assetUrn)) {
              throw new RuntimeException(
                  String.format("Asset with provided urn %s does not exist", assetUrn));
            }

            // An empty input names no property. Return the current aspect without writing it back:
            // a rewrite would bump the version and reindex an asset the caller did not change.
            if (propertyUrns.isEmpty()) {
              return StructuredPropertiesMapper.map(
                  context,
                  getStructuredProperties(context.getOperationContext(), assetUrn),
                  assetUrn);
            }

            // Write only the given properties, as a patch. GMS applies it to the latest aspect
            // inside the write transaction, so a concurrent write to another property of the same
            // entity is kept. Upserting the whole aspect from an earlier read would overwrite it.
            final StructuredPropertiesPatchBuilder patchBuilder =
                new StructuredPropertiesPatchBuilder().urn(assetUrn);

            // A manual value replaces the attributed values of that property (for example
            // propagated ones), as the whole-aspect upsert did. The patch keys assignments by
            // property and attribution source, so remove them in the same patch: a second
            // assignment for the property would fail StructuredPropertiesValidator.
            getStructuredProperties(context.getOperationContext(), assetUrn)
                .getProperties()
                .stream()
                .filter(assignment -> updateMap.containsKey(assignment.getPropertyUrn().toString()))
                .filter(
                    assignment ->
                        assignment.hasAttribution() && assignment.getAttribution().hasSource())
                .forEach(
                    assignment ->
                        patchBuilder.removeProperty(
                            assignment.getPropertyUrn(), assignment.getAttribution().getSource()));

            updateMap.forEach(
                (propertyUrn, values) ->
                    patchBuilder.setProperty(
                        UrnUtils.getUrn(propertyUrn),
                        new PrimitivePropertyValueArray(
                            values.stream()
                                .map(StructuredPropertyUtils::mapPropertyValueInput)
                                .collect(Collectors.toList())),
                        auditStamp));

            _entityClient.ingestProposal(
                context.getOperationContext(), patchBuilder.build(), false);

            return StructuredPropertiesMapper.map(
                context,
                getStructuredProperties(context.getOperationContext(), assetUrn),
                assetUrn);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private StructuredProperties getStructuredProperties(
      @Nonnull OperationContext opContext, Urn assetUrn) throws Exception {
    EntityResponse response =
        _entityClient.getV2(
            opContext.withReadPreference(ReadPreference.PRIMARY),
            assetUrn.getEntityType(),
            assetUrn,
            ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME));
    StructuredProperties structuredProperties = new StructuredProperties();
    structuredProperties.setProperties(new StructuredPropertyValueAssignmentArray());
    if (response != null
        && response.getAspects() != null
        && response.getAspects().containsKey(STRUCTURED_PROPERTIES_ASPECT_NAME)) {
      structuredProperties =
          new StructuredProperties(
              response.getAspects().get(STRUCTURED_PROPERTIES_ASPECT_NAME).getValue().data());
    }
    return structuredProperties;
  }
}

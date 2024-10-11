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
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import graphql.com.google.common.collect.ImmutableSet;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
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
            if (!AuthorizationUtils.canEditProperties(assetUrn, context)) {
              throw new AuthorizationException(
                  String.format(
                      "Not authorized to update properties on the gives urn %s", assetUrn));
            }

            final AuditStamp auditStamp =
                AuditStampUtils.createAuditStamp(authentication.getActor().toUrnStr());

            // schemaField entities often don't exist, create it if upserting on a schema field
            if (!assetUrn.getEntityType().equals(SCHEMA_FIELD_ENTITY_NAME)
                && !_entityClient.exists(context.getOperationContext(), assetUrn)) {
              throw new RuntimeException(
                  String.format("Asset with provided urn %s does not exist", assetUrn));
            }

            // get or default the structured properties aspect
            StructuredProperties structuredProperties =
                getStructuredProperties(context.getOperationContext(), assetUrn);

            // update the existing properties based on new value
            StructuredPropertyValueAssignmentArray properties =
                updateExistingProperties(structuredProperties, updateMap, auditStamp);

            // append any new properties from our input
            addNewProperties(properties, updateMap, auditStamp);

            structuredProperties.setProperties(properties);

            // ingest change proposal
            final MetadataChangeProposal structuredPropertiesProposal =
                AspectUtils.buildMetadataChangeProposal(
                    assetUrn, STRUCTURED_PROPERTIES_ASPECT_NAME, structuredProperties);

            _entityClient.ingestProposal(
                context.getOperationContext(), structuredPropertiesProposal, false);

            return StructuredPropertiesMapper.map(context, structuredProperties, assetUrn);
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
            opContext,
            assetUrn.getEntityType(),
            assetUrn,
            ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME));
    StructuredProperties structuredProperties = new StructuredProperties();
    structuredProperties.setProperties(new StructuredPropertyValueAssignmentArray());
    if (response != null && response.getAspects().containsKey(STRUCTURED_PROPERTIES_ASPECT_NAME)) {
      structuredProperties =
          new StructuredProperties(
              response.getAspects().get(STRUCTURED_PROPERTIES_ASPECT_NAME).getValue().data());
    }
    return structuredProperties;
  }

  private StructuredPropertyValueAssignmentArray updateExistingProperties(
      StructuredProperties structuredProperties,
      Map<String, List<PropertyValueInput>> updateMap,
      AuditStamp auditStamp) {
    return new StructuredPropertyValueAssignmentArray(
        structuredProperties.getProperties().stream()
            .map(
                propAssignment -> {
                  String propUrnString = propAssignment.getPropertyUrn().toString();
                  if (updateMap.containsKey(propUrnString)) {
                    List<PropertyValueInput> valueList = updateMap.get(propUrnString);
                    PrimitivePropertyValueArray values =
                        new PrimitivePropertyValueArray(
                            valueList.stream()
                                .map(StructuredPropertyUtils::mapPropertyValueInput)
                                .collect(Collectors.toList()));
                    propAssignment.setValues(values);
                    propAssignment.setLastModified(auditStamp);
                  }
                  return propAssignment;
                })
            .collect(Collectors.toList()));
  }

  private void addNewProperties(
      StructuredPropertyValueAssignmentArray properties,
      Map<String, List<PropertyValueInput>> updateMap,
      AuditStamp auditStamp) {
    // first remove existing properties from updateMap so that we append only new properties
    properties.forEach(prop -> updateMap.remove(prop.getPropertyUrn().toString()));

    updateMap.forEach(
        (structuredPropUrn, values) -> {
          StructuredPropertyValueAssignment valueAssignment =
              new StructuredPropertyValueAssignment();
          valueAssignment.setPropertyUrn(UrnUtils.getUrn(structuredPropUrn));
          valueAssignment.setValues(
              new PrimitivePropertyValueArray(
                  values.stream()
                      .map(StructuredPropertyUtils::mapPropertyValueInput)
                      .collect(Collectors.toList())));
          valueAssignment.setCreated(auditStamp);
          valueAssignment.setLastModified(auditStamp);
          properties.add(valueAssignment);
        });
  }
}

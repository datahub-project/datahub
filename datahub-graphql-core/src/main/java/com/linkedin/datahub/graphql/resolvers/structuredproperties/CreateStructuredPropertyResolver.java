package com.linkedin.datahub.graphql.resolvers.structuredproperties;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateStructuredPropertyInput;
import com.linkedin.datahub.graphql.generated.StructuredPropertyEntity;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertyMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.aspect.patch.builder.StructuredPropertyDefinitionPatchBuilder;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PropertyCardinality;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.StructuredPropertyKey;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

public class CreateStructuredPropertyResolver
    implements DataFetcher<CompletableFuture<StructuredPropertyEntity>> {

  private final EntityClient _entityClient;

  public CreateStructuredPropertyResolver(@Nonnull final EntityClient entityClient) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public CompletableFuture<StructuredPropertyEntity> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final CreateStructuredPropertyInput input =
        bindArgument(environment.getArgument("input"), CreateStructuredPropertyInput.class);

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            if (!AuthorizationUtils.canManageStructuredProperties(context)) {
              throw new AuthorizationException(
                  "Unable to create structured property. Please contact your admin.");
            }
            final StructuredPropertyKey key = new StructuredPropertyKey();
            final String id = input.getId() != null ? input.getId() : UUID.randomUUID().toString();
            key.setId(id);
            final Urn propertyUrn =
                EntityKeyUtils.convertEntityKeyToUrn(key, STRUCTURED_PROPERTY_ENTITY_NAME);
            StructuredPropertyDefinitionPatchBuilder builder =
                new StructuredPropertyDefinitionPatchBuilder().urn(propertyUrn);

            builder.setQualifiedName(input.getQualifiedName());
            builder.setValueType(input.getValueType());
            input.getEntityTypes().forEach(builder::addEntityType);
            if (input.getDisplayName() != null) {
              builder.setDisplayName(input.getDisplayName());
            }
            if (input.getDescription() != null) {
              builder.setDescription(input.getDescription());
            }
            if (input.getImmutable() != null) {
              builder.setImmutable(input.getImmutable());
            }
            if (input.getTypeQualifier() != null) {
              buildTypeQualifier(input, builder);
            }
            if (input.getAllowedValues() != null) {
              buildAllowedValues(input, builder);
            }
            if (input.getCardinality() != null) {
              builder.setCardinality(
                  PropertyCardinality.valueOf(input.getCardinality().toString()));
            }
            builder.setCreated(context.getOperationContext().getAuditStamp());
            builder.setLastModified(context.getOperationContext().getAuditStamp());

            MetadataChangeProposal mcp = builder.build();
            _entityClient.ingestProposal(context.getOperationContext(), mcp, false);

            EntityResponse response =
                _entityClient.getV2(
                    context.getOperationContext(),
                    STRUCTURED_PROPERTY_ENTITY_NAME,
                    propertyUrn,
                    null);
            return StructuredPropertyMapper.map(context, response);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        });
  }

  private void buildTypeQualifier(
      @Nonnull final CreateStructuredPropertyInput input,
      @Nonnull final StructuredPropertyDefinitionPatchBuilder builder) {
    if (input.getTypeQualifier().getAllowedTypes() != null) {
      final StringArrayMap typeQualifier = new StringArrayMap();
      StringArray allowedTypes = new StringArray();
      allowedTypes.addAll(input.getTypeQualifier().getAllowedTypes());
      typeQualifier.put("allowedTypes", allowedTypes);
      builder.setTypeQualifier(typeQualifier);
    }
  }

  private void buildAllowedValues(
      @Nonnull final CreateStructuredPropertyInput input,
      @Nonnull final StructuredPropertyDefinitionPatchBuilder builder) {
    input
        .getAllowedValues()
        .forEach(
            allowedValueInput -> {
              PropertyValue value = new PropertyValue();
              PrimitivePropertyValue primitiveValue = new PrimitivePropertyValue();
              if (allowedValueInput.getStringValue() != null) {
                primitiveValue.setString(allowedValueInput.getStringValue());
              }
              if (allowedValueInput.getNumberValue() != null) {
                primitiveValue.setDouble(allowedValueInput.getNumberValue().doubleValue());
              }
              value.setValue(primitiveValue);
              value.setDescription(allowedValueInput.getDescription(), SetMode.IGNORE_NULL);
              builder.addAllowedValue(value);
            });
  }
}

package com.linkedin.datahub.graphql.resolvers.structuredproperties;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.StructuredPropertyEntity;
import com.linkedin.datahub.graphql.generated.UpdateStructuredPropertyInput;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertyMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.aspect.patch.builder.StructuredPropertyDefinitionPatchBuilder;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PropertyCardinality;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.StructuredPropertyDefinition;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UpdateStructuredPropertyResolver
    implements DataFetcher<CompletableFuture<StructuredPropertyEntity>> {

  private final EntityClient _entityClient;

  private static final String ALLOWED_TYPES = "allowedTypes";

  public UpdateStructuredPropertyResolver(@Nonnull final EntityClient entityClient) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public CompletableFuture<StructuredPropertyEntity> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final UpdateStructuredPropertyInput input =
        bindArgument(environment.getArgument("input"), UpdateStructuredPropertyInput.class);

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            if (!AuthorizationUtils.canManageStructuredProperties(context)) {
              throw new AuthorizationException(
                  "Unable to update structured property. Please contact your admin.");
            }
            final Urn propertyUrn = UrnUtils.getUrn(input.getUrn());
            StructuredPropertyDefinition existingDefinition =
                getExistingStructuredProperty(context, propertyUrn);
            StructuredPropertyDefinitionPatchBuilder builder =
                new StructuredPropertyDefinitionPatchBuilder().urn(propertyUrn);

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
              buildTypeQualifier(input, builder, existingDefinition);
            }
            if (input.getNewAllowedValues() != null) {
              buildAllowedValues(input, builder);
            }
            if (input.getSetCardinalityAsMultiple() != null) {
              builder.setCardinality(PropertyCardinality.MULTIPLE);
            }
            if (input.getNewEntityTypes() != null) {
              input.getNewEntityTypes().forEach(builder::addEntityType);
            }
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
      @Nonnull final UpdateStructuredPropertyInput input,
      @Nonnull final StructuredPropertyDefinitionPatchBuilder builder,
      @Nullable final StructuredPropertyDefinition existingDefinition) {
    if (input.getTypeQualifier().getNewAllowedTypes() != null) {
      final StringArrayMap typeQualifier = new StringArrayMap();
      StringArray allowedTypes = new StringArray();
      if (existingDefinition != null
          && existingDefinition.getTypeQualifier() != null
          && existingDefinition.getTypeQualifier().get(ALLOWED_TYPES) != null) {
        allowedTypes.addAll(existingDefinition.getTypeQualifier().get(ALLOWED_TYPES));
      }
      allowedTypes.addAll(input.getTypeQualifier().getNewAllowedTypes());
      typeQualifier.put("allowedTypes", allowedTypes);
      builder.setTypeQualifier(typeQualifier);
    }
  }

  private void buildAllowedValues(
      @Nonnull final UpdateStructuredPropertyInput input,
      @Nonnull final StructuredPropertyDefinitionPatchBuilder builder) {
    input
        .getNewAllowedValues()
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

  private StructuredPropertyDefinition getExistingStructuredProperty(
      @Nonnull final QueryContext context, @Nonnull final Urn propertyUrn) throws Exception {
    EntityResponse response =
        _entityClient.getV2(
            context.getOperationContext(), STRUCTURED_PROPERTY_ENTITY_NAME, propertyUrn, null);

    if (response != null
        && response.getAspects().containsKey(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)) {
      return new StructuredPropertyDefinition(
          response.getAspects().get(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME).getValue().data());
    }
    return null;
  }
}

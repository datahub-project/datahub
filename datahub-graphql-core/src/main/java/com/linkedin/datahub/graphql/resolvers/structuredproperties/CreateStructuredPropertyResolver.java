package com.linkedin.datahub.graphql.resolvers.structuredproperties;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.buildMetadataChangeProposalWithUrn;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateStructuredPropertyInput;
import com.linkedin.datahub.graphql.generated.StructuredPropertyEntity;
import com.linkedin.datahub.graphql.generated.StructuredPropertySettingsInput;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertyMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.aspect.patch.builder.StructuredPropertyDefinitionPatchBuilder;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PropertyCardinality;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.StructuredPropertyKey;
import com.linkedin.structured.StructuredPropertySettings;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
            final String id =
                StructuredPropertyUtils.getPropertyId(input.getId(), input.getQualifiedName());
            key.setId(id);
            final Urn propertyUrn =
                EntityKeyUtils.convertEntityKeyToUrn(key, STRUCTURED_PROPERTY_ENTITY_NAME);

            if (_entityClient.exists(context.getOperationContext(), propertyUrn)) {
              throw new IllegalArgumentException(
                  "A structured property already exists with this urn");
            }

            List<MetadataChangeProposal> mcps = new ArrayList<>();

            // first, create the property definition itself
            mcps.add(createPropertyDefinition(context, propertyUrn, id, input));

            // then add the settings aspect if we're adding any settings inputs
            if (input.getSettings() != null) {
              mcps.add(createPropertySettings(context, propertyUrn, input.getSettings()));
            }

            _entityClient.batchIngestProposals(context.getOperationContext(), mcps, false);

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

  private MetadataChangeProposal createPropertySettings(
      @Nonnull final QueryContext context,
      @Nonnull final Urn propertyUrn,
      final StructuredPropertySettingsInput settingsInput)
      throws Exception {
    StructuredPropertySettings settings = new StructuredPropertySettings();

    if (settingsInput.getIsHidden() != null) {
      settings.setIsHidden(settingsInput.getIsHidden());
    }
    if (settingsInput.getShowInSearchFilters() != null) {
      settings.setShowInSearchFilters(settingsInput.getShowInSearchFilters());
    }
    if (settingsInput.getShowInAssetSummary() != null) {
      settings.setShowInAssetSummary(settingsInput.getShowInAssetSummary());
    }
    if (settingsInput.getShowAsAssetBadge() != null) {
      settings.setShowAsAssetBadge(settingsInput.getShowAsAssetBadge());
    }
    if (settingsInput.getShowInColumnsTable() != null) {
      settings.setShowInColumnsTable(settingsInput.getShowInColumnsTable());
    }
    settings.setLastModified(context.getOperationContext().getAuditStamp());

    StructuredPropertyUtils.validatePropertySettings(settings, true);

    return buildMetadataChangeProposalWithUrn(
        propertyUrn, STRUCTURED_PROPERTY_SETTINGS_ASPECT_NAME, settings);
  }

  private MetadataChangeProposal createPropertyDefinition(
      @Nonnull final QueryContext context,
      @Nonnull final Urn propertyUrn,
      @Nonnull final String id,
      final CreateStructuredPropertyInput input)
      throws Exception {
    StructuredPropertyDefinitionPatchBuilder builder =
        new StructuredPropertyDefinitionPatchBuilder().urn(propertyUrn);

    builder.setQualifiedName(id);
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
      builder.setCardinality(PropertyCardinality.valueOf(input.getCardinality().toString()));
    }
    builder.setCreated(context.getOperationContext().getAuditStamp());
    builder.setLastModified(context.getOperationContext().getAuditStamp());

    return builder.build();
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

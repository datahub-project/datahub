package com.linkedin.datahub.graphql.resolvers.structuredproperties;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.buildMetadataChangeProposalWithUrn;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.StructuredPropertyEntity;
import com.linkedin.datahub.graphql.generated.StructuredPropertySettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateStructuredPropertyInput;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertyMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.aspect.patch.builder.StructuredPropertyDefinitionPatchBuilder;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PropertyCardinality;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertySettings;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
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
            final EntityResponse entityResponse =
                getExistingStructuredProperty(context, propertyUrn);

            List<MetadataChangeProposal> mcps = new ArrayList<>();

            // first update the definition aspect if we need to
            MetadataChangeProposal definitionMcp =
                updateDefinition(input, context, propertyUrn, entityResponse);
            if (definitionMcp != null) {
              mcps.add(definitionMcp);
            }

            // then update the settings aspect if we need to
            if (input.getSettings() != null) {
              mcps.add(updateSettings(context, input.getSettings(), propertyUrn, entityResponse));
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

  private boolean hasSettingsChanged(
      StructuredPropertySettings existingSettings, StructuredPropertySettingsInput settingsInput) {
    if (settingsInput.getIsHidden() != null
        && !existingSettings.isIsHidden().equals(settingsInput.getIsHidden())) {
      return true;
    }
    if (settingsInput.getShowInSearchFilters() != null
        && !existingSettings
            .isShowInSearchFilters()
            .equals(settingsInput.getShowInSearchFilters())) {
      return true;
    }
    if (settingsInput.getShowInAssetSummary() != null
        && !existingSettings.isShowInAssetSummary().equals(settingsInput.getShowInAssetSummary())) {
      return true;
    }
    if (settingsInput.getShowAsAssetBadge() != null
        && !existingSettings.isShowAsAssetBadge().equals(settingsInput.getShowAsAssetBadge())) {
      return true;
    }
    if (settingsInput.getShowInColumnsTable() != null
        && !existingSettings.isShowInColumnsTable().equals(settingsInput.getShowInColumnsTable())) {
      return true;
    }
    return false;
  }

  private MetadataChangeProposal updateSettings(
      @Nonnull final QueryContext context,
      @Nonnull final StructuredPropertySettingsInput settingsInput,
      @Nonnull final Urn propertyUrn,
      @Nonnull final EntityResponse entityResponse)
      throws Exception {
    StructuredPropertySettings existingSettings =
        getExistingStructuredPropertySettings(entityResponse);
    // check if settings has changed to determine if we should update the timestamp
    boolean hasChanged = hasSettingsChanged(existingSettings, settingsInput);
    if (hasChanged) {
      existingSettings.setLastModified(context.getOperationContext().getAuditStamp());
    }

    if (settingsInput.getIsHidden() != null) {
      existingSettings.setIsHidden(settingsInput.getIsHidden());
    }
    if (settingsInput.getShowInSearchFilters() != null) {
      existingSettings.setShowInSearchFilters(settingsInput.getShowInSearchFilters());
    }
    if (settingsInput.getShowInAssetSummary() != null) {
      existingSettings.setShowInAssetSummary(settingsInput.getShowInAssetSummary());
    }
    if (settingsInput.getShowAsAssetBadge() != null) {
      existingSettings.setShowAsAssetBadge(settingsInput.getShowAsAssetBadge());
    }
    if (settingsInput.getShowInColumnsTable() != null) {
      existingSettings.setShowInColumnsTable(settingsInput.getShowInColumnsTable());
    }

    StructuredPropertyUtils.validatePropertySettings(existingSettings, true);

    return buildMetadataChangeProposalWithUrn(
        propertyUrn, STRUCTURED_PROPERTY_SETTINGS_ASPECT_NAME, existingSettings);
  }

  private MetadataChangeProposal updateDefinition(
      @Nonnull final UpdateStructuredPropertyInput input,
      @Nonnull final QueryContext context,
      @Nonnull final Urn propertyUrn,
      @Nonnull final EntityResponse entityResponse)
      throws Exception {
    StructuredPropertyDefinition existingDefinition =
        getExistingStructuredPropertyDefinition(entityResponse);
    StructuredPropertyDefinitionPatchBuilder builder =
        new StructuredPropertyDefinitionPatchBuilder().urn(propertyUrn);

    boolean hasUpdatedDefinition = false;

    if (input.getDisplayName() != null) {
      builder.setDisplayName(input.getDisplayName());
      hasUpdatedDefinition = true;
    }
    if (input.getDescription() != null) {
      builder.setDescription(input.getDescription());
      hasUpdatedDefinition = true;
    }
    if (input.getImmutable() != null) {
      builder.setImmutable(input.getImmutable());
      hasUpdatedDefinition = true;
    }
    if (input.getTypeQualifier() != null) {
      buildTypeQualifier(input, builder, existingDefinition);
      hasUpdatedDefinition = true;
    }
    if (input.getNewAllowedValues() != null) {
      buildAllowedValues(input, builder);
      hasUpdatedDefinition = true;
    }
    if (input.getSetCardinalityAsMultiple() != null
        && input.getSetCardinalityAsMultiple().equals(true)) {
      builder.setCardinality(PropertyCardinality.MULTIPLE);
      hasUpdatedDefinition = true;
    }
    if (input.getNewEntityTypes() != null) {
      input.getNewEntityTypes().forEach(builder::addEntityType);
      hasUpdatedDefinition = true;
    }

    if (hasUpdatedDefinition) {
      builder.setLastModified(context.getOperationContext().getAuditStamp());

      return builder.build();
    }
    return null;
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

  private EntityResponse getExistingStructuredProperty(
      @Nonnull final QueryContext context, @Nonnull final Urn propertyUrn) throws Exception {
    return _entityClient.getV2(
        context.getOperationContext(), STRUCTURED_PROPERTY_ENTITY_NAME, propertyUrn, null);
  }

  private StructuredPropertyDefinition getExistingStructuredPropertyDefinition(
      EntityResponse response) throws Exception {
    if (response != null
        && response.getAspects().containsKey(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)) {
      return new StructuredPropertyDefinition(
          response
              .getAspects()
              .get(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)
              .getValue()
              .data()
              .copy());
    }
    throw new IllegalArgumentException(
        "Attempting to update a structured property with no definition aspect.");
  }

  private StructuredPropertySettings getExistingStructuredPropertySettings(EntityResponse response)
      throws Exception {
    if (response != null
        && response.getAspects().containsKey(STRUCTURED_PROPERTY_SETTINGS_ASPECT_NAME)) {
      return new StructuredPropertySettings(
          response
              .getAspects()
              .get(STRUCTURED_PROPERTY_SETTINGS_ASPECT_NAME)
              .getValue()
              .data()
              .copy());
    }
    return new StructuredPropertySettings();
  }
}

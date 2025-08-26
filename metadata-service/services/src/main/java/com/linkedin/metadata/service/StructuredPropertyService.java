package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.PropertyCardinality;
import com.linkedin.structured.PropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

// todo: the value validation logic needs to be improved and merged with
// StructuredPropertiesValidator.java.
public class StructuredPropertyService extends BaseService {
  private static final Set<LogicalValueType> VALID_VALUE_STORED_AS_STRING =
      new HashSet<>(
          Arrays.asList(
              LogicalValueType.STRING,
              LogicalValueType.RICH_TEXT,
              LogicalValueType.DATE,
              LogicalValueType.URN));

  public StructuredPropertyService(
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull final ObjectMapper objectMapper) {
    super(entityClient, openApiClient, objectMapper);
  }

  /**
   * Retrieve all asset level structured properties for a given asset.
   *
   * @param opContext the operation context
   * @param entityUrn the entity to retrieve properties for.
   * @return the structured properties associated with the entity.
   */
  public List<StructuredPropertyValueAssignment> getEntityStructuredProperties(
      @Nonnull OperationContext opContext, @Nonnull Urn entityUrn) {
    final StructuredProperties maybeStructuredProperties =
        getStructuredProperties(opContext, entityUrn);
    if (maybeStructuredProperties != null) {
      return maybeStructuredProperties.getProperties();
    }
    return Collections.emptyList();
  }

  /**
   * Retrieve all schema-field level structured properties for a given asset.
   *
   * @param opContext the operation context
   * @param entityUrn the entity to retrieve properties for
   * @return the structured property associations associated with the schema field
   */
  public List<StructuredPropertyValueAssignment> getSchemaFieldStructuredProperties(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String fieldPath) {
    final Urn schemaFieldUrn = createSchemaFieldUrn(entityUrn, fieldPath);
    final StructuredProperties maybeStructuredProperties =
        getStructuredProperties(opContext, schemaFieldUrn);
    if (maybeStructuredProperties != null) {
      return maybeStructuredProperties.getProperties();
    }
    return Collections.emptyList();
  }

  /**
   * Note: This is LIGHTWEIGHT Pre-validation. Please refer to {@link
   * StructuredPropertiesValidator.java} for full validation logic. Returns true if the provided
   * values are valid for the given structured property.
   *
   * @param opContext the operation context
   * @param values the entity to retrieve properties for
   * @return true if the provided values are valid, false otherwise.
   */
  public boolean areProposedStructuredPropertyValuesValid(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn structuredPropertyUrn,
      @Nonnull final List<PrimitivePropertyValue> values) {
    // First, fetch the definition for the structured property
    final StructuredPropertyDefinition definition =
        getStructuredPropertyDefinition(opContext, structuredPropertyUrn);

    if (definition == null) {
      throw new EntityDoesNotExistException(
          String.format("Structured property with urn %s does not exist", structuredPropertyUrn));
    }

    // Validate that the provided values are valid for the structured property
    return areProposedValuesValid(definition, values);
  }

  /**
   * Update structured properties for an entity. Note that authorization checks should already be
   * done before calling this!
   *
   * <p>This operation assumes that the entity with provided URN already exists.
   *
   * @param opContext the operation context
   * @param entityUrn the entity to update structured properties fofr
   * @param propertyValueAssignments the property values to assign
   * @return the updated structured properties
   *     <p>TODO: Migrate UpsertStructuredPropertiesResolver to use this method in OSS + SaaS.
   */
  @Nonnull
  public StructuredProperties updateEntityStructuredProperties(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final List<StructuredPropertyValueAssignment> propertyValueAssignments)
      throws RemoteInvocationException {

    // get or default the structured properties aspect
    final StructuredProperties structuredProperties =
        getStructuredPropertiesOrDefault(
            opContext,
            entityUrn,
            new StructuredProperties().setProperties(new StructuredPropertyValueAssignmentArray()));

    // update the existing properties based on new value
    final StructuredPropertyValueAssignmentArray properties =
        updateExistingProperties(
            structuredProperties, propertyValueAssignments, opContext.getAuditStamp());

    // append any new properties from our input
    addNewProperties(properties, propertyValueAssignments, opContext.getAuditStamp());

    // Update properties.
    structuredProperties.setProperties(properties);

    // Ingest change proposal
    final MetadataChangeProposal structuredPropertiesProposal =
        AspectUtils.buildMetadataChangeProposal(
            entityUrn, STRUCTURED_PROPERTIES_ASPECT_NAME, structuredProperties);

    this.entityClient.ingestProposal(opContext, structuredPropertiesProposal, false);

    return structuredProperties;
  }

  private boolean areProposedValuesValid(
      @Nonnull final StructuredPropertyDefinition definition,
      @Nonnull final List<PrimitivePropertyValue> values) {
    // 1: Cardinality validation.
    if (PropertyCardinality.SINGLE.equals(definition.getCardinality())) {
      if (values.size() > 1) {
        return false;
      }
    }

    // 2: Type validation.
    Urn valueType = definition.getValueType();
    if (!areValuesValidForValueType(valueType, values)) {
      return false;
    }

    // 3: Allowed Values Validation.
    if (definition.hasAllowedValues() && definition.getAllowedValues().size() > 0) {
      // Validate that values are present in allowed values.
      final PropertyValueArray allowedValues = definition.getAllowedValues();
      for (PrimitivePropertyValue value : values) {
        if (allowedValues.stream().noneMatch(allowed -> allowed.getValue().equals(value))) {
          return false;
        }
      }
    }

    // All pre-flight validation passed!
    return true;
  }

  private boolean areValuesValidForValueType(
      @Nonnull final Urn valueType, @Nonnull final List<PrimitivePropertyValue> values) {
    // This is the most complex piece. For now, we do a lightweight validation
    // Until we can figure out how to reuse the existing validation logic more cleanly.
    if (VALID_VALUE_STORED_AS_STRING.contains(
        StructuredPropertyUtils.getLogicalValueType(valueType))) {
      return values.stream().allMatch(value -> value.getString() != null);
    } else {
      return values.stream().allMatch(value -> value.getDouble() != null);
    }
  }

  private StructuredPropertyValueAssignmentArray updateExistingProperties(
      @Nonnull final StructuredProperties structuredProperties,
      @Nonnull final List<StructuredPropertyValueAssignment> propertyValueAssignments,
      @Nonnull final AuditStamp auditStamp) {

    final Map<Urn, StructuredPropertyValueAssignment> updateMap =
        propertyValueAssignments.stream()
            .collect(
                Collectors.toMap(
                    StructuredPropertyValueAssignment::getPropertyUrn,
                    propAssignment -> propAssignment));

    return new StructuredPropertyValueAssignmentArray(
        structuredProperties.getProperties().stream()
            .map(
                propAssignment -> {
                  if (updateMap.containsKey(propAssignment.getPropertyUrn())) {
                    StructuredPropertyValueAssignment valueList =
                        updateMap.get(propAssignment.getPropertyUrn());
                    PrimitivePropertyValueArray newValues =
                        new PrimitivePropertyValueArray(valueList.getValues());
                    propAssignment.setValues(newValues);
                    propAssignment.setLastModified(auditStamp);
                  }
                  return propAssignment;
                })
            .collect(Collectors.toList()));
  }

  private void addNewProperties(
      @Nonnull final StructuredPropertyValueAssignmentArray properties,
      @Nonnull final List<StructuredPropertyValueAssignment> propertyValueAssignments,
      @Nonnull final AuditStamp auditStamp) {

    final Map<Urn, StructuredPropertyValueAssignment> updateMap =
        propertyValueAssignments.stream()
            .collect(
                Collectors.toMap(
                    StructuredPropertyValueAssignment::getPropertyUrn,
                    propAssignment -> propAssignment));

    /* Remove existing properties */
    properties.forEach(prop -> updateMap.remove(prop.getPropertyUrn()));

    /* Add new properties */
    updateMap.forEach(
        (structuredPropUrn, proposedValueAssignment) -> {
          StructuredPropertyValueAssignment valueAssignment =
              new StructuredPropertyValueAssignment();
          valueAssignment.setPropertyUrn(proposedValueAssignment.getPropertyUrn());
          valueAssignment.setValues(
              new PrimitivePropertyValueArray(proposedValueAssignment.getValues()));
          valueAssignment.setLastModified(auditStamp);
          properties.add(valueAssignment);
        });
  }

  @Nonnull
  private StructuredProperties getStructuredPropertiesOrDefault(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull StructuredProperties defaultProperties) {
    final StructuredProperties maybeStructuredProperties =
        getStructuredProperties(opContext, entityUrn);
    if (maybeStructuredProperties != null) {
      return maybeStructuredProperties;
    }
    return defaultProperties;
  }

  @Nullable
  private StructuredProperties getStructuredProperties(
      @Nonnull OperationContext opContext, @Nonnull Urn entityUrn) {
    final EntityResponse response = getStructuredPropertiesEntityResponse(opContext, entityUrn);
    if (response != null && response.getAspects().containsKey(STRUCTURED_PROPERTIES_ASPECT_NAME)) {
      return new StructuredProperties(
          response.getAspects().get(STRUCTURED_PROPERTIES_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  @Nullable
  private EntityResponse getStructuredPropertiesEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    try {
      return this.entityClient.getV2(
          opContext,
          entityUrn.getEntityType(),
          entityUrn,
          ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to retrieve structured properties for entity with urn %s", entityUrn),
          e);
    }
  }

  @Nullable
  private StructuredPropertyDefinition getStructuredPropertyDefinition(
      @Nonnull OperationContext opContext, @Nonnull Urn entityUrn) {
    final EntityResponse response =
        getStructuredPropertyDefinitionEntityResponse(opContext, entityUrn);
    if (response != null
        && response.getAspects().containsKey(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)) {
      return new StructuredPropertyDefinition(
          response.getAspects().get(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  @Nullable
  private EntityResponse getStructuredPropertyDefinitionEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    try {
      return this.entityClient.getV2(
          opContext,
          entityUrn.getEntityType(),
          entityUrn,
          ImmutableSet.of(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to retrieve structured property definition for entity with urn %s",
              entityUrn),
          e);
    }
  }

  // TODO: Validate that we don't need to downgrade to V1 before fetching.
  private Urn createSchemaFieldUrn(@Nonnull final Urn entityUrn, @Nonnull final String fieldPath) {
    return UrnUtils.getUrn(String.format("urn:li:schemaField:(%s,%s)", entityUrn, fieldPath));
  }
}

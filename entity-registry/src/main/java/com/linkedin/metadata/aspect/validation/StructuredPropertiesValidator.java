package com.linkedin.metadata.aspect.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayMap;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.PropertyCardinality;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** A Validator for StructuredProperties Aspect that is attached to entities like Datasets, etc. */
@Slf4j
public class StructuredPropertiesValidator extends AspectPayloadValidator {

  private static final Set<LogicalValueType> VALID_VALUE_STORED_AS_STRING =
      new HashSet<>(
          Arrays.asList(
              LogicalValueType.STRING,
              LogicalValueType.RICH_TEXT,
              LogicalValueType.DATE,
              LogicalValueType.URN));

  public StructuredPropertiesValidator(AspectPluginConfig aspectPluginConfig) {
    super(aspectPluginConfig);
  }

  public static LogicalValueType getLogicalValueType(Urn valueType) {
    String valueTypeId = getValueTypeId(valueType);
    if (valueTypeId.equals("string")) {
      return LogicalValueType.STRING;
    } else if (valueTypeId.equals("date")) {
      return LogicalValueType.DATE;
    } else if (valueTypeId.equals("number")) {
      return LogicalValueType.NUMBER;
    } else if (valueTypeId.equals("urn")) {
      return LogicalValueType.URN;
    } else if (valueTypeId.equals("rich_text")) {
      return LogicalValueType.RICH_TEXT;
    }

    return LogicalValueType.UNKNOWN;
  }

  @Override
  protected void validateProposedAspect(
      @Nonnull ChangeType changeType,
      @Nonnull Urn entityUrn,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull RecordTemplate aspectPayload,
      @Nonnull AspectRetriever aspectRetriever)
      throws AspectValidationException {
    validate(aspectPayload, aspectRetriever);
  }

  public static boolean validate(
      @Nonnull RecordTemplate aspectPayload, @Nonnull AspectRetriever aspectRetriever)
      throws AspectValidationException {
    StructuredProperties structuredProperties = (StructuredProperties) aspectPayload;
    log.warn("Validator called with {}", structuredProperties);
    Map<Urn, List<StructuredPropertyValueAssignment>> structuredPropertiesMap =
        structuredProperties.getProperties().stream()
            .collect(
                Collectors.groupingBy(
                    x -> x.getPropertyUrn(),
                    HashMap::new,
                    Collectors.toCollection(ArrayList::new)));
    for (Map.Entry<Urn, List<StructuredPropertyValueAssignment>> entry :
        structuredPropertiesMap.entrySet()) {
      // There should only be one entry per structured property
      List<StructuredPropertyValueAssignment> values = entry.getValue();
      if (values.size() > 1) {
        throw new AspectValidationException(
            "Property: " + entry.getKey() + " has multiple entries: " + values);
      }
    }

    for (StructuredPropertyValueAssignment structuredPropertyValueAssignment :
        structuredProperties.getProperties()) {
      Urn propertyUrn = structuredPropertyValueAssignment.getPropertyUrn();
      String property = propertyUrn.toString();
      if (!propertyUrn.getEntityType().equals("structuredProperty")) {
        throw new IllegalStateException(
            "Unexpected entity type. Expected: structuredProperty Found: "
                + propertyUrn.getEntityType());
      }
      Aspect structuredPropertyDefinitionAspect = null;
      try {
        structuredPropertyDefinitionAspect =
            aspectRetriever.getLatestAspectObject(propertyUrn, "propertyDefinition");

        if (structuredPropertyDefinitionAspect == null) {
          throw new AspectValidationException("Unexpected null value found.");
        }
      } catch (Exception e) {
        log.error("Could not fetch latest aspect. PropertyUrn: {}", propertyUrn, e);
        throw new AspectValidationException("Could not fetch latest aspect: " + e.getMessage(), e);
      }

      StructuredPropertyDefinition structuredPropertyDefinition =
          new StructuredPropertyDefinition(structuredPropertyDefinitionAspect.data());
      log.warn(
          "Retrieved property definition for {}. {}", propertyUrn, structuredPropertyDefinition);
      if (structuredPropertyDefinition != null) {
        PrimitivePropertyValueArray values = structuredPropertyValueAssignment.getValues();
        // Check cardinality
        if (structuredPropertyDefinition.getCardinality() == PropertyCardinality.SINGLE) {
          if (values.size() > 1) {
            throw new AspectValidationException(
                "Property: "
                    + property
                    + " has cardinality 1, but multiple values were assigned: "
                    + values);
          }
        }
        // Check values
        for (PrimitivePropertyValue value : values) {
          validateType(propertyUrn, structuredPropertyDefinition, value);
          validateAllowedValues(propertyUrn, structuredPropertyDefinition, value);
        }
      }
    }

    return true;
  }

  private static void validateAllowedValues(
      Urn propertyUrn, StructuredPropertyDefinition definition, PrimitivePropertyValue value)
      throws AspectValidationException {
    if (definition.getAllowedValues() != null) {
      Set<PrimitivePropertyValue> definedValues =
          definition.getAllowedValues().stream()
              .map(PropertyValue::getValue)
              .collect(Collectors.toSet());
      if (definedValues.stream().noneMatch(definedPrimitive -> definedPrimitive.equals(value))) {
        throw new AspectValidationException(
            String.format(
                "Property: %s, value: %s should be one of %s", propertyUrn, value, definedValues));
      }
    }
  }

  private static void validateType(
      Urn propertyUrn, StructuredPropertyDefinition definition, PrimitivePropertyValue value)
      throws AspectValidationException {
    Urn valueType = definition.getValueType();
    LogicalValueType typeDefinition = getLogicalValueType(valueType);

    // Primitive Type Validation
    if (VALID_VALUE_STORED_AS_STRING.contains(typeDefinition)) {
      log.debug(
          "Property definition demands a string value. {}, {}", value.isString(), value.isDouble());
      if (value.getString() == null) {
        throw new AspectValidationException(
            "Property: " + propertyUrn.toString() + ", value: " + value + " should be a string");
      } else if (typeDefinition.equals(LogicalValueType.DATE)) {
        if (!StructuredPropertyUtils.isValidDate(value)) {
          throw new AspectValidationException(
              "Property: "
                  + propertyUrn.toString()
                  + ", value: "
                  + value
                  + " should be a date with format YYYY-MM-DD");
        }
      } else if (typeDefinition.equals(LogicalValueType.URN)) {
        StringArrayMap valueTypeQualifier = definition.getTypeQualifier();
        Urn typeValue;
        try {
          typeValue = Urn.createFromString(value.getString());
        } catch (URISyntaxException e) {
          throw new AspectValidationException(
              "Property: " + propertyUrn.toString() + ", value: " + value + " should be an urn", e);
        }
        if (valueTypeQualifier != null) {
          if (valueTypeQualifier.containsKey("allowedTypes")) {
            // Let's get the allowed types and validate that the value is one of those types
            StringArray allowedTypes = valueTypeQualifier.get("allowedTypes");
            boolean matchedAny = false;
            for (String type : allowedTypes) {
              Urn typeUrn = null;
              try {
                typeUrn = Urn.createFromString(type);
              } catch (URISyntaxException e) {

                // we don't expect to have types that we allowed to be written that aren't
                // urns
                throw new RuntimeException(e);
              }
              String allowedEntityName = getValueTypeId(typeUrn);
              if (typeValue.getEntityType().equals(allowedEntityName)) {
                matchedAny = true;
              }
            }
            if (!matchedAny) {
              throw new AspectValidationException(
                  "Property: "
                      + propertyUrn.toString()
                      + ", value: "
                      + value
                      + " is not of any supported urn types:"
                      + allowedTypes);
            }
          }
        }
      }
    } else if (typeDefinition.equals(LogicalValueType.NUMBER)) {
      log.debug("Property definition demands a numeric value. {}, {}", value.isString(), value);
      try {
        Double doubleValue =
            value.getDouble() != null ? value.getDouble() : Double.parseDouble(value.getString());
      } catch (NumberFormatException | NullPointerException e) {
        throw new AspectValidationException(
            "Property: " + propertyUrn.toString() + ", value: " + value + " should be a number");
      }
    } else {
      throw new AspectValidationException(
          "Validation support for type " + definition.getValueType() + " is not yet implemented.");
    }
  }

  private static String getValueTypeId(@Nonnull final Urn valueType) {
    String valueTypeId = valueType.getId();
    if (valueTypeId.startsWith("datahub.")) {
      valueTypeId = valueTypeId.split("\\.")[1];
    }
    return valueTypeId;
  }

  @Override
  protected void validatePreCommitAspect(
      @Nonnull ChangeType changeType,
      @Nonnull Urn entityUrn,
      @Nonnull AspectSpec aspectSpec,
      @Nullable RecordTemplate previousAspect,
      @Nonnull RecordTemplate proposedAspect,
      AspectRetriever aspectRetriever)
      throws AspectValidationException {
    // No-op
  }
}

package com.linkedin.metadata.aspect.validation;

import static com.linkedin.structured.PropertyCardinality.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.StructuredPropertyDefinition;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class PropertyDefinitionValidator extends AspectPayloadValidator {

  public PropertyDefinitionValidator(AspectPluginConfig aspectPluginConfig) {
    super(aspectPluginConfig);
  }

  @Override
  protected void validateProposedAspect(
      @Nonnull ChangeType changeType,
      @Nonnull Urn entityUrn,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull RecordTemplate aspectPayload,
      @Nonnull AspectRetriever aspectRetriever)
      throws AspectValidationException {
    // No-op
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
    validate(previousAspect, proposedAspect);
  }

  public static boolean validate(
      @Nullable RecordTemplate previousAspect, @Nonnull RecordTemplate proposedAspect)
      throws AspectValidationException {
    if (previousAspect != null) {
      StructuredPropertyDefinition previousDefinition =
          (StructuredPropertyDefinition) previousAspect;
      StructuredPropertyDefinition newDefinition = (StructuredPropertyDefinition) proposedAspect;
      if (!newDefinition.getValueType().equals(previousDefinition.getValueType())) {
        throw new AspectValidationException(
            "Value type cannot be changed as this is a backwards incompatible change");
      }
      if (newDefinition.getCardinality().equals(SINGLE)
          && previousDefinition.getCardinality().equals(MULTIPLE)) {
        throw new AspectValidationException(
            "Property definition cardinality cannot be changed from MULTI to SINGLE");
      }
      if (!newDefinition.getQualifiedName().equals(previousDefinition.getQualifiedName())) {
        throw new AspectValidationException(
            "Cannot change the fully qualified name of a Structured Property");
      }
      // Assure new definition has only added allowed values, not removed them
      if (newDefinition.getAllowedValues() != null) {
        if (!previousDefinition.hasAllowedValues()
            || previousDefinition.getAllowedValues() == null) {
          throw new AspectValidationException(
              "Cannot restrict values that were previously allowed");
        }
        Set<PrimitivePropertyValue> newAllowedValues =
            newDefinition.getAllowedValues().stream()
                .map(PropertyValue::getValue)
                .collect(Collectors.toSet());
        for (PropertyValue value : previousDefinition.getAllowedValues()) {
          if (!newAllowedValues.contains(value.getValue())) {
            throw new AspectValidationException(
                "Cannot restrict values that were previously allowed");
          }
        }
      }
    }
    return true;
  }
}

package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;
import static com.linkedin.structured.PropertyCardinality.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.StructuredPropertyDefinition;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(chain = true)
public class PropertyDefinitionValidator extends AspectPayloadValidator {
  private AspectPluginConfig config;

  /**
   * Prevent deletion of the definition or key aspect (only soft delete)
   *
   * @param mcpItems
   * @param retrieverContext
   * @return
   */
  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    final String entityKeyAspect =
        retrieverContext
            .getAspectRetriever()
            .getEntityRegistry()
            .getEntitySpec(STRUCTURED_PROPERTY_ENTITY_NAME)
            .getKeyAspectName();

    return mcpItems.stream()
        .filter(i -> ChangeType.DELETE.equals(i.getChangeType()))
        .map(
            i -> {
              if (ImmutableSet.of(entityKeyAspect, STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)
                  .contains(i.getAspectSpec().getName())) {
                return AspectValidationException.forItem(
                    i, "Hard delete of Structured Property Definitions is not supported.");
              }
              return null;
            })
        .filter(Objects::nonNull);
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return validateDefinitionUpserts(
        changeMCPs.stream()
            .filter(
                i ->
                    ChangeType.UPSERT.equals(i.getChangeType())
                        && STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME.equals(i.getAspectName()))
            .collect(Collectors.toList()),
        retrieverContext);
  }

  public static Stream<AspectValidationException> validateDefinitionUpserts(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    Set<Urn> propertyUrns = changeMCPs.stream().map(ChangeMCP::getUrn).collect(Collectors.toSet());

    // Batch fetch status aspects
    Map<Urn, Map<String, Aspect>> structuredPropertyAspects =
        fetchPropertyStatusAspects(propertyUrns, retrieverContext.getAspectRetriever());

    for (ChangeMCP item : changeMCPs) {
      // Prevent updates to the definition, if soft deleted property
      softDeleteCheck(
              item,
              structuredPropertyAspects.getOrDefault(item.getUrn(), Collections.emptyMap()),
              "Cannot mutate a soft deleted Structured Property Definition")
          .ifPresent(exceptions::addException);

      if (item.getPreviousSystemAspect() != null) {

        StructuredPropertyDefinition previousDefinition =
            item.getPreviousSystemAspect().getAspect(StructuredPropertyDefinition.class);
        StructuredPropertyDefinition newDefinition =
            item.getAspect(StructuredPropertyDefinition.class);

        if (!newDefinition.getValueType().equals(previousDefinition.getValueType())) {
          exceptions.addException(
              item, "Value type cannot be changed as this is a backwards incompatible change");
        }
        if (newDefinition.getCardinality().equals(SINGLE)
            && previousDefinition.getCardinality().equals(MULTIPLE)) {
          exceptions.addException(
              item, "Property definition cardinality cannot be changed from MULTI to SINGLE");
        }
        if (!newDefinition.getQualifiedName().equals(previousDefinition.getQualifiedName())) {
          exceptions.addException(
              item, "Cannot change the fully qualified name of a Structured Property");
        }
        // Assure new definition has only added allowed values, not removed them
        if (newDefinition.getAllowedValues() != null) {
          if (!previousDefinition.hasAllowedValues()
              || previousDefinition.getAllowedValues() == null) {
            exceptions.addException(item, "Cannot restrict values that were previously allowed");
          } else {
            Set<PrimitivePropertyValue> newAllowedValues =
                newDefinition.getAllowedValues().stream()
                    .map(PropertyValue::getValue)
                    .collect(Collectors.toSet());
            for (PropertyValue value : previousDefinition.getAllowedValues()) {
              if (!newAllowedValues.contains(value.getValue())) {
                exceptions.addException(
                    item, "Cannot restrict values that were previously allowed");
              }
            }
          }
        }
      }
    }

    return exceptions.streamAllExceptions();
  }

  private static Map<Urn, Map<String, Aspect>> fetchPropertyStatusAspects(
      Set<Urn> structuredPropertyUrns, AspectRetriever aspectRetriever) {
    return aspectRetriever.getLatestAspectObjects(
        structuredPropertyUrns, ImmutableSet.of(Constants.STATUS_ASPECT_NAME));
  }

  static <T extends BatchItem> Optional<AspectValidationException> softDeleteCheck(
      T item, @Nonnull Map<String, Aspect> structuredPropertyAspects, String message) {
    Aspect aspect = structuredPropertyAspects.get(STATUS_ASPECT_NAME);
    if (aspect != null && new Status(aspect.data()).isRemoved()) {
      return Optional.of(AspectValidationException.forItem(item, message));
    }
    return Optional.empty();
  }
}

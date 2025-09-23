package com.linkedin.metadata.structuredproperties.validation;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.structured.PropertyCardinality.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.StringArrayMap;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.StructuredPropertyDefinition;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(chain = true)
public class PropertyDefinitionValidator extends AspectPayloadValidator {
  private AspectPluginConfig config;

  private static String ALLOWED_TYPES = "allowedTypes";

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
    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return validateDefinitionUpserts(
        changeMCPs.stream()
            .filter(i -> STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME.equals(i.getAspectName()))
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

      final StructuredPropertyDefinition newDefinition =
          item.getAspect(StructuredPropertyDefinition.class);

      versionFormatCheck(item, newDefinition.getVersion()).ifPresent(exceptions::addException);
      urnIdCheck(item).ifPresent(exceptions::addException);
      qualifiedNameCheck(item, newDefinition.getQualifiedName())
          .ifPresent(exceptions::addException);
      allowedTypesCheck(
              item, newDefinition.getTypeQualifier(), retrieverContext.getAspectRetriever())
          .ifPresent(exceptions::addException);

      if (item.getPreviousSystemAspect() != null) {

        StructuredPropertyDefinition previousDefinition =
            item.getPreviousSystemAspect().getAspect(StructuredPropertyDefinition.class);

        if (!newDefinition.getValueType().equals(previousDefinition.getValueType())
            && !allowBreakingWithVersion(previousDefinition, newDefinition, item, exceptions)) {
          exceptions.addException(
              item, "Value type cannot be changed as this is a backwards incompatible change");
        }
        if (newDefinition.getCardinality().equals(SINGLE)
            && previousDefinition.getCardinality().equals(MULTIPLE)
            && !allowBreakingWithVersion(previousDefinition, newDefinition, item, exceptions)) {
          exceptions.addException(
              item, "Property definition cardinality cannot be changed from MULTI to SINGLE");
        }
        if (!newDefinition.getQualifiedName().equals(previousDefinition.getQualifiedName())) {
          exceptions.addException(
              item, "Cannot change the fully qualified name of a Structured Property");
        }
        // Assure new definition has only added allowed values, not removed them
        if (newDefinition.getAllowedValues() != null) {
          if ((!previousDefinition.hasAllowedValues()
                  || previousDefinition.getAllowedValues() == null)
              && !allowBreakingWithVersion(previousDefinition, newDefinition, item, exceptions)) {
            exceptions.addException(item, "Cannot restrict values that were previously allowed");
          } else if (!allowBreakingWithVersion(
              previousDefinition, newDefinition, item, exceptions)) {
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

  /**
   * Allow new version if monotonically increasing
   *
   * @param oldDefinition previous version
   * @param newDefinition next version
   * @return whether version increase should allow breaking change
   */
  private static boolean allowBreakingWithVersion(
      @Nonnull StructuredPropertyDefinition oldDefinition,
      @Nonnull StructuredPropertyDefinition newDefinition,
      @Nonnull ChangeMCP item,
      @Nonnull ValidationExceptionCollection exceptions) {
    final String oldVersion = oldDefinition.getVersion(GetMode.NULL);
    final String newVersion = newDefinition.getVersion(GetMode.NULL);

    if (newVersion != null && newVersion.contains(".")) {
      exceptions.addException(
          item,
          String.format("Invalid version `%s` cannot contain the `.` character.", newVersion));
    }

    if (oldVersion == null && newVersion != null) {
      return true;
    } else if (newVersion != null) {
      return newVersion.compareToIgnoreCase(oldVersion) > 0;
    }
    return false;
  }

  private static Pattern VERSION_REGEX = Pattern.compile("[0-9]{14}");

  private static Optional<AspectValidationException> versionFormatCheck(
      MCPItem item, @Nullable String version) {
    if (version != null && !VERSION_REGEX.matcher(version).matches()) {
      return Optional.of(
          AspectValidationException.forItem(
              item,
              String.format("Invalid version specified. Must match %s", VERSION_REGEX.toString())));
    }
    return Optional.empty();
  }

  private static Optional<AspectValidationException> urnIdCheck(MCPItem item) {
    if (item.getUrn().getId().contains(" ")) {
      return Optional.of(AspectValidationException.forItem(item, "Urn ID cannot have spaces"));
    }
    return Optional.empty();
  }

  private static Optional<AspectValidationException> qualifiedNameCheck(
      MCPItem item, @Nonnull String qualifiedName) {
    if (qualifiedName.contains(" ")) {
      return Optional.of(
          AspectValidationException.forItem(item, "Qualified names cannot have spaces"));
    }
    return Optional.empty();
  }

  private static Optional<AspectValidationException> allowedTypesCheck(
      MCPItem item, @Nullable StringArrayMap typeQualifier, AspectRetriever aspectRetriever) {
    if (typeQualifier == null || typeQualifier.get(ALLOWED_TYPES) == null) {
      return Optional.empty();
    }
    List<String> allowedTypes = typeQualifier.get(ALLOWED_TYPES);
    try {
      List<Urn> allowedTypesUrns =
          allowedTypes.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

      // ensure all types are entityTypes
      if (allowedTypesUrns.stream()
          .anyMatch(t -> !t.getEntityType().equals(ENTITY_TYPE_ENTITY_NAME))) {
        return Optional.of(
            AspectValidationException.forItem(
                item,
                String.format(
                    "Provided allowedType that is not an entityType entity. List of allowedTypes: %s",
                    allowedTypes)));
      }

      // ensure all types exist as entities
      Map<Urn, Boolean> existsMap = aspectRetriever.entityExists(new HashSet<>(allowedTypesUrns));
      if (existsMap.containsValue(false)) {
        return Optional.of(
            AspectValidationException.forItem(
                item,
                String.format(
                    "Provided allowedType that does not exist. List of allowedTypes: %s",
                    allowedTypes)));
      }
    } catch (Exception e) {
      return Optional.of(
          AspectValidationException.forItem(
              item,
              String.format(
                  "Issue resolving allowedTypes inside of typeQualifier. These must be entity type urns. List of allowedTypes: %s",
                  allowedTypes)));
    }

    return Optional.empty();
  }
}

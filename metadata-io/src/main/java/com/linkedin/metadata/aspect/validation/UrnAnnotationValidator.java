package com.linkedin.metadata.aspect.validation;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.annotation.UrnValidationAnnotation;
import com.linkedin.metadata.utils.UrnValidationUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Getter
@Accessors(chain = true)
public class UrnAnnotationValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    List<BatchItem> typeSafeItems = new ArrayList<>(mcpItems);

    Map<AspectSpec, List<BatchItem>> byAspectSpec =
        typeSafeItems.stream()
            .filter(
                item ->
                    item.getAspectSpec() != null
                        && item.getRecordTemplate() != null
                        && item.getRecordTemplate().data() != null)
            .collect(Collectors.groupingBy(ReadItem::getAspectSpec, Collectors.toList()));

    Map<BatchItem, Set<UrnValidationUtil.UrnValidationEntry>> urnValidationEntries =
        byAspectSpec.entrySet().stream()
            .flatMap(
                entry ->
                    UrnValidationUtil.findUrnValidationFields(entry.getValue(), entry.getKey())
                        .entrySet()
                        .stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // First check non-database validations
    Map<BatchItem, Set<AspectValidationException>> nonExistenceFailures =
        urnValidationEntries.entrySet().stream()
            .flatMap(
                itemEntry -> {
                  return itemEntry.getValue().stream()
                      .map(
                          validationEntry -> {
                            UrnValidationAnnotation annotation = validationEntry.getAnnotation();
                            String urnStr = validationEntry.getUrn();
                            String fieldPath = validationEntry.getFieldPath();

                            final Urn urn;
                            try {
                              urn = UrnUtils.requireUrn(urnStr);
                            } catch (IllegalArgumentException ex) {
                              return Map.entry(
                                  itemEntry.getKey(),
                                  AspectValidationException.forItem(
                                      itemEntry.getKey(),
                                      formatInvalidUrnMessage(
                                          fieldPath, urnStr, annotation.getEntityTypes())));
                            }

                            if (annotation.isStrict()) {
                              try {
                                UrnValidationUtil.validateUrn(
                                    retrieverContext.getAspectRetriever().getEntityRegistry(),
                                    urn,
                                    true);
                              } catch (RuntimeException ex) {
                                return Map.entry(
                                    itemEntry.getKey(),
                                    AspectValidationException.forItem(
                                        itemEntry.getKey(),
                                        String.format(
                                            "Invalid URN at path %s: %s",
                                            fieldPath, ex.getMessage())));
                              }
                            }
                            if (annotation.getEntityTypes() != null
                                && !annotation.getEntityTypes().isEmpty()) {
                              if (annotation.getEntityTypes().stream()
                                  .noneMatch(
                                      entityType -> entityType.equals(urn.getEntityType()))) {
                                return Map.entry(
                                    itemEntry.getKey(),
                                    AspectValidationException.forItem(
                                        itemEntry.getKey(),
                                        formatEntityTypeMismatchMessage(
                                            fieldPath, urn, annotation.getEntityTypes())));
                              }
                            }
                            return null;
                          });
                })
            .filter(Objects::nonNull)
            .collect(
                Collectors.groupingBy(
                    Map.Entry::getKey,
                    Collectors.mapping(Map.Entry::getValue, Collectors.toSet())));

    // Next check the database
    Set<Urn> checkUrns =
        urnValidationEntries.entrySet().stream()
            .filter(itemEntry -> !nonExistenceFailures.containsKey(itemEntry.getKey()))
            .flatMap(itemEntry -> itemEntry.getValue().stream())
            .filter(validationEntry -> validationEntry.getAnnotation().isExist())
            .map(entry -> UrnUtils.requireUrn(entry.getUrn()))
            .collect(Collectors.toSet());
    Map<Urn, Boolean> missingUrns =
        retrieverContext
            .getAspectRetriever()
            .entityExists(operationContext, checkUrns)
            .entrySet()
            .stream()
            .filter(urnExistsEntry -> Boolean.FALSE.equals(urnExistsEntry.getValue()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Set<AspectValidationException> existenceFailures =
        urnValidationEntries.entrySet().stream()
            .filter(itemEntry -> !nonExistenceFailures.containsKey(itemEntry.getKey()))
            .flatMap(
                itemEntry ->
                    itemEntry.getValue().stream()
                        .filter(validationEntry -> validationEntry.getAnnotation().isExist())
                        .map(
                            validationEntry -> {
                              if (missingUrns.containsKey(
                                  UrnUtils.requireUrn(validationEntry.getUrn()))) {
                                return AspectValidationException.forItem(
                                    itemEntry.getKey(),
                                    String.format(
                                        "Urn validation failure. Urn does not exist. Path: %s Urn: %s",
                                        validationEntry.getFieldPath(), validationEntry.getUrn()));
                              }
                              return null;
                            })
                        .filter(Objects::nonNull))
            .collect(Collectors.toSet());

    return Stream.concat(
        nonExistenceFailures.values().stream().flatMap(Set::stream), existenceFailures.stream());
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @Nonnull
  static String formatEntityTypeMismatchMessage(
      @Nonnull String fieldPath, @Nonnull Urn urn, @Nonnull List<String> expectedEntityTypes) {
    return String.format(
        "Invalid URN entity type at path %s: expected one of %s, got %s (%s)",
        fieldPath, expectedEntityTypes, urn.getEntityType(), urn);
  }

  @Nonnull
  static String formatInvalidUrnMessage(
      @Nonnull String fieldPath,
      @Nullable String urnStr,
      @Nullable List<String> expectedEntityTypes) {
    String expected =
        (expectedEntityTypes == null || expectedEntityTypes.isEmpty())
            ? "a valid URN"
            : formatExpectedUrnDescription(expectedEntityTypes);
    return String.format(
        "Invalid URN at path %s: expected %s, got \"%s\"", fieldPath, expected, urnStr);
  }

  @Nonnull
  private static String formatExpectedUrnDescription(@Nonnull List<String> expectedEntityTypes) {
    if (expectedEntityTypes.size() == 1 && "schemaField".equals(expectedEntityTypes.get(0))) {
      return "a schemaField URN (urn:li:schemaField:(<dataset>,<column>))";
    }
    if (expectedEntityTypes.size() == 1) {
      return "a " + expectedEntityTypes.get(0) + " URN";
    }
    return "a valid URN of type(s) " + expectedEntityTypes;
  }
}

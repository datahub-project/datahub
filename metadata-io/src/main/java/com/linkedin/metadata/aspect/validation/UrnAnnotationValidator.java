package com.linkedin.metadata.aspect.validation;

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

                            if (annotation.isStrict()) {
                              try {
                                UrnValidationUtil.validateUrn(
                                    retrieverContext.getAspectRetriever().getEntityRegistry(),
                                    UrnUtils.getUrn(validationEntry.getUrn()),
                                    true);
                              } catch (RuntimeException ex) {
                                return Map.entry(
                                    itemEntry.getKey(),
                                    AspectValidationException.forItem(
                                        itemEntry.getKey(), ex.getMessage()));
                              }
                            }
                            Urn urn = UrnUtils.getUrn(validationEntry.getUrn());
                            if (annotation.getEntityTypes() != null
                                && !annotation.getEntityTypes().isEmpty()) {
                              if (annotation.getEntityTypes().stream()
                                  .noneMatch(
                                      entityType -> entityType.equals(urn.getEntityType()))) {
                                return Map.entry(
                                    itemEntry.getKey(),
                                    AspectValidationException.forItem(
                                        itemEntry.getKey(),
                                        String.format(
                                            "Invalid entity type urn validation failure (Required: %s). Path: %s Urn: %s",
                                            validationEntry.getAnnotation().getEntityTypes(),
                                            validationEntry.getFieldPath(),
                                            urn)));
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
            .map(entry -> UrnUtils.getUrn(entry.getUrn()))
            .collect(Collectors.toSet());
    Map<Urn, Boolean> missingUrns =
        retrieverContext.getAspectRetriever().entityExists(checkUrns).entrySet().stream()
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
                                  UrnUtils.getUrn(validationEntry.getUrn()))) {
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
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}

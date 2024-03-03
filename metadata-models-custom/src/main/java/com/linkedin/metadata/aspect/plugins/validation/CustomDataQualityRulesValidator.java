package com.linkedin.metadata.aspect.plugins.validation;

import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.mycompany.dq.DataQualityRules;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public class CustomDataQualityRulesValidator extends AspectPayloadValidator {

  public CustomDataQualityRulesValidator(AspectPluginConfig config) {
    super(config);
  }

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems, @Nonnull AspectRetriever aspectRetriever) {
    return mcpItems.stream()
        .map(
            item -> {
              DataQualityRules rules = new DataQualityRules(item.getRecordTemplate().data());
              // Enforce at least 1 rule
              return rules.getRules().isEmpty()
                  ? new AspectValidationException(
                      item.getUrn(), item.getAspectName(), "At least one rule is required.")
                  : null;
            })
        .filter(Objects::nonNull);
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, AspectRetriever aspectRetriever) {
    return changeMCPs.stream()
        .flatMap(
            changeMCP -> {
              if (changeMCP.getPreviousSystemAspect() != null) {
                DataQualityRules oldRules = changeMCP.getPreviousAspect(DataQualityRules.class);
                DataQualityRules newRules = changeMCP.getAspect(DataQualityRules.class);

                Map<String, String> newFieldTypeMap =
                    newRules.getRules().stream()
                        .filter(rule -> rule.getField() != null)
                        .map(rule -> Map.entry(rule.getField(), rule.getType()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                // Ensure the old and new field type is the same
                return oldRules.getRules().stream()
                    .map(
                        oldRule -> {
                          if (!newFieldTypeMap
                              .getOrDefault(oldRule.getField(), oldRule.getType())
                              .equals(oldRule.getType())) {
                            return new AspectValidationException(
                                changeMCP.getUrn(),
                                changeMCP.getAspectName(),
                                String.format(
                                    "Field type mismatch. Field: %s Old: %s New: %s",
                                    oldRule.getField(),
                                    oldRule.getType(),
                                    newFieldTypeMap.get(oldRule.getField())));
                          }
                          return null;
                        })
                    .filter(Objects::nonNull);
              }

              return Stream.empty();
            });
  }
}

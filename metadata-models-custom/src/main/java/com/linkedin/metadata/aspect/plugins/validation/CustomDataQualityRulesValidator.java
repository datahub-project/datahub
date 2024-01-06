package com.linkedin.metadata.aspect.plugins.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.AspectSpec;
import com.mycompany.dq.DataQualityRule;
import com.mycompany.dq.DataQualityRules;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class CustomDataQualityRulesValidator extends AspectPayloadValidator {

  public CustomDataQualityRulesValidator(AspectPluginConfig config) {
    super(config);
  }

  @Override
  protected void validateProposedAspect(
      @Nonnull ChangeType changeType,
      @Nonnull Urn entityUrn,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull RecordTemplate aspectPayload,
      @Nonnull AspectRetriever aspectRetriever)
      throws AspectValidationException {
    DataQualityRules rules = new DataQualityRules(aspectPayload.data());

    // Enforce at least 1 rule
    if (rules.getRules().isEmpty()) {
      throw new AspectValidationException("At least one rule is required.");
    }
  }

  @Override
  protected void validatePreCommitAspect(
      @Nonnull ChangeType changeType,
      @Nonnull Urn entityUrn,
      @Nonnull AspectSpec aspectSpec,
      @Nullable RecordTemplate previousAspect,
      @Nonnull RecordTemplate proposedAspect,
      @Nonnull AspectRetriever aspectRetriever)
      throws AspectValidationException {

    if (previousAspect != null) {
      DataQualityRules oldRules = new DataQualityRules(previousAspect.data());
      DataQualityRules newRules = new DataQualityRules(proposedAspect.data());

      Map<String, String> newFieldTypeMap =
          newRules.getRules().stream()
              .filter(rule -> rule.getField() != null)
              .map(rule -> Map.entry(rule.getField(), rule.getType()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      // Ensure the old and new field type is the same
      for (DataQualityRule oldRule : oldRules.getRules()) {
        if (!newFieldTypeMap
            .getOrDefault(oldRule.getField(), oldRule.getType())
            .equals(oldRule.getType())) {
          throw new AspectValidationException(
              String.format(
                  "Field type mismatch. Field: %s Old: %s New: %s",
                  oldRule.getField(), oldRule.getType(), newFieldTypeMap.get(oldRule.getField())));
        }
      }
    }
  }
}

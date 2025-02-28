package com.linkedin.metadata.aspect.plugins.spring.validation;

import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.mycompany.dq.DataQualityRules;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/**
 * Same as the non-Spring example however this is an example of using Spring to inject the plugins.
 *
 * <p>This also allows use of other Spring enabled libraries
 */
@Component
@Import(CustomDataQualityRulesConfig.class)
public class CustomDataQualityRulesValidator extends AspectPayloadValidator {
  @Autowired
  @Qualifier("myCustomMessage")
  private String myCustomMessage;

  private AspectPluginConfig config;

  @PostConstruct
  public void message() {
    System.out.println(myCustomMessage);
  }

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {

    return mcpItems.stream()
        .map(
            item -> {
              DataQualityRules rules = new DataQualityRules(item.getRecordTemplate().data());
              // Enforce at least 1 rule
              return rules.getRules().isEmpty()
                  ? AspectValidationException.forItem(item, "At least one rule is required.")
                  : null;
            })
        .filter(Objects::nonNull);
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
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
                            return AspectValidationException.forItem(
                                changeMCP,
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

  @Nonnull
  @Override
  public AspectPluginConfig getConfig() {
    return config;
  }

  @Override
  public CustomDataQualityRulesValidator setConfig(@Nonnull AspectPluginConfig config) {
    this.config = config;
    return this;
  }
}

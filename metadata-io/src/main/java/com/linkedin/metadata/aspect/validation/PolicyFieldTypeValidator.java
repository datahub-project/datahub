package com.linkedin.metadata.aspect.validation;

import com.datahub.authorization.EntityFieldType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.PolicyMatchCriterion;
import com.linkedin.policy.PolicyMatchFilter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Validator to ensure that policy filter field types and privilege constraint field types are valid
 * EntityFieldType enum values.
 */
@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class PolicyFieldTypeValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  private static final Set<String> VALID_ENTITY_FIELD_TYPES =
      Arrays.stream(EntityFieldType.values())
          .map(EntityFieldType::name)
          .collect(Collectors.toSet());

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    mcpItems.forEach(
        item -> {
          DataHubPolicyInfo policyInfo = item.getAspect(DataHubPolicyInfo.class);
          if (policyInfo != null && policyInfo.hasResources()) {
            if (policyInfo.getResources().hasFilter()) {
              validateFilter(item, policyInfo.getResources().getFilter(), exceptions);
            }
            if (policyInfo.getResources().hasPrivilegeConstraints()) {
              validateFilter(item, policyInfo.getResources().getPrivilegeConstraints(), exceptions);
            }
          }
        });

    return exceptions.streamAllExceptions();
  }

  private void validateFilter(
      BatchItem item, PolicyMatchFilter filter, ValidationExceptionCollection exceptions) {
    if (filter != null && filter.hasCriteria()) {
      for (PolicyMatchCriterion criterion : filter.getCriteria()) {
        String field = criterion.getField();
        if (!VALID_ENTITY_FIELD_TYPES.contains(field)) {
          exceptions.addException(
              AspectValidationException.forItem(
                  item,
                  String.format(
                      "Invalid field type '%s'. Must be one of: %s",
                      field, String.join(", ", VALID_ENTITY_FIELD_TYPES))));
        }
      }
    }
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}

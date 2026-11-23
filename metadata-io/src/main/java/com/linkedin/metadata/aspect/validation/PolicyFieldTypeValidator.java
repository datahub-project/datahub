package com.linkedin.metadata.aspect.validation;

import com.datahub.authorization.EntityFieldType;
import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.PatchOperationUtils;
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
import javax.annotation.Nullable;
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
      OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    // Field types are defined in code, so a newer version adding one doesn't bump the schema
    // version; any stored value at the same criteria field counts.
    final StoredAspectValues stored = StoredAspectValues.any(operationContext, retrieverContext);

    mcpItems.forEach(
        item -> {
          if (ChangeType.PATCH.equals(item.getChangeType()) && item instanceof MCPItem) {
            validatePatchItem((MCPItem) item, exceptions, stored);
            return;
          }
          validatePolicyInfo(item, item.getAspect(DataHubPolicyInfo.class), exceptions, stored);
        });

    return exceptions.streamAllExceptions();
  }

  /**
   * A patch item carries only its delta; rebuild a partial policy from each add/replace operation
   * (e.g. a value at {@code /resources/filter} becomes {@code {"resources":{"filter":<value>}}})
   * and validate the field types it contains. Unparseable values are left to schema validation at
   * merge time.
   */
  private void validatePatchItem(
      MCPItem item, ValidationExceptionCollection exceptions, StoredAspectValues stored) {
    PatchOperationUtils.addAndReplaceValues(item)
        .forEach(
            op ->
                PatchOperationUtils.nestValueAtObjectPath(op.getFirst(), op.getSecond())
                    .ifPresent(
                        nested -> {
                          try {
                            validatePolicyInfo(
                                item,
                                RecordUtils.toRecordTemplate(
                                    DataHubPolicyInfo.class, nested.toString()),
                                exceptions,
                                stored);
                          } catch (RuntimeException e) {
                            // unparseable delta — schema validation rejects it at merge time
                          }
                        }));
  }

  private void validatePolicyInfo(
      BatchItem item,
      DataHubPolicyInfo policyInfo,
      ValidationExceptionCollection exceptions,
      StoredAspectValues stored) {
    if (policyInfo != null && policyInfo.hasResources()) {
      if (policyInfo.getResources().hasFilter()) {
        validateFilter(
            item, policyInfo.getResources().getFilter(), "/resources/filter", exceptions, stored);
      }
      if (policyInfo.getResources().hasPrivilegeConstraints()) {
        validateFilter(
            item,
            policyInfo.getResources().getPrivilegeConstraints(),
            "/resources/privilegeConstraints",
            exceptions,
            stored);
      }
    }
  }

  private void validateFilter(
      BatchItem item,
      PolicyMatchFilter filter,
      @Nonnull final String filterPath,
      ValidationExceptionCollection exceptions,
      StoredAspectValues stored) {
    if (filter != null && filter.hasCriteria()) {
      for (PolicyMatchCriterion criterion : filter.getCriteria()) {
        String field = criterion.getField();
        // A field type a newer version added (read after a rollback) that the policy already
        // stores is kept on re-save; only newly added unknown field types are rejected.
        if (!VALID_ENTITY_FIELD_TYPES.contains(field)
            && !stored.contains(item, filterPath + "/criteria/*/field", field)) {
          exceptions.addException(
              AspectValidationException.forItem(item, invalidFieldTypeMessage(field)));
        }
      }
    }
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    // Index-addressed and append ops on a criteria array (e.g. /resources/filter/criteria/0 or
    // /criteria/-) cannot be rebuilt as a partial policy at the request stage, so the patched
    // policy is checked here. Only field types the stored policy did not already have at the same
    // filter are reported: stored data must not block unrelated patches.
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    changeMCPs.stream()
        .filter(PatchOperationUtils::isAppliedPatch)
        .forEach(
            item -> {
              DataHubPolicyInfo before = item.getPreviousAspect(DataHubPolicyInfo.class);
              DataHubPolicyInfo after = item.getAspect(DataHubPolicyInfo.class);
              reportIntroducedFieldTypes(
                  item, filter(after, false), filter(before, false), exceptions);
              reportIntroducedFieldTypes(
                  item, filter(after, true), filter(before, true), exceptions);
            });
    return exceptions.streamAllExceptions();
  }

  @Nullable
  private static PolicyMatchFilter filter(
      @Nullable DataHubPolicyInfo policyInfo, boolean privilegeConstraints) {
    if (policyInfo == null || !policyInfo.hasResources()) {
      return null;
    }
    return privilegeConstraints
        ? policyInfo.getResources().getPrivilegeConstraints()
        : policyInfo.getResources().getFilter();
  }

  private static Set<String> criteriaFields(@Nullable PolicyMatchFilter filter) {
    if (filter == null || !filter.hasCriteria()) {
      return Set.of();
    }
    return filter.getCriteria().stream()
        .map(PolicyMatchCriterion::getField)
        .collect(Collectors.toSet());
  }

  private static void reportIntroducedFieldTypes(
      ChangeMCP item,
      @Nullable PolicyMatchFilter after,
      @Nullable PolicyMatchFilter before,
      ValidationExceptionCollection exceptions) {
    Set<String> stored = criteriaFields(before);
    criteriaFields(after).stream()
        .filter(field -> !VALID_ENTITY_FIELD_TYPES.contains(field) && !stored.contains(field))
        .forEach(field -> exceptions.addException(item, invalidFieldTypeMessage(field)));
  }

  private static String invalidFieldTypeMessage(String field) {
    return String.format(
        "Invalid field type '%s'. Must be one of: %s",
        field, String.join(", ", VALID_ENTITY_FIELD_TYPES));
  }
}

package com.linkedin.metadata.kafka.hook.rule;

import static com.linkedin.metadata.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.rule.AssertionAssignmentRuleInfo;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.assertions.AssertionAssignmentRuleServiceFactory;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.service.AssertionAssignmentRuleService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/**
 * MCL bridge hook that reacts to assertion assignment rule events.
 *
 * <p>This is implemented as an MCL Hook rather than an MCPSideEffect because the upsert path
 * requires OperationContext and SystemEntityClient (via AssertionAssignmentRuleService), which are
 * not available in the MCPSideEffect's RetrieverContext. The future async cleanup path (removing
 * managed assertions via SearchBasedAssertionAssignmentRuleRunner) also requires service-layer
 * access. Transactional deletion of the backing test entity is handled separately by
 * AssertionAssignmentRuleDeleteSideEffect.
 *
 * <p>Specifically, this hook performs the following operations:
 *
 * <p>1. When an assertion assignment rule is created or updated, the backing metadata test
 * automation is upserted and Phase 1 execution is launched.
 *
 * <p>2. When an assertion assignment rule is hard deleted, any async cleanup (e.g., removing
 * managed assertions) is performed.
 */
@Slf4j
@Component
@Import({AssertionAssignmentRuleServiceFactory.class, SystemAuthenticationFactory.class})
public class AssertionAssignmentRuleHook implements MetadataChangeLogHook {

  private static final Set<ChangeType> SUPPORTED_UPDATE_TYPES =
      ImmutableSet.of(
          ChangeType.UPSERT,
          ChangeType.CREATE,
          ChangeType.CREATE_ENTITY,
          ChangeType.RESTATE,
          ChangeType.PATCH);

  private final AssertionAssignmentRuleService assertionAssignmentRuleService;
  private final boolean isEnabled;

  private OperationContext systemOperationContext;
  @Getter private final String consumerGroupSuffix;

  @Autowired
  public AssertionAssignmentRuleHook(
      @Nonnull final AssertionAssignmentRuleService assertionAssignmentRuleService,
      @Nonnull @Value("${assertionAssignmentRules.hook.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${assertionAssignmentRules.hook.consumerGroupSuffix}")
          String consumerGroupSuffix) {
    this.assertionAssignmentRuleService =
        Objects.requireNonNull(
            assertionAssignmentRuleService, "assertionAssignmentRuleService is required");
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public AssertionAssignmentRuleHook(
      @Nonnull final AssertionAssignmentRuleService assertionAssignmentRuleService,
      @Nonnull Boolean isEnabled) {
    this(assertionAssignmentRuleService, isEnabled, "");
  }

  @Override
  public AssertionAssignmentRuleHook init(@Nonnull OperationContext systemOperationContext) {
    this.systemOperationContext = systemOperationContext;
    return this;
  }

  @Override
  public boolean isEnabled() {
    return isEnabled;
  }

  @Override
  public void invoke(@Nonnull final MetadataChangeLog event) {
    if (isEnabled && isEligibleForProcessing(event)) {
      if (isRuleInfoUpdated(event)) {
        handleRuleInfoUpdated(event);
      } else if (isRuleDeleted(event)) {
        handleRuleDeleted(event);
      }
    }
  }

  private void handleRuleInfoUpdated(@Nonnull final MetadataChangeLog event) {
    final AssertionAssignmentRuleInfo ruleInfo =
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(),
            event.getAspect().getContentType(),
            AssertionAssignmentRuleInfo.class);
    assertionAssignmentRuleService.upsertAssertionAssignmentRuleAutomation(
        systemOperationContext, event.getEntityUrn(), ruleInfo);
  }

  private void handleRuleDeleted(@Nonnull final MetadataChangeLog event) {
    assertionAssignmentRuleService.removeAssertionAssignmentRuleAutomation(
        systemOperationContext, event.getEntityUrn());
  }

  private boolean isEligibleForProcessing(@Nonnull final MetadataChangeLog event) {
    return isRuleInfoUpdated(event) || isRuleDeleted(event);
  }

  private boolean isRuleInfoUpdated(@Nonnull final MetadataChangeLog event) {
    return ASSERTION_ASSIGNMENT_RULE_ENTITY_NAME.equals(event.getEntityType())
        && SUPPORTED_UPDATE_TYPES.contains(event.getChangeType())
        && ASSERTION_ASSIGNMENT_RULE_INFO_ASPECT_NAME.equals(event.getAspectName());
  }

  private boolean isRuleDeleted(@Nonnull final MetadataChangeLog event) {
    return ASSERTION_ASSIGNMENT_RULE_ENTITY_NAME.equals(event.getEntityType())
        && ChangeType.DELETE.equals(event.getChangeType())
        && ASSERTION_ASSIGNMENT_RULE_KEY_ASPECT_NAME.equals(event.getAspectName());
  }
}

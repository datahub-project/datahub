package com.linkedin.metadata.test.action.assertion;

import com.linkedin.assertion.rule.AssertionAssignmentRuleInfo;
import com.linkedin.assertion.rule.AssertionAssignmentRuleMode;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.AssertionAssignmentRuleService;
import com.linkedin.metadata.service.util.AssertionAssignmentRuleParams;
import com.linkedin.metadata.test.action.Action;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Action that upserts managed assertions, monitors, and subscriptions for an assertion assignment
 * rule on matching entities. Delegates to {@link AssertionAssignmentRuleService} for the actual
 * lifecycle management; this action handles orchestration, capacity tracking, and per-entity error
 * isolation.
 */
@RequiredArgsConstructor
@Slf4j
public class UpsertAssertionAssignmentRuleAction implements Action {

  private final AssertionAssignmentRuleService ruleService;
  private final int maxMonitors;

  @Override
  public ActionType getActionType() {
    return ActionType.UPSERT_ASSERTION_ASSIGNMENT_RULE;
  }

  @Override
  public void validate(ActionParameters params) throws InvalidActionParamsException {
    if (!params.getParams().containsKey(AssertionAssignmentRuleParams.RULE_URN)
        || params.getParams().get(AssertionAssignmentRuleParams.RULE_URN).size() != 1) {
      throw new InvalidActionParamsException(
          "Action parameters are missing the required 'ruleUrn' parameter.");
    }
  }

  @Override
  public void apply(@Nonnull OperationContext opContext, List<Urn> urns, ActionParameters params)
      throws InvalidOperandException {
    final Urn ruleUrn =
        UrnUtils.getUrn(params.getParams().get(AssertionAssignmentRuleParams.RULE_URN).get(0));

    final AssertionAssignmentRuleInfo ruleInfo = ruleService.getRuleInfo(opContext, ruleUrn);
    if (ruleInfo == null) {
      log.warn("Assertion assignment rule {} not found, skipping upsert action", ruleUrn);
      return;
    }

    if (ruleInfo.getMode() == AssertionAssignmentRuleMode.DISABLED) {
      log.info("Assertion assignment rule {} is DISABLED, skipping upsert action", ruleUrn);
      return;
    }

    final int currentMonitorCount = ruleService.countActiveMonitors(opContext);
    int remainingCapacity = maxMonitors - currentMonitorCount;

    for (Urn entityUrn : urns) {
      if (remainingCapacity <= 0) {
        log.warn(
            "Monitor limit reached ({}/{}), skipping remaining entities for rule {}",
            currentMonitorCount,
            maxMonitors,
            ruleUrn);
        break;
      }
      try {
        int newMonitorsCount = processEntity(opContext, entityUrn, ruleUrn, ruleInfo);
        remainingCapacity -= newMonitorsCount;
      } catch (Exception e) {
        log.error(
            "Failed to process entity {} for assertion assignment rule {}", entityUrn, ruleUrn, e);
      }
    }
  }

  /**
   * Process a single entity: create/update assertions, monitors, and subscriptions.
   *
   * @return number of new monitors created
   */
  private int processEntity(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn ruleUrn,
      @Nonnull final AssertionAssignmentRuleInfo ruleInfo)
      throws Exception {
    int newMonitorsCount = 0;

    if (ruleInfo.hasFreshnessConfig() && ruleInfo.getFreshnessConfig().isEnabled()) {
      newMonitorsCount +=
          ruleService.upsertManagedFreshnessAssertion(
              opContext, entityUrn, ruleUrn, ruleInfo.getFreshnessConfig());
    }

    if (ruleInfo.hasVolumeConfig() && ruleInfo.getVolumeConfig().isEnabled()) {
      newMonitorsCount +=
          ruleService.upsertManagedVolumeAssertion(
              opContext, entityUrn, ruleUrn, ruleInfo.getVolumeConfig());
    }

    if (ruleInfo.hasSubscriptionConfig()) {
      ruleService.syncSubscriptionsForEntity(
          opContext, entityUrn, ruleUrn, ruleInfo.getSubscriptionConfig());
    }

    return newMonitorsCount;
  }
}

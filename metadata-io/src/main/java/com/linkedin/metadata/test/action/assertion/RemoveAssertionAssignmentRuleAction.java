package com.linkedin.metadata.test.action.assertion;

import com.google.common.collect.Lists;
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
 * Action that removes managed assertions for an assertion assignment rule from non-matching
 * entities. Delegates to {@link AssertionAssignmentRuleService} for the actual removal; monitors
 * are cleaned up by a separate hook when the assertion is deleted. Subscriptions are NOT removed
 * (additive-only).
 *
 * <p>URNs are partitioned into batches of {@link #BATCH_SIZE} and each batch is retried up to
 * {@link #MAX_RETRIES} times with linear backoff on transient failures. A batch failure does not
 * block subsequent batches.
 */
@Slf4j
@RequiredArgsConstructor
public class RemoveAssertionAssignmentRuleAction implements Action {

  static final int MAX_RETRIES = 3;
  static final long RETRY_BASE_DELAY_MS = 500;
  static final int BATCH_SIZE = 50;

  private final AssertionAssignmentRuleService ruleService;

  @Override
  public ActionType getActionType() {
    return ActionType.REMOVE_ASSERTION_ASSIGNMENT_RULE;
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

    for (List<Urn> batch : Lists.partition(urns, BATCH_SIZE)) {
      applyWithRetry(opContext, ruleUrn, batch);
    }
  }

  private void applyWithRetry(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn ruleUrn,
      @Nonnull final List<Urn> batch) {
    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        ruleService.removeManagedAssertionsForEntities(opContext, ruleUrn, batch);
        return;
      } catch (Exception e) {
        if (attempt == MAX_RETRIES) {
          log.error(
              "Failed to remove assertions for batch of {} entities under rule {} after {} attempts",
              batch.size(),
              ruleUrn,
              MAX_RETRIES,
              e);
        } else {
          log.warn(
              "Attempt {}/{} failed for batch of {} entities under rule {}, retrying",
              attempt,
              MAX_RETRIES,
              batch.size(),
              ruleUrn,
              e);
          try {
            Thread.sleep(RETRY_BASE_DELAY_MS * attempt);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.error(
                "Retry interrupted for batch of {} entities under rule {}", batch.size(), ruleUrn);
            return;
          }
        }
      }
    }
  }
}

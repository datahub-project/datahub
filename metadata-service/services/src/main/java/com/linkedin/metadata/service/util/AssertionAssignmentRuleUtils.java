package com.linkedin.metadata.service.util;

import static com.linkedin.metadata.Constants.TEST_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;

/**
 * Shared utility methods for assertion assignment rules. This class ensures consistent URN
 * derivation across both creation and deletion paths.
 */
public final class AssertionAssignmentRuleUtils {

  private AssertionAssignmentRuleUtils() {}

  /**
   * Derives the backing test entity URN from the assertion assignment rule URN. The test entity is
   * used to implement the automation that keeps assertions assigned based on the rule's filter
   * criteria.
   *
   * <p>This method must be used by both creation (AssertionAssignmentRuleTestBuilder) and deletion
   * (AssertionAssignmentRuleDeleteSideEffect) paths to ensure they target the same entity.
   */
  public static Urn createTestUrnForAssertionAssignmentRule(@Nonnull final Urn ruleUrn) {
    final String testId = URLEncoder.encode(ruleUrn.toString(), StandardCharsets.UTF_8);
    return UrnUtils.getUrn(String.format("urn:li:%s:%s", TEST_ENTITY_NAME, testId));
  }
}

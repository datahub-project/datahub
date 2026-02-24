package com.linkedin.metadata.service.util;

import static com.linkedin.metadata.Constants.TEST_ENTITY_NAME;

import com.linkedin.assertion.AssertionType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import javax.annotation.Nonnull;

/**
 * Shared utility methods for assertion assignment rules. This class ensures consistent URN
 * derivation across both creation and deletion paths.
 */
public final class AssertionAssignmentRuleUtils {

  // Pipe is safe as a separator: URNs and AssertionType enum values never contain '|'.
  private static final String COMPOSITE_KEY_SEPARATOR = "|";

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

  /**
   * Derives a deterministic assertion URN from (ruleUrn, entityUrn, assertionType). Uses UUID v3
   * (MD5-based) so the same inputs always produce the same URN, enabling direct PK lookups instead
   * of ES search queries.
   */
  public static Urn createAssertionUrnForRule(
      @Nonnull final Urn ruleUrn,
      @Nonnull final Urn entityUrn,
      @Nonnull final AssertionType assertionType) {
    final String compositeKey =
        String.join(
            COMPOSITE_KEY_SEPARATOR,
            ruleUrn.toString(),
            entityUrn.toString(),
            assertionType.toString());
    final String assertionId =
        UUID.nameUUIDFromBytes(compositeKey.getBytes(StandardCharsets.UTF_8)).toString();
    return UrnUtils.getUrn(String.format("urn:li:assertion:%s", assertionId));
  }
}

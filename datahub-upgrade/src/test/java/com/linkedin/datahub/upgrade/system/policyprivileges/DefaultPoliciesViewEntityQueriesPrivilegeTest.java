package com.linkedin.datahub.upgrade.system.policyprivileges;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

/**
 * Guards the invariant behind {@link BackfillViewEntityQueriesPrivilegeStep}'s fresh-install
 * premise: every DEFAULT policy that grants {@code VIEW_ENTITY_PAGE} must also grant {@code
 * VIEW_ENTITY_QUERIES}, so fresh installs never depend on the backfill (which exists for upgrades
 * and may be disabled via BOOTSTRAP_SYSTEM_UPDATE_VIEW_ENTITY_QUERIES_PRIVILEGE_ENABLED).
 *
 * <p>This checks the copy of {@code boot/policies.json} bundled with datahub-upgrade — the repo
 * carries a second copy under metadata-service/war and the two have drifted before (the privilege
 * was added to the war copy only, leaving inhibited fresh installs without default grants).
 */
public class DefaultPoliciesViewEntityQueriesPrivilegeTest {

  @Test
  public void everyDefaultPolicyGrantingViewEntityPageAlsoGrantsViewEntityQueries()
      throws Exception {
    InputStream policies = getClass().getClassLoader().getResourceAsStream("boot/policies.json");
    assertNotNull(policies, "boot/policies.json not found on datahub-upgrade classpath");

    JsonNode root = new ObjectMapper().readTree(policies);
    List<String> violations = new ArrayList<>();
    for (JsonNode policy : root) {
      JsonNode privileges = policy.path("info").path("privileges");
      boolean viewEntityPage = false;
      boolean viewEntityQueries = false;
      for (JsonNode p : privileges) {
        viewEntityPage |= "VIEW_ENTITY_PAGE".equals(p.asText());
        viewEntityQueries |= "VIEW_ENTITY_QUERIES".equals(p.asText());
      }
      if (viewEntityPage && !viewEntityQueries) {
        violations.add(policy.path("urn").asText());
      }
    }
    assertTrue(
        violations.isEmpty(),
        "default policies grant VIEW_ENTITY_PAGE without VIEW_ENTITY_QUERIES (fresh installs "
            + "would depend on the upgrade-only backfill): "
            + violations);
  }

  /**
   * On a fresh install, {@code VIEW_ENTITY_QUERIES} alone is not resource-scoped in any meaningful
   * way for the average user: the default all-users grant is unscoped ({@code resources} absent),
   * so every user can already see every dataset's entity page and its queries wherever subjects are
   * recorded. Withholding {@code VIEW_ALL_QUERIES} from that same policy would only ever hide
   * orphan queries (no recorded subjects) from ordinary users, with no corresponding dataset-level
   * restriction to justify it — so the all-users policy carries both privileges. An administrator
   * who wants a genuinely restrictive posture must replace or scope this policy; that decision is
   * unaffected by this default.
   */
  @Test
  public void allUsersDefaultPolicyGrantsViewAllQueries() throws Exception {
    InputStream policies = getClass().getClassLoader().getResourceAsStream("boot/policies.json");
    assertNotNull(policies, "boot/policies.json not found on datahub-upgrade classpath");

    JsonNode root = new ObjectMapper().readTree(policies);
    JsonNode allUsersPolicy = null;
    for (JsonNode policy : root) {
      if (policy.path("info").path("actors").path("allUsers").asBoolean(false)
          && "urn:li:dataHubPolicy:view-entity-page-all".equals(policy.path("urn").asText())) {
        allUsersPolicy = policy;
        break;
      }
    }
    assertNotNull(
        allUsersPolicy,
        "expected default policy urn:li:dataHubPolicy:view-entity-page-all not found");

    boolean hasViewAllQueries = false;
    for (JsonNode p : allUsersPolicy.path("info").path("privileges")) {
      hasViewAllQueries |= "VIEW_ALL_QUERIES".equals(p.asText());
    }
    assertTrue(
        hasViewAllQueries,
        "the default all-users policy must grant VIEW_ALL_QUERIES, matching its unscoped "
            + "VIEW_ENTITY_QUERIES grant");
  }

  /**
   * Contrast case: the resource-owner policy is deliberately NOT included in the all-users grant
   * above — ownership-based scoping is a real per-asset restriction (unlike the all-users policy's
   * absent resource filter), so it is not equivalent, and VIEW_ALL_QUERIES should not be assumed to
   * spread to it by extension of this change.
   */
  @Test
  public void assetOwnersDefaultPolicyDoesNotGrantViewAllQueries() throws Exception {
    InputStream policies = getClass().getClassLoader().getResourceAsStream("boot/policies.json");
    assertNotNull(policies, "boot/policies.json not found on datahub-upgrade classpath");

    JsonNode root = new ObjectMapper().readTree(policies);
    JsonNode ownersPolicy = null;
    for (JsonNode policy : root) {
      if ("urn:li:dataHubPolicy:asset-owners-metadata-policy".equals(policy.path("urn").asText())) {
        ownersPolicy = policy;
        break;
      }
    }
    assertNotNull(
        ownersPolicy,
        "expected default policy urn:li:dataHubPolicy:asset-owners-metadata-policy not found");

    boolean hasViewAllQueries = false;
    for (JsonNode p : ownersPolicy.path("info").path("privileges")) {
      hasViewAllQueries |= "VIEW_ALL_QUERIES".equals(p.asText());
    }
    assertFalse(
        hasViewAllQueries,
        "the asset-owners policy grants VIEW_ENTITY_QUERIES on owned assets only — it should "
            + "not also carry the platform-wide VIEW_ALL_QUERIES bypass");
  }
}

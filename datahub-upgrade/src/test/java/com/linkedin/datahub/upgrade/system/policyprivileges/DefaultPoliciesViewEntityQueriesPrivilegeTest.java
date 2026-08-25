package com.linkedin.datahub.upgrade.system.policyprivileges;

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
}

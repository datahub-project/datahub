package com.linkedin.metadata.authorization;

import static org.testng.Assert.assertTrue;

import java.util.Set;
import org.testng.annotations.Test;

public class PoliciesConfigTest {

  @Test
  public void testReadInheritance() {
    assertTrue(
        PoliciesConfig.lookupAPIPrivilege(ApiGroup.ENTITY, ApiOperation.READ)
            .containsAll(
                Set.of(
                    Conjunctive.of(PoliciesConfig.VIEW_ENTITY_PRIVILEGE),
                    Conjunctive.of(PoliciesConfig.VIEW_ENTITY_PAGE_PRIVILEGE),
                    Conjunctive.of(PoliciesConfig.DELETE_ENTITY_PRIVILEGE),
                    Conjunctive.of(PoliciesConfig.GET_ENTITY_PRIVILEGE),
                    Conjunctive.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE))),
        "Expected most privileges to imply VIEW");
  }

  @Test
  public void testManageConjoin() {
    assertTrue(
        PoliciesConfig.lookupAPIPrivilege(ApiGroup.ENTITY, ApiOperation.MANAGE)
            .contains(
                Conjunctive.of(
                    PoliciesConfig.EDIT_ENTITY_PRIVILEGE, PoliciesConfig.DELETE_ENTITY_PRIVILEGE)),
        "Expected MANAGE to require both EDIT and DELETE");
  }

  @Test
  public void testEntityType() {
    assertTrue(
        PoliciesConfig.lookupEntityAPIPrivilege("dataset", ApiOperation.MANAGE)
            .contains(
                Conjunctive.of(
                    PoliciesConfig.EDIT_ENTITY_PRIVILEGE, PoliciesConfig.DELETE_ENTITY_PRIVILEGE)),
        "Expected MANAGE on dataset to require both EDIT and DELETE");

    assertTrue(
        PoliciesConfig.lookupEntityAPIPrivilege("dataHubPolicy", ApiOperation.MANAGE)
            .contains(Conjunctive.of(PoliciesConfig.MANAGE_POLICIES_PRIVILEGE)),
        "Expected MANAGE permission directly on dataHubPolicy entity");
  }
}

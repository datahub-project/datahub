package com.linkedin.metadata.authorization;

import static com.linkedin.metadata.authorization.PoliciesConfig.DELETE_ENTITY_PRIVILEGE;
import static com.linkedin.metadata.authorization.PoliciesConfig.EDIT_ENTITY_PRIVILEGE;
import static com.linkedin.metadata.authorization.PoliciesConfig.EDIT_LINEAGE_PRIVILEGE;
import static com.linkedin.metadata.authorization.PoliciesConfig.VIEW_ENTITY_PAGE_PRIVILEGE;
import static org.testng.Assert.assertEquals;

import java.util.List;
import org.testng.annotations.Test;

public class DisjunctiveTest {

  @Test
  public void testDisjointHelper() {
    assertEquals(
        Disjunctive.disjoint(VIEW_ENTITY_PAGE_PRIVILEGE, VIEW_ENTITY_PAGE_PRIVILEGE),
        new Disjunctive<>(List.of(new Conjunctive<>(List.of(VIEW_ENTITY_PAGE_PRIVILEGE)))));
  }

  @Test
  public void testDisjointConjoin() {
    Disjunctive<Conjunctive<PoliciesConfig.Privilege>> a =
        new Disjunctive<>(
            List.of(Conjunctive.of(EDIT_ENTITY_PRIVILEGE), Conjunctive.of(EDIT_LINEAGE_PRIVILEGE)));
    Disjunctive<Conjunctive<PoliciesConfig.Privilege>> b =
        new Disjunctive<>(
            List.of(
                Conjunctive.of(DELETE_ENTITY_PRIVILEGE), Conjunctive.of(EDIT_LINEAGE_PRIVILEGE)));

    assertEquals(
        Disjunctive.conjoin(a, b),
        Disjunctive.of(
            Conjunctive.of(EDIT_ENTITY_PRIVILEGE, DELETE_ENTITY_PRIVILEGE),
            Conjunctive.of(EDIT_ENTITY_PRIVILEGE, EDIT_LINEAGE_PRIVILEGE),
            Conjunctive.of(EDIT_LINEAGE_PRIVILEGE, DELETE_ENTITY_PRIVILEGE),
            Conjunctive.of(EDIT_LINEAGE_PRIVILEGE)));
  }
}

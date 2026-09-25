package com.linkedin.metadata.authorization;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class PoliciesConfigGlossaryNodeTagsTest {

  @Test
  public void testGlossaryNodePrivilegesIncludeEditTags() {
    assertTrue(
        PoliciesConfig.GLOSSARY_NODE_PRIVILEGES
            .getPrivileges()
            .contains(PoliciesConfig.EDIT_ENTITY_TAGS_PRIVILEGE),
        "Glossary term groups must allow EDIT_ENTITY_TAGS so users can tag nodes");
  }
}

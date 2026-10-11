package com.linkedin.metadata.authorization;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class PoliciesConfigGlossaryTermTagsTest {

  @Test
  public void testGlossaryTermPrivilegesIncludeEditTags() {
    assertTrue(
        PoliciesConfig.GLOSSARY_TERM_PRIVILEGES
            .getPrivileges()
            .contains(PoliciesConfig.EDIT_ENTITY_TAGS_PRIVILEGE),
        "Glossary terms must allow EDIT_ENTITY_TAGS so users can tag terms");
  }
}

package com.linkedin.metadata.authorization;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class PoliciesConfigIngestionSourceViewTest {

  @Test
  public void testIngestionSourcePrivilegesIncludeViewEntityPage() {
    assertTrue(
        PoliciesConfig.INGESTION_SOURCE_PRIVILEGES
            .getPrivileges()
            .contains(PoliciesConfig.VIEW_ENTITY_PAGE_PRIVILEGE),
        "Ingestion sources must allow VIEW_ENTITY_PAGE so policies can grant read access to them");
  }
}

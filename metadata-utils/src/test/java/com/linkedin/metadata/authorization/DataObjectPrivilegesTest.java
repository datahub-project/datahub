package com.linkedin.metadata.authorization;

import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.testng.annotations.Test;

/**
 * Guards that {@link PoliciesConfig#DATA_OBJECT_PRIVILEGES} grants the governance privileges that
 * correspond to the aspects/capabilities the dataObject entity actually supports (ownership, tags,
 * glossary terms, domains, status, deprecation, data products, applications, properties, plus
 * view/delete). The original entity shipped with status/deprecation/applications/data-products
 * privileges missing; this asserts containment of the required set so that drift fails loudly.
 *
 * <p>Containment (not full equality) on purpose: the full list also carries docs/links/create
 * privileges, and asserting an exact set would be brittle against unrelated additions.
 */
public class DataObjectPrivilegesTest {

  @Test
  public void testDataObjectGrantsRequiredGovernancePrivileges() {
    final List<PoliciesConfig.Privilege> required =
        ImmutableList.of(
            PoliciesConfig.VIEW_ENTITY_PAGE_PRIVILEGE,
            PoliciesConfig.EDIT_ENTITY_OWNERS_PRIVILEGE,
            PoliciesConfig.EDIT_ENTITY_TAGS_PRIVILEGE,
            PoliciesConfig.EDIT_ENTITY_GLOSSARY_TERMS_PRIVILEGE,
            PoliciesConfig.EDIT_ENTITY_DOMAINS_PRIVILEGE,
            PoliciesConfig.EDIT_ENTITY_STATUS_PRIVILEGE,
            PoliciesConfig.EDIT_ENTITY_DEPRECATION_PRIVILEGE,
            PoliciesConfig.EDIT_ENTITY_DATA_PRODUCTS_PRIVILEGE,
            PoliciesConfig.EDIT_ENTITY_APPLICATIONS_PRIVILEGE,
            PoliciesConfig.EDIT_ENTITY_PROPERTIES_PRIVILEGE,
            PoliciesConfig.DELETE_ENTITY_PRIVILEGE);

    final List<PoliciesConfig.Privilege> granted =
        PoliciesConfig.DATA_OBJECT_PRIVILEGES.getPrivileges();

    for (PoliciesConfig.Privilege privilege : required) {
      assertTrue(
          granted.contains(privilege),
          "DATA_OBJECT_PRIVILEGES is missing required governance privilege: "
              + privilege.getType());
    }
  }
}

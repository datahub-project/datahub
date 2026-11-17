package com.linkedin.datahub.upgrade.system.policies;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

/**
 * Guards the default document policies: every user can create a document, and the owners of a
 * document can edit it. These policies are non-editable, so {@link IngestPoliciesUpgradeStep}
 * re-ingests them on every deploy and a deployment cannot drift away from them.
 */
public class DefaultDocumentPoliciesTest {

  private static final String CREATE_POLICY_URN =
      "urn:li:dataHubPolicy:all-users-create-documents-policy";
  private static final String OWNERS_EDIT_POLICY_URN =
      "urn:li:dataHubPolicy:document-owners-edit-policy";

  private static final Path WAR_POLICIES =
      Paths.get("..", "metadata-service", "war", "src", "main", "resources", "boot")
          .resolve("policies.json");

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void allUsersCanCreateDocuments() throws Exception {
    JsonNode info = documentPolicyInfo(CREATE_POLICY_URN);

    assertTrue(info.path("actors").path("allUsers").asBoolean());
    assertFalse(info.path("actors").path("resourceOwners").asBoolean());
    // CREATE_ENTITY only: an EDIT_ENTITY grant here would let every user edit every document.
    assertEquals(privileges(info), List.of("CREATE_ENTITY"));
  }

  @Test
  public void documentOwnersCanEditDocuments() throws Exception {
    JsonNode info = documentPolicyInfo(OWNERS_EDIT_POLICY_URN);

    assertTrue(info.path("actors").path("resourceOwners").asBoolean());
    assertFalse(info.path("actors").path("allUsers").asBoolean());
    assertEquals(privileges(info), List.of("EDIT_ENTITY"));
  }

  /**
   * System-update loads the copy bundled with datahub-upgrade, while the policy lint reads the copy
   * under metadata-service/war. A change to one copy only either ships unlinted or never ships.
   */
  @Test
  public void bothPolicyFilesAreIdentical() throws Exception {
    assertTrue(Files.exists(WAR_POLICIES), "expected a policies.json at " + WAR_POLICIES);

    JsonNode war;
    try (InputStream in = Files.newInputStream(WAR_POLICIES)) {
      war = MAPPER.readTree(in);
    }
    assertEquals(
        loadBundledPolicies(),
        war,
        "boot/policies.json in datahub-upgrade and metadata-service/war must be identical");
  }

  /**
   * Returns the info of a default policy after checking the properties every default document
   * policy shares: active, non-editable, and scoped to the document entity type only.
   */
  private static JsonNode documentPolicyInfo(String urn) throws Exception {
    JsonNode info = null;
    for (JsonNode policy : loadBundledPolicies()) {
      if (urn.equals(policy.path("urn").asText())) {
        info = policy.path("info");
      }
    }
    assertNotNull(info, "default policy not found: " + urn);

    assertFalse(info.path("editable").asBoolean(true), urn + " must not be editable");
    assertEquals(info.path("state").asText(), "ACTIVE");
    assertEquals(info.path("type").asText(), "METADATA");

    JsonNode resources = info.path("resources");
    assertFalse(resources.path("allResources").asBoolean(true));
    JsonNode criteria = resources.path("filter").path("criteria");
    assertEquals(criteria.size(), 1, urn + " must have exactly one resource criterion");
    JsonNode criterion = criteria.get(0);
    assertEquals(criterion.path("field").asText(), "TYPE");
    assertEquals(criterion.path("condition").asText(), "EQUALS");
    assertEquals(criterion.path("values").size(), 1);
    assertEquals(criterion.path("values").get(0).asText(), "document");
    return info;
  }

  private static List<String> privileges(JsonNode info) {
    List<String> privileges = new ArrayList<>();
    info.path("privileges").forEach(p -> privileges.add(p.asText()));
    return privileges;
  }

  private static JsonNode loadBundledPolicies() throws Exception {
    try (InputStream in =
        DefaultDocumentPoliciesTest.class
            .getClassLoader()
            .getResourceAsStream("boot/policies.json")) {
      assertNotNull(in, "boot/policies.json not found on datahub-upgrade classpath");
      return MAPPER.readTree(in);
    }
  }
}

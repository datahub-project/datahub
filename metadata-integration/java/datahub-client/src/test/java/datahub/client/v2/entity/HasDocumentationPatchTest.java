package datahub.client.v2.entity;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.Documentation;
import com.linkedin.common.DocumentationAssociation;
import com.linkedin.common.DocumentationAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.mxe.MetadataChangeProposal;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.Test;

/**
 * Unit tests for HasDocumentation patch operations.
 *
 * <p>These tests verify that documentation patch operations produce correct MCPs and that the
 * resulting patch payloads have the expected structure.
 */
public class HasDocumentationPatchTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // ==================== addDocumentation (unattributed) ====================

  @Test
  public void testAddDocumentation() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.addDocumentation("This is a test documentation entry.");

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have a pending patch", patches.isEmpty());
    assertEquals("documentation", patches.get(0).getAspectName());
    assertEquals(ChangeType.PATCH, patches.get(0).getChangeType());
  }

  @Test
  public void testAddMultipleDocumentations_AccumulatesInOnePatch() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.addDocumentation("First doc.");
    dataset.addDocumentation("Second doc.");

    // Both calls accumulate into the same patch builder, so one patch MCP is emitted
    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertEquals("Both documentation adds should produce one accumulated patch", 1, patches.size());
    assertEquals("documentation", patches.get(0).getAspectName());
  }

  // ==================== addDocumentation (attributed) ====================

  @Test
  public void testAddAttributedDocumentation() throws URISyntaxException {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();
    Urn source = Urn.createFromString("urn:li:corpuser:datahub");

    dataset.addDocumentation("Attributed documentation.", source);

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have a pending patch", patches.isEmpty());
    assertEquals("documentation", patches.get(0).getAspectName());
  }

  // ==================== removeDocumentation ====================

  @Test
  public void testRemoveDocumentation() throws URISyntaxException {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();
    Urn source = Urn.createFromString("urn:li:corpuser:datahub");

    dataset.removeDocumentation(source);

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have a pending remove patch", patches.isEmpty());
    assertEquals("documentation", patches.get(0).getAspectName());
    assertEquals(ChangeType.PATCH, patches.get(0).getChangeType());
  }

  // ==================== removeAllDocumentation ====================

  @Test
  public void testRemoveAllDocumentation() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.removeAllDocumentation();

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have a pending remove-all patch", patches.isEmpty());
    assertEquals("documentation", patches.get(0).getAspectName());
    assertEquals(ChangeType.PATCH, patches.get(0).getChangeType());
  }

  // ==================== patch payload structure ====================

  @Test
  public void testAddDocumentationBuildsCorrectPatch() throws IOException {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.addDocumentation("Patch format verification.");

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertEquals(1, patches.size());

    MetadataChangeProposal mcp = patches.get(0);
    assertEquals("documentation", mcp.getAspectName());
    assertEquals(ChangeType.PATCH, mcp.getChangeType());
    assertEquals("application/json-patch+json", mcp.getAspect().getContentType());

    String payloadJson = mcp.getAspect().getValue().asString(StandardCharsets.UTF_8);
    JsonNode payload = OBJECT_MAPPER.readTree(payloadJson);

    // Plain patch: a JSON array, no arrayPrimaryKeys envelope
    assertTrue("add must produce a plain JSON array (no envelope)", payload.isArray());
    assertFalse("patch array must not be empty", payload.isEmpty());

    JsonNode firstOp = payload.get(0);
    assertEquals("add", firstOp.get("op").asText());
  }

  @Test
  public void testRemoveAllDocumentationBuildsWildcardPatch() throws IOException {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.removeAllDocumentation();

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertEquals(1, patches.size());

    String payloadJson = patches.get(0).getAspect().getValue().asString(StandardCharsets.UTF_8);
    JsonNode payload = OBJECT_MAPPER.readTree(payloadJson);

    JsonNode patchOps = payload.get("patch");
    assertTrue("patch must be an array", patchOps.isArray());
    JsonNode removeOp = patchOps.get(0);
    assertEquals("remove", removeOp.get("op").asText());

    // The remove-all path should be "/documentations" (no wildcard)
    String path = removeOp.get("path").asText();
    assertEquals("/documentations", path);
  }

  // ==================== getDocumentations ====================

  @Test
  public void testGetDocumentations_ReturnsNullWhenNoAspect() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    assertNull(
        "getDocumentations() should return null when no Documentation aspect is cached",
        dataset.getDocumentations());
  }

  @Test
  public void testGetDocumentations_ReturnsListFromCachedAspect() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    // Inject a Documentation aspect into the aspect cache
    Documentation documentation = new Documentation();
    DocumentationAssociationArray docsArray = new DocumentationAssociationArray();
    DocumentationAssociation assoc = new DocumentationAssociation();
    assoc.setDocumentation("Cached documentation text.");
    docsArray.add(assoc);
    documentation.setDocumentations(docsArray);

    dataset.cache.put("documentation", documentation, AspectSource.LOCAL, false);

    List<DocumentationAssociation> result = dataset.getDocumentations();
    assertNotNull("getDocumentations() should return the cached list", result);
    assertEquals(1, result.size());
    assertEquals("Cached documentation text.", result.get(0).getDocumentation());
  }

  // ==================== setDocumentations (full replace) ====================

  @Test
  public void testSetDocumentations_ProducesUpsertMcp() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.setDocumentations(List.of("Doc 1", "Doc 2"));

    // Full-replace goes through pending MCPs, not patch MCPs
    List<com.linkedin.mxe.MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertTrue("setDocumentations should not produce patch MCPs", patches.isEmpty());

    List<datahub.event.MetadataChangeProposalWrapper> pendingMcps = dataset.getPendingMCPs();
    boolean hasDocumentationAspect =
        pendingMcps.stream().anyMatch(mcp -> "documentation".equals(mcp.getAspectName()));
    assertTrue(
        "getPendingMCPs() should contain a 'documentation' aspect MCP", hasDocumentationAspect);
  }
}

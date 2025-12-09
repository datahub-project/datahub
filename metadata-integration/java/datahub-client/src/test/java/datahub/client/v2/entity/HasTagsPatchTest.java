package datahub.client.v2.entity;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.data.ByteString;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.v2.operations.EntityClient;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

/**
 * Unit tests for HasTags patch operation transformation logic.
 *
 * <p>These tests verify the patch-to-full-aspect transformation that's needed for compatibility
 * with older DataHub versions that have broken tag patch operations.
 */
public class HasTagsPatchTest {

  @Test
  public void testApplyPatchOperations_AddSingleTag() throws IOException, URISyntaxException {
    // Create initial GlobalTags with one existing tag
    GlobalTags current = new GlobalTags();
    TagAssociationArray tags = new TagAssociationArray();
    TagAssociation existingTag = new TagAssociation();
    existingTag.setTag(TagUrn.createFromString("urn:li:tag:existing"));
    tags.add(existingTag);
    current.setTags(tags);

    // Create patch MCP that adds a new tag
    MetadataChangeProposal patch = createAddTagPatch("urn:li:tag:newtag");

    // Apply patch operations
    GlobalTags result = HasTags.applyPatchOperations(current, patch);

    // Verify the result has both tags
    assertEquals(2, result.getTags().size());
    assertTrue(
        result.getTags().stream()
            .anyMatch(t -> t.getTag().toString().equals("urn:li:tag:existing")));
    assertTrue(
        result.getTags().stream().anyMatch(t -> t.getTag().toString().equals("urn:li:tag:newtag")));
  }

  @Test
  public void testApplyPatchOperations_AddDuplicateTag() throws IOException, URISyntaxException {
    // Create initial GlobalTags with one tag
    GlobalTags current = new GlobalTags();
    TagAssociationArray tags = new TagAssociationArray();
    TagAssociation existingTag = new TagAssociation();
    existingTag.setTag(TagUrn.createFromString("urn:li:tag:existing"));
    tags.add(existingTag);
    current.setTags(tags);

    // Create patch that tries to add the same tag
    MetadataChangeProposal patch = createAddTagPatch("urn:li:tag:existing");

    // Apply patch operations
    GlobalTags result = HasTags.applyPatchOperations(current, patch);

    // Verify no duplicate - still only one tag
    assertEquals(1, result.getTags().size());
    assertEquals("urn:li:tag:existing", result.getTags().get(0).getTag().toString());
  }

  @Test
  public void testApplyPatchOperations_RemoveTag() throws IOException, URISyntaxException {
    // Create initial GlobalTags with two tags
    GlobalTags current = new GlobalTags();
    TagAssociationArray tags = new TagAssociationArray();

    TagAssociation tag1 = new TagAssociation();
    tag1.setTag(TagUrn.createFromString("urn:li:tag:tag1"));
    tags.add(tag1);

    TagAssociation tag2 = new TagAssociation();
    tag2.setTag(TagUrn.createFromString("urn:li:tag:tag2"));
    tags.add(tag2);

    current.setTags(tags);

    // Create patch that removes tag1
    MetadataChangeProposal patch = createRemoveTagPatch("urn:li:tag:tag1");

    // Apply patch operations
    GlobalTags result = HasTags.applyPatchOperations(current, patch);

    // Verify only tag2 remains
    assertEquals(1, result.getTags().size());
    assertEquals("urn:li:tag:tag2", result.getTags().get(0).getTag().toString());
  }

  @Test
  public void testApplyPatchOperations_RemoveNonExistentTag()
      throws IOException, URISyntaxException {
    // Create initial GlobalTags with one tag
    GlobalTags current = new GlobalTags();
    TagAssociationArray tags = new TagAssociationArray();
    TagAssociation tag1 = new TagAssociation();
    tag1.setTag(TagUrn.createFromString("urn:li:tag:existing"));
    tags.add(tag1);
    current.setTags(tags);

    // Create patch that tries to remove a non-existent tag
    MetadataChangeProposal patch = createRemoveTagPatch("urn:li:tag:nonexistent");

    // Apply patch operations
    GlobalTags result = HasTags.applyPatchOperations(current, patch);

    // Verify the existing tag is still there
    assertEquals(1, result.getTags().size());
    assertEquals("urn:li:tag:existing", result.getTags().get(0).getTag().toString());
  }

  @Test
  public void testApplyPatchOperations_MultipleOperations() throws IOException, URISyntaxException {
    // Create initial GlobalTags with three tags
    GlobalTags current = new GlobalTags();
    TagAssociationArray tags = new TagAssociationArray();

    for (String tagName : new String[] {"tag1", "tag2", "tag3"}) {
      TagAssociation tag = new TagAssociation();
      tag.setTag(TagUrn.createFromString("urn:li:tag:" + tagName));
      tags.add(tag);
    }
    current.setTags(tags);

    // Create patch with multiple operations: add tag4, remove tag2
    String patchJson =
        "{"
            + "\"patch\": ["
            + "  {"
            + "    \"op\": \"add\","
            + "    \"path\": \"/tags/urn:li:tag:tag4\","
            + "    \"value\": {\"tag\": \"urn:li:tag:tag4\"}"
            + "  },"
            + "  {"
            + "    \"op\": \"remove\","
            + "    \"path\": \"/tags/urn:li:tag:tag2\""
            + "  }"
            + "]"
            + "}";

    MetadataChangeProposal patch = new MetadataChangeProposal();
    GenericAspect aspect = new GenericAspect();
    aspect.setValue(ByteString.copyString(patchJson, StandardCharsets.UTF_8));
    aspect.setContentType("application/json");
    patch.setAspect(aspect);

    // Apply patch operations
    GlobalTags result = HasTags.applyPatchOperations(current, patch);

    // Verify result has tag1, tag3, tag4 (tag2 removed, tag4 added)
    assertEquals(3, result.getTags().size());
    assertTrue(
        result.getTags().stream().anyMatch(t -> t.getTag().toString().equals("urn:li:tag:tag1")));
    assertFalse(
        result.getTags().stream().anyMatch(t -> t.getTag().toString().equals("urn:li:tag:tag2")));
    assertTrue(
        result.getTags().stream().anyMatch(t -> t.getTag().toString().equals("urn:li:tag:tag3")));
    assertTrue(
        result.getTags().stream().anyMatch(t -> t.getTag().toString().equals("urn:li:tag:tag4")));
  }

  @Test
  public void testApplyPatchOperations_EmptyInitialTags() throws IOException, URISyntaxException {
    // Create initial GlobalTags with empty tags
    GlobalTags current = new GlobalTags();
    current.setTags(new TagAssociationArray());

    // Create patch that adds a tag
    MetadataChangeProposal patch = createAddTagPatch("urn:li:tag:firsttag");

    // Apply patch operations
    GlobalTags result = HasTags.applyPatchOperations(current, patch);

    // Verify the tag was added
    assertEquals(1, result.getTags().size());
    assertEquals("urn:li:tag:firsttag", result.getTags().get(0).getTag().toString());
  }

  @Test
  public void testApplyPatchOperations_InvalidTagUrn() throws IOException {
    // Create initial GlobalTags
    GlobalTags current = new GlobalTags();
    current.setTags(new TagAssociationArray());

    // Create patch with an invalid tag URN
    String patchJson =
        "{"
            + "\"patch\": ["
            + "  {"
            + "    \"op\": \"add\","
            + "    \"path\": \"/tags/invalid-urn\","
            + "    \"value\": {\"tag\": \"not-a-valid-urn\"}"
            + "  }"
            + "]"
            + "}";

    MetadataChangeProposal patch = new MetadataChangeProposal();
    GenericAspect aspect = new GenericAspect();
    aspect.setValue(ByteString.copyString(patchJson, StandardCharsets.UTF_8));
    aspect.setContentType("application/json");
    patch.setAspect(aspect);

    // Apply patch operations - should not throw, just log warning
    GlobalTags result = HasTags.applyPatchOperations(current, patch);

    // Verify no tags were added due to invalid URN
    assertEquals(0, result.getTags().size());
  }

  @Test
  public void testApplyPatchOperations_MalformedPatchNoPatchArray()
      throws IOException, URISyntaxException {
    // Create initial GlobalTags
    GlobalTags current = new GlobalTags();
    TagAssociationArray tags = new TagAssociationArray();
    TagAssociation tag = new TagAssociation();
    tag.setTag(TagUrn.createFromString("urn:li:tag:existing"));
    tags.add(tag);
    current.setTags(tags);

    // Create malformed patch without 'patch' array
    String patchJson = "{\"notAPatch\": \"value\"}";

    MetadataChangeProposal patch = new MetadataChangeProposal();
    GenericAspect aspect = new GenericAspect();
    aspect.setValue(ByteString.copyString(patchJson, StandardCharsets.UTF_8));
    aspect.setContentType("application/json");
    patch.setAspect(aspect);

    // Apply patch operations - should return current unchanged
    GlobalTags result = HasTags.applyPatchOperations(current, patch);

    // Verify the original tag is still there and nothing changed
    assertEquals(1, result.getTags().size());
    assertEquals("urn:li:tag:existing", result.getTags().get(0).getTag().toString());
  }

  @Test
  public void testApplyPatchOperations_TagWithContext() throws IOException, URISyntaxException {
    // Create initial GlobalTags
    GlobalTags current = new GlobalTags();
    current.setTags(new TagAssociationArray());

    // Create patch that adds a tag with context
    String patchJson =
        "{"
            + "\"patch\": ["
            + "  {"
            + "    \"op\": \"add\","
            + "    \"path\": \"/tags/urn:li:tag:contextTag\","
            + "    \"value\": {"
            + "      \"tag\": \"urn:li:tag:contextTag\","
            + "      \"context\": \"automated-classification\""
            + "    }"
            + "  }"
            + "]"
            + "}";

    MetadataChangeProposal patch = new MetadataChangeProposal();
    GenericAspect aspect = new GenericAspect();
    aspect.setValue(ByteString.copyString(patchJson, StandardCharsets.UTF_8));
    aspect.setContentType("application/json");
    patch.setAspect(aspect);

    // Apply patch operations
    GlobalTags result = HasTags.applyPatchOperations(current, patch);

    // Verify the tag was added with context
    assertEquals(1, result.getTags().size());
    TagAssociation addedTag = result.getTags().get(0);
    assertEquals("urn:li:tag:contextTag", addedTag.getTag().toString());
    assertEquals("automated-classification", addedTag.getContext());
  }

  @Test
  public void testTransformPatchToFullAspect_WithExistingTags()
      throws IOException, ExecutionException, InterruptedException, URISyntaxException {
    // Mock EntityClient
    EntityClient mockClient = mock(EntityClient.class);

    // Setup existing tags on server
    GlobalTags existingTags = new GlobalTags();
    TagAssociationArray tags = new TagAssociationArray();
    TagAssociation tag1 = new TagAssociation();
    tag1.setTag(TagUrn.createFromString("urn:li:tag:existing"));
    tags.add(tag1);
    existingTags.setTags(tags);

    // Wrap in AspectWithMetadata as getAspect now returns this wrapper
    datahub.client.v2.operations.AspectWithMetadata<GlobalTags> aspectWithMetadata =
        datahub.client.v2.operations.AspectWithMetadata.from(existingTags, null);
    when(mockClient.getAspect(any(), eq(GlobalTags.class))).thenReturn(aspectWithMetadata);

    // Create patch that adds a new tag
    MetadataChangeProposal patch = createAddTagPatch("urn:li:tag:newtag");
    patch.setEntityUrn(TagUrn.createFromString("urn:li:tag:test"));
    patch.setEntityType("dataset");
    patch.setAspectName("globalTags");
    patch.setChangeType(ChangeType.PATCH);

    // Transform patch to full aspect
    MetadataChangeProposal fullMcp = HasTags.transformPatchToFullAspect(patch, mockClient);

    // Verify the result is a full UPSERT MCP
    assertEquals(ChangeType.UPSERT, fullMcp.getChangeType());
    assertEquals("globalTags", fullMcp.getAspectName());
    assertEquals("dataset", fullMcp.getEntityType());
    assertNotNull(fullMcp.getAspect());

    // Verify the aspect contains both tags
    String aspectJson = fullMcp.getAspect().getValue().asString(StandardCharsets.UTF_8);
    assertTrue(aspectJson.contains("urn:li:tag:existing"));
    assertTrue(aspectJson.contains("urn:li:tag:newtag"));
  }

  @Test
  public void testTransformPatchToFullAspect_NoExistingTags()
      throws IOException, ExecutionException, InterruptedException, URISyntaxException {
    // Mock EntityClient that returns non-existent wrapper (no existing tags)
    EntityClient mockClient = mock(EntityClient.class);
    when(mockClient.getAspect(any(), eq(GlobalTags.class)))
        .thenReturn(datahub.client.v2.operations.AspectWithMetadata.nonExistent());

    // Create patch that adds a tag
    MetadataChangeProposal patch = createAddTagPatch("urn:li:tag:firsttag");
    patch.setEntityUrn(TagUrn.createFromString("urn:li:tag:test"));
    patch.setEntityType("dataset");
    patch.setAspectName("globalTags");
    patch.setChangeType(ChangeType.PATCH);

    // Transform patch to full aspect
    MetadataChangeProposal fullMcp = HasTags.transformPatchToFullAspect(patch, mockClient);

    // Verify the result is a full UPSERT MCP
    assertEquals(ChangeType.UPSERT, fullMcp.getChangeType());
    assertEquals("globalTags", fullMcp.getAspectName());

    // Verify the aspect contains the new tag
    String aspectJson = fullMcp.getAspect().getValue().asString(StandardCharsets.UTF_8);
    assertTrue(aspectJson.contains("urn:li:tag:firsttag"));
  }

  /** Helper method to create an ADD tag patch MCP. */
  private MetadataChangeProposal createAddTagPatch(String tagUrn) {
    String patchJson =
        "{"
            + "\"patch\": ["
            + "  {"
            + "    \"op\": \"add\","
            + "    \"path\": \"/tags/"
            + tagUrn
            + "\","
            + "    \"value\": {\"tag\": \""
            + tagUrn
            + "\"}"
            + "  }"
            + "]"
            + "}";

    MetadataChangeProposal patch = new MetadataChangeProposal();
    GenericAspect aspect = new GenericAspect();
    aspect.setValue(ByteString.copyString(patchJson, StandardCharsets.UTF_8));
    aspect.setContentType("application/json");
    patch.setAspect(aspect);
    return patch;
  }

  /** Helper method to create a REMOVE tag patch MCP. */
  private MetadataChangeProposal createRemoveTagPatch(String tagUrn) {
    String patchJson =
        "{"
            + "\"patch\": ["
            + "  {"
            + "    \"op\": \"remove\","
            + "    \"path\": \"/tags/"
            + tagUrn
            + "\""
            + "  }"
            + "]"
            + "}";

    MetadataChangeProposal patch = new MetadataChangeProposal();
    GenericAspect aspect = new GenericAspect();
    aspect.setValue(ByteString.copyString(patchJson, StandardCharsets.UTF_8));
    aspect.setContentType("application/json");
    patch.setAspect(aspect);
    return patch;
  }
}

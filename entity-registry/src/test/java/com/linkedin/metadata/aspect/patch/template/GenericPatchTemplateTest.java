package com.linkedin.metadata.aspect.patch.template;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.template.common.GenericPatchTemplate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GenericPatchTemplateTest {

  @Test
  public void testGlobalTagsWithAttribution() throws Exception {
    // Prepare initial GlobalTags
    GlobalTags initialTags = new GlobalTags();
    TagAssociationArray initialTagsArray = new TagAssociationArray();
    initialTags.setTags(initialTagsArray);

    // Prepare GenericJsonPatch with nested keys
    GenericJsonPatch.PatchOp patchOp1 = new GenericJsonPatch.PatchOp();
    patchOp1.setOp("add");
    patchOp1.setPath("/tags/urn:li:platformResource:source1/urn:li:tag:tag1");
    patchOp1.setValue(
        Map.of(
            "tag",
            "urn:li:tag:tag1",
            "attribution",
            Map.of(
                "source", "urn:li:platformResource:source1",
                "actor", "urn:li:corpuser:datahub",
                "time", 0)));

    GenericJsonPatch.PatchOp patchOp2 = new GenericJsonPatch.PatchOp();
    patchOp2.setOp("add");
    patchOp2.setPath("/tags/urn:li:platformResource:source2/urn:li:tag:tag2");
    patchOp2.setValue(
        Map.of(
            "tag",
            "urn:li:tag:tag2",
            "attribution",
            Map.of(
                "source", "urn:li:platformResource:source2",
                "actor", "urn:li:corpuser:datahub",
                "time", 0)));

    GenericJsonPatch genericJsonPatch =
        GenericJsonPatch.builder()
            .patch(List.of(patchOp1, patchOp2))
            .arrayPrimaryKeys(Map.of("tags", Arrays.asList("attribution" + "␟" + "source", "tag")))
            .build();

    // Create GenericPatchTemplate
    GenericPatchTemplate<GlobalTags> template =
        GenericPatchTemplate.<GlobalTags>builder()
            .genericJsonPatch(genericJsonPatch)
            .templateType(GlobalTags.class)
            .templateDefault(new GlobalTags())
            .build();

    // Apply patch
    GlobalTags patchedTags = template.applyPatch(initialTags);

    // Verify patch results
    Assert.assertNotNull(patchedTags);
    Assert.assertNotNull(patchedTags.getTags());
    Assert.assertEquals(patchedTags.getTags().size(), 2);

    // Create a map of tag to its attribution source for unordered comparison
    Map<String, String> tagToSourceMap =
        patchedTags.getTags().stream()
            .collect(
                Collectors.toMap(
                    tag -> tag.getTag().toString(),
                    tag ->
                        tag.getAttribution() != null
                            ? tag.getAttribution().getSource().toString()
                            : null));

    // Check specific tags exist with correct attribution
    Assert.assertTrue(tagToSourceMap.containsKey("urn:li:tag:tag1"));
    Assert.assertEquals(tagToSourceMap.get("urn:li:tag:tag1"), "urn:li:platformResource:source1");

    Assert.assertTrue(tagToSourceMap.containsKey("urn:li:tag:tag2"));
    Assert.assertEquals(tagToSourceMap.get("urn:li:tag:tag2"), "urn:li:platformResource:source2");
  }

  @Test
  public void testGlobalTagsWithAttributionNonAttributed() throws Exception {
    // Prepare initial GlobalTags
    GlobalTags initialTags = new GlobalTags();
    TagAssociationArray initialTagsArray =
        new TagAssociationArray(
            new TagAssociation().setTag(TagUrn.createFromString("urn:li:tag:existingTag")));
    initialTags.setTags(initialTagsArray);

    // Prepare GenericJsonPatch with nested keys
    GenericJsonPatch.PatchOp patchOp1 = new GenericJsonPatch.PatchOp();
    patchOp1.setOp("add");
    patchOp1.setPath("/tags/urn:li:platformResource:source1/urn:li:tag:tag1");
    patchOp1.setValue(
        Map.of(
            "tag",
            "urn:li:tag:tag1",
            "attribution",
            Map.of(
                "source", "urn:li:platformResource:source1",
                "actor", "urn:li:corpuser:datahub",
                "time", 0)));

    GenericJsonPatch.PatchOp patchOp2 = new GenericJsonPatch.PatchOp();
    patchOp2.setOp("add");
    patchOp2.setPath("/tags/urn:li:platformResource:source2/urn:li:tag:tag2");
    patchOp2.setValue(
        Map.of(
            "tag",
            "urn:li:tag:tag2",
            "attribution",
            Map.of(
                "source", "urn:li:platformResource:source2",
                "actor", "urn:li:corpuser:datahub",
                "time", 0)));

    GenericJsonPatch genericJsonPatch =
        GenericJsonPatch.builder()
            .patch(List.of(patchOp1, patchOp2))
            .arrayPrimaryKeys(Map.of("tags", Arrays.asList("attribution" + "␟" + "source", "tag")))
            .build();

    // Create GenericPatchTemplate
    GenericPatchTemplate<GlobalTags> template =
        GenericPatchTemplate.<GlobalTags>builder()
            .genericJsonPatch(genericJsonPatch)
            .templateType(GlobalTags.class)
            .templateDefault(new GlobalTags())
            .build();

    // Apply patch
    GlobalTags patchedTags = template.applyPatch(initialTags);

    // Verify patch results
    Assert.assertNotNull(patchedTags);
    Assert.assertNotNull(patchedTags.getTags());
    Assert.assertEquals(patchedTags.getTags().size(), 3);

    // Create a map of tag to its attribution source for unordered comparison
    Map<String, String> tagToSourceMap =
        patchedTags.getTags().stream()
            .collect(
                Collectors.toMap(
                    tag -> tag.getTag().toString(),
                    tag ->
                        tag.getAttribution() != null
                            ? tag.getAttribution().getSource().toString()
                            : ""));

    // Check existing tag
    Assert.assertTrue(tagToSourceMap.containsKey("urn:li:tag:existingTag"));
    Assert.assertEquals(tagToSourceMap.get("urn:li:tag:existingTag"), "");

    // Check added tags
    Assert.assertTrue(tagToSourceMap.containsKey("urn:li:tag:tag1"));
    Assert.assertEquals(tagToSourceMap.get("urn:li:tag:tag1"), "urn:li:platformResource:source1");

    Assert.assertTrue(tagToSourceMap.containsKey("urn:li:tag:tag2"));
    Assert.assertEquals(tagToSourceMap.get("urn:li:tag:tag2"), "urn:li:platformResource:source2");
  }

  @Test
  public void testGlobalTagsWithExistingAttributedTagAndNewUnattributedTag() throws Exception {
    // Prepare initial GlobalTags with an existing attributed tag
    GlobalTags initialTags = new GlobalTags();
    TagAssociation existingTag =
        new TagAssociation()
            .setTag(TagUrn.createFromString("urn:li:tag:existingTag"))
            .setAttribution(
                new MetadataAttribution()
                    .setSource(UrnUtils.getUrn("urn:li:platformResource:existingSource"))
                    .setActor(UrnUtils.getUrn("urn:li:corpuser:existingUser"))
                    .setTime(System.currentTimeMillis()));

    TagAssociationArray initialTagsArray = new TagAssociationArray(existingTag);
    initialTags.setTags(initialTagsArray);

    // Prepare GenericJsonPatch to add an unattributed tag
    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp("add");
    patchOp.setPath("/tags//urn:li:tag:newTag");
    patchOp.setValue(
        Map.of(
            "tag", "urn:li:tag:newTag"
            // No attribution included
            ));

    GenericJsonPatch genericJsonPatch =
        GenericJsonPatch.builder()
            .patch(List.of(patchOp))
            .arrayPrimaryKeys(Map.of("tags", Arrays.asList("attribution" + "␟" + "source", "tag")))
            .build();

    // Create GenericPatchTemplate
    GenericPatchTemplate<GlobalTags> template =
        GenericPatchTemplate.<GlobalTags>builder()
            .genericJsonPatch(genericJsonPatch)
            .templateType(GlobalTags.class)
            .templateDefault(new GlobalTags())
            .build();

    // Apply patch
    GlobalTags patchedTags = template.applyPatch(initialTags);

    // Verify patch results
    Assert.assertNotNull(patchedTags);
    Assert.assertNotNull(patchedTags.getTags());
    Assert.assertEquals(patchedTags.getTags().size(), 2);

    // Create a map of tag to its attribution source for unordered comparison
    Map<String, String> tagToSourceMap =
        patchedTags.getTags().stream()
            .collect(
                Collectors.toMap(
                    tag -> tag.getTag().toString(),
                    tag ->
                        tag.getAttribution() != null
                            ? tag.getAttribution().getSource().toString()
                            : ""));

    // Check existing attributed tag
    Assert.assertTrue(tagToSourceMap.containsKey("urn:li:tag:existingTag"));
    Assert.assertEquals(
        tagToSourceMap.get("urn:li:tag:existingTag"), "urn:li:platformResource:existingSource");

    // Check new unattributed tag
    Assert.assertTrue(tagToSourceMap.containsKey("urn:li:tag:newTag"));
    Assert.assertEquals(tagToSourceMap.get("urn:li:tag:newTag"), "");
  }
}

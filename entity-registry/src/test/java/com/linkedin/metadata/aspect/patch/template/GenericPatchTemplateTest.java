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

    // Prepare GenericJsonPatch with nested keys (tag first, then attribution.source)
    GenericJsonPatch.PatchOp patchOp1 = new GenericJsonPatch.PatchOp();
    patchOp1.setOp("add");
    patchOp1.setPath("/tags/urn:li:tag:tag1/urn:li:platformResource:source1");
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
    patchOp2.setPath("/tags/urn:li:tag:tag2/urn:li:platformResource:source2");
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
            .arrayPrimaryKeys(Map.of("tags", Arrays.asList("tag", "attribution" + "␟" + "source")))
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

    // Prepare GenericJsonPatch with nested keys (tag first, then attribution.source)
    GenericJsonPatch.PatchOp patchOp1 = new GenericJsonPatch.PatchOp();
    patchOp1.setOp("add");
    patchOp1.setPath("/tags/urn:li:tag:tag1/urn:li:platformResource:source1");
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
    patchOp2.setPath("/tags/urn:li:tag:tag2/urn:li:platformResource:source2");
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
            .arrayPrimaryKeys(Map.of("tags", Arrays.asList("tag", "attribution" + "␟" + "source")))
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

  // ---------------------------------------------------------------------------
  // Wildcard expansion tests
  // ---------------------------------------------------------------------------

  /** Builds an attributed TagAssociation for use in wildcard tests. */
  private static TagAssociation attributedTag(String tagUrn, String sourceUrn) throws Exception {
    return new TagAssociation()
        .setTag(TagUrn.createFromString(tagUrn))
        .setAttribution(
            new MetadataAttribution()
                .setSource(UrnUtils.getUrn(sourceUrn))
                .setActor(UrnUtils.getUrn("urn:li:corpuser:datahub"))
                .setTime(0L));
  }

  /** Builds a GenericPatchTemplate for GlobalTags with the compound-key configuration. */
  private static GenericPatchTemplate<GlobalTags> compoundKeyTemplate(
      List<GenericJsonPatch.PatchOp> ops) {
    GenericJsonPatch genericJsonPatch =
        GenericJsonPatch.builder()
            .patch(ops)
            .arrayPrimaryKeys(Map.of("tags", Arrays.asList("tag", "attribution" + "␟" + "source")))
            .build();
    return GenericPatchTemplate.<GlobalTags>builder()
        .genericJsonPatch(genericJsonPatch)
        .templateType(GlobalTags.class)
        .templateDefault(new GlobalTags())
        .build();
  }

  @Test
  public void testWildcardRemoveDeletesAllSourcesForTag() throws Exception {
    // (tagX, srcA), (tagX, srcB), (tagY, srcA) — remove /tags/urn:li:tag:tagX (truncated path
    // removes all attribution source entries for tagX).
    GlobalTags initial = new GlobalTags();
    initial.setTags(
        new TagAssociationArray(
            attributedTag("urn:li:tag:tagX", "urn:li:platformResource:srcA"),
            attributedTag("urn:li:tag:tagX", "urn:li:platformResource:srcB"),
            attributedTag("urn:li:tag:tagY", "urn:li:platformResource:srcA")));

    GenericJsonPatch.PatchOp removeOp = new GenericJsonPatch.PatchOp();
    removeOp.setOp("remove");
    removeOp.setPath("/tags/urn:li:tag:tagX");

    GlobalTags result = compoundKeyTemplate(List.of(removeOp)).applyPatch(initial);

    Assert.assertNotNull(result.getTags());
    List<String> remainingTagUrns =
        result.getTags().stream().map(t -> t.getTag().toString()).collect(Collectors.toList());
    Assert.assertFalse(remainingTagUrns.contains("urn:li:tag:tagX"), "tagX should be gone");
    Assert.assertTrue(remainingTagUrns.contains("urn:li:tag:tagY"), "tagY should survive");
    Assert.assertEquals(result.getTags().size(), 1);
  }

  @Test
  public void testWildcardRemoveOnlyAffectsTargetedTag() throws Exception {
    // (tagX, srcA), (tagX, srcB), (tagY, srcA), (tagY, srcB) — remove /tags/urn:li:tag:tagX
    // (truncated path removes entire tagX sub-tree, leaving tagY entries intact).
    GlobalTags initial = new GlobalTags();
    initial.setTags(
        new TagAssociationArray(
            attributedTag("urn:li:tag:tagX", "urn:li:platformResource:srcA"),
            attributedTag("urn:li:tag:tagX", "urn:li:platformResource:srcB"),
            attributedTag("urn:li:tag:tagY", "urn:li:platformResource:srcA"),
            attributedTag("urn:li:tag:tagY", "urn:li:platformResource:srcB")));

    GenericJsonPatch.PatchOp removeOp = new GenericJsonPatch.PatchOp();
    removeOp.setOp("remove");
    removeOp.setPath("/tags/urn:li:tag:tagX");

    GlobalTags result = compoundKeyTemplate(List.of(removeOp)).applyPatch(initial);

    Assert.assertNotNull(result.getTags());
    List<String> remainingTagUrns =
        result.getTags().stream().map(t -> t.getTag().toString()).collect(Collectors.toList());
    long tagXCount = remainingTagUrns.stream().filter("urn:li:tag:tagX"::equals).count();
    long tagYCount = remainingTagUrns.stream().filter("urn:li:tag:tagY"::equals).count();
    Assert.assertEquals(tagXCount, 0L, "all tagX entries should be removed");
    Assert.assertEquals(tagYCount, 2L, "both tagY entries should survive");
  }

  @Test
  public void testNonWildcardRemoveIsUnchanged() throws Exception {
    // (tagX, srcA), (tagX, srcB) — remove /tags/urn:li:tag:tagX/urn:li:platformResource:srcA (tag
    // first)
    GlobalTags initial = new GlobalTags();
    initial.setTags(
        new TagAssociationArray(
            attributedTag("urn:li:tag:tagX", "urn:li:platformResource:srcA"),
            attributedTag("urn:li:tag:tagX", "urn:li:platformResource:srcB")));

    GenericJsonPatch.PatchOp removeOp = new GenericJsonPatch.PatchOp();
    removeOp.setOp("remove");
    removeOp.setPath("/tags/urn:li:tag:tagX/urn:li:platformResource:srcA");

    GlobalTags result = compoundKeyTemplate(List.of(removeOp)).applyPatch(initial);

    Assert.assertNotNull(result.getTags());
    Assert.assertEquals(result.getTags().size(), 1, "only srcA entry removed, srcB should remain");
    TagAssociation remaining = result.getTags().get(0);
    Assert.assertEquals(remaining.getTag().toString(), "urn:li:tag:tagX");
    Assert.assertNotNull(remaining.getAttribution());
    Assert.assertEquals(
        remaining.getAttribution().getSource().toString(), "urn:li:platformResource:srcB");
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

    // Prepare GenericJsonPatch to add an unattributed tag (tag first, trailing slash for empty
    // source)
    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp("add");
    patchOp.setPath("/tags/urn:li:tag:newTag/");
    patchOp.setValue(
        Map.of(
            "tag", "urn:li:tag:newTag"
            // No attribution included
            ));

    GenericJsonPatch genericJsonPatch =
        GenericJsonPatch.builder()
            .patch(List.of(patchOp))
            .arrayPrimaryKeys(Map.of("tags", Arrays.asList("tag", "attribution" + "␟" + "source")))
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

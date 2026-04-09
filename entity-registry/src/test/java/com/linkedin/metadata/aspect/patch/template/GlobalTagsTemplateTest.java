package com.linkedin.metadata.aspect.patch.template;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.template.common.GenericPatchTemplate;
import com.linkedin.metadata.aspect.patch.template.common.GlobalTagsTemplate;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GlobalTagsTemplateTest {

  private static final GlobalTagsTemplate TEMPLATE = new GlobalTagsTemplate();

  /** Builds an attributed TagAssociation. */
  private static TagAssociation attributedTag(String tagUrn, String sourceUrn) throws Exception {
    return new TagAssociation()
        .setTag(TagUrn.createFromString(tagUrn))
        .setAttribution(
            new MetadataAttribution()
                .setSource(UrnUtils.getUrn(sourceUrn))
                .setActor(UrnUtils.getUrn("urn:li:corpuser:datahub"))
                .setTime(0L));
  }

  @Test
  public void testAddTwoDistinctEntries() throws Exception {
    GlobalTags initial = new GlobalTags();
    initial.setTags(new TagAssociationArray());

    JsonPatch patchA =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/tags/urn:li:tag:tagA")
                        .add(
                            "value",
                            Json.createArrayBuilder()
                                .add(Json.createObjectBuilder().add("tag", "urn:li:tag:tagA"))))
                .build());

    JsonPatch patchB =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/tags/urn:li:tag:tagB")
                        .add(
                            "value",
                            Json.createArrayBuilder()
                                .add(Json.createObjectBuilder().add("tag", "urn:li:tag:tagB"))))
                .build());

    GlobalTags afterA = TEMPLATE.applyPatch(initial, patchA);
    GlobalTags result = TEMPLATE.applyPatch(afterA, patchB);

    Assert.assertNotNull(result.getTags());
    Assert.assertEquals(result.getTags().size(), 2);
    List<String> tagUrns = result.getTags().stream().map(t -> t.getTag().toString()).toList();
    Assert.assertTrue(tagUrns.contains("urn:li:tag:tagA"), "tagA should be present");
    Assert.assertTrue(tagUrns.contains("urn:li:tag:tagB"), "tagB should be present");
  }

  @Test
  public void testUnattributedAddPreservesAttributedDuplicates() throws Exception {
    // Start with (srcA, tagX) and (srcB, tagX) — two attributed entries for the same tag URN.
    GlobalTags initial = new GlobalTags();
    initial.setTags(
        new TagAssociationArray(
            attributedTag("urn:li:tag:tagX", "urn:li:platformResource:srcA"),
            attributedTag("urn:li:tag:tagX", "urn:li:platformResource:srcB")));

    // Plain patch: add tagY (no attribution).
    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/tags/urn:li:tag:tagY")
                        .add(
                            "value",
                            Json.createArrayBuilder()
                                .add(Json.createObjectBuilder().add("tag", "urn:li:tag:tagY"))))
                .build());

    GlobalTags result = TEMPLATE.applyPatch(initial, patch);

    Assert.assertNotNull(result.getTags());
    List<String> tagUrns = result.getTags().stream().map(t -> t.getTag().toString()).toList();
    // Both attributed tagX entries should survive, plus the new tagY.
    long tagXCount = tagUrns.stream().filter("urn:li:tag:tagX"::equals).count();
    long tagYCount = tagUrns.stream().filter("urn:li:tag:tagY"::equals).count();
    Assert.assertEquals(tagXCount, 2L, "both attributed tagX entries should be preserved");
    Assert.assertEquals(tagYCount, 1L, "new tagY entry should be present");
    Assert.assertEquals(result.getTags().size(), 3);
  }

  @Test
  public void testUnattributedRemoveDeletesAllEntriesForTag() throws Exception {
    // (srcA, tagX), (srcB, tagX), (srcC, tagY)
    GlobalTags initial = new GlobalTags();
    initial.setTags(
        new TagAssociationArray(
            attributedTag("urn:li:tag:tagX", "urn:li:platformResource:srcA"),
            attributedTag("urn:li:tag:tagX", "urn:li:platformResource:srcB"),
            attributedTag("urn:li:tag:tagY", "urn:li:platformResource:srcC")));

    // Plain remove of tagX — should delete the entire list at key tagX.
    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "remove")
                        .add("path", "/tags/urn:li:tag:tagX"))
                .build());

    GlobalTags result = TEMPLATE.applyPatch(initial, patch);

    Assert.assertNotNull(result.getTags());
    List<String> tagUrns = result.getTags().stream().map(t -> t.getTag().toString()).toList();
    Assert.assertFalse(tagUrns.contains("urn:li:tag:tagX"), "all tagX entries should be gone");
    Assert.assertTrue(tagUrns.contains("urn:li:tag:tagY"), "tagY entry should survive");
    Assert.assertEquals(result.getTags().size(), 1);
    TagAssociation survivor = result.getTags().get(0);
    Assert.assertNotNull(survivor.getAttribution());
    Assert.assertEquals(
        survivor.getAttribution().getSource().toString(), "urn:li:platformResource:srcC");
  }

  @Test
  public void testUnattributedRemoveDeletesAllEntriesForAttributionSource() throws Exception {
    // (srcA, tagX), (srcB, tagX), (srcC, tagY)
    GlobalTags initial = new GlobalTags();
    initial.setTags(
        new TagAssociationArray(
            attributedTag("urn:li:tag:tagX", "urn:li:platformResource:srcA"),
            attributedTag("urn:li:tag:tagX", "urn:li:platformResource:srcB"),
            attributedTag("urn:li:tag:tagY", "urn:li:platformResource:srcA")));

    // Remove attribution source srcA
    GenericJsonPatch.PatchOp removeOp = new GenericJsonPatch.PatchOp();
    removeOp.setOp("remove");
    removeOp.setPath("/tags/urn:li:platformResource:srcA");
    GenericJsonPatch patch =
        GenericJsonPatch.builder()
            .patch(List.of(removeOp))
            .arrayPrimaryKeys(Map.of("tags", Arrays.asList("attribution␟source", "tag")))
            .build();
    GenericPatchTemplate<GlobalTags> template =
        GenericPatchTemplate.<GlobalTags>builder()
            .genericJsonPatch(patch)
            .templateType(GlobalTags.class)
            .templateDefault(new GlobalTagsTemplate().getDefault())
            .build();

    GlobalTags result = template.applyPatch(initial);

    Assert.assertNotNull(result.getTags());
    Assert.assertEquals(result.getTags().size(), 1);
    TagAssociation survivor = result.getTags().get(0);
    Assert.assertNotNull(survivor.getAttribution());
    Assert.assertEquals(
        survivor.getAttribution().getSource().toString(), "urn:li:platformResource:srcB");
    Assert.assertEquals(survivor.getTag().toString(), "urn:li:tag:tagX");
  }

  @Test
  public void testRemoveOneOfTwoEntries() throws Exception {
    GlobalTags initial = new GlobalTags();
    initial.setTags(
        new TagAssociationArray(
            new TagAssociation().setTag(TagUrn.createFromString("urn:li:tag:tagA")),
            new TagAssociation().setTag(TagUrn.createFromString("urn:li:tag:tagB"))));

    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "remove")
                        .add("path", "/tags/urn:li:tag:tagA"))
                .build());

    GlobalTags result = TEMPLATE.applyPatch(initial, patch);

    Assert.assertNotNull(result.getTags());
    Assert.assertEquals(result.getTags().size(), 1);
    List<String> tagUrns = result.getTags().stream().map(t -> t.getTag().toString()).toList();
    Assert.assertFalse(tagUrns.contains("urn:li:tag:tagA"), "tagA should be removed");
    Assert.assertTrue(tagUrns.contains("urn:li:tag:tagB"), "tagB should remain");
  }

  @Test
  public void testUnattributedAddToDuplicateDoesNotUpsert() throws Exception {
    // (srcA, tagX), (srcB, tagX) — plain add for tagX replaces the whole list at that key.
    GlobalTags initial = new GlobalTags();
    initial.setTags(
        new TagAssociationArray(
            attributedTag("urn:li:tag:tagX", "urn:li:platformResource:srcA"),
            attributedTag("urn:li:tag:tagX", "urn:li:platformResource:srcB")));

    // Plain add of tagX with no attribution — replaces all entries at /tags/urn:li:tag:tagX.
    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/tags/urn:li:tag:tagX")
                        .add(
                            "value",
                            Json.createArrayBuilder()
                                .add(Json.createObjectBuilder().add("tag", "urn:li:tag:tagX"))))
                .build());

    GlobalTags result = TEMPLATE.applyPatch(initial, patch);

    Assert.assertNotNull(result.getTags());
    List<String> tagUrns = result.getTags().stream().map(t -> t.getTag().toString()).toList();
    // A plain add at the tag level replaces the entire sub-map for tagX (including attributed
    // ones).
    long tagXCount = tagUrns.stream().filter("urn:li:tag:tagX"::equals).count();
    Assert.assertEquals(tagXCount, 1L, "plain add at tag level replaces all entries for that tag");
  }
}

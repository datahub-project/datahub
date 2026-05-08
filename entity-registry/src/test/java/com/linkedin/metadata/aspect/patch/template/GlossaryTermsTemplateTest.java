package com.linkedin.metadata.aspect.patch.template;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.template.common.GlossaryTermsTemplate;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GlossaryTermsTemplateTest {

  private static final GlossaryTermsTemplate TEMPLATE = new GlossaryTermsTemplate();

  /** Builds an attributed GlossaryTermAssociation. */
  private static GlossaryTermAssociation attributedTerm(String termUrn, String sourceUrn)
      throws Exception {
    return new GlossaryTermAssociation()
        .setUrn(GlossaryTermUrn.createFromString(termUrn))
        .setAttribution(
            new MetadataAttribution()
                .setSource(UrnUtils.getUrn(sourceUrn))
                .setActor(UrnUtils.getUrn("urn:li:corpuser:datahub"))
                .setTime(0L));
  }

  private static GlossaryTerms initialGlossaryTerms(GlossaryTermAssociation... associations) {
    GlossaryTerms glossaryTerms = new GlossaryTerms();
    glossaryTerms
        .setTerms(new GlossaryTermAssociationArray(Arrays.asList(associations)))
        .setAuditStamp(
            new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:datahub")).setTime(0L));
    return glossaryTerms;
  }

  @Test
  public void testAddTwoDistinctEntries() throws Exception {
    GlossaryTerms initial = initialGlossaryTerms();

    JsonPatch patchA =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/terms/urn:li:glossaryTerm:termA")
                        .add(
                            "value",
                            Json.createArrayBuilder()
                                .add(
                                    Json.createObjectBuilder()
                                        .add("urn", "urn:li:glossaryTerm:termA"))))
                .build());

    JsonPatch patchB =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/terms/urn:li:glossaryTerm:termB")
                        .add(
                            "value",
                            Json.createArrayBuilder()
                                .add(
                                    Json.createObjectBuilder()
                                        .add("urn", "urn:li:glossaryTerm:termB"))))
                .build());

    GlossaryTerms afterA = TEMPLATE.applyPatch(initial, patchA);
    GlossaryTerms result = TEMPLATE.applyPatch(afterA, patchB);

    Assert.assertNotNull(result.getTerms());
    Assert.assertEquals(result.getTerms().size(), 2);
    List<String> termUrns =
        result.getTerms().stream().map(t -> t.getUrn().toString()).collect(Collectors.toList());
    Assert.assertTrue(termUrns.contains("urn:li:glossaryTerm:termA"), "termA should be present");
    Assert.assertTrue(termUrns.contains("urn:li:glossaryTerm:termB"), "termB should be present");
  }

  @Test
  public void testRemoveOneOfTwoEntries() throws Exception {
    GlossaryTerms initial =
        initialGlossaryTerms(
            new GlossaryTermAssociation()
                .setUrn(GlossaryTermUrn.createFromString("urn:li:glossaryTerm:termA")),
            new GlossaryTermAssociation()
                .setUrn(GlossaryTermUrn.createFromString("urn:li:glossaryTerm:termB")));

    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "remove")
                        .add("path", "/terms/urn:li:glossaryTerm:termA"))
                .build());

    GlossaryTerms result = TEMPLATE.applyPatch(initial, patch);

    Assert.assertNotNull(result.getTerms());
    Assert.assertEquals(result.getTerms().size(), 1);
    List<String> termUrns =
        result.getTerms().stream().map(t -> t.getUrn().toString()).collect(Collectors.toList());
    Assert.assertFalse(termUrns.contains("urn:li:glossaryTerm:termA"), "termA should be removed");
    Assert.assertTrue(termUrns.contains("urn:li:glossaryTerm:termB"), "termB should remain");
  }

  @Test
  public void testUnattributedRemoveDeletesAllEntriesForTerm() throws Exception {
    // (srcA, termX), (srcB, termX), (srcC, termY)
    GlossaryTerms initial =
        initialGlossaryTerms(
            attributedTerm("urn:li:glossaryTerm:termX", "urn:li:dataHubAction:srcA"),
            attributedTerm("urn:li:glossaryTerm:termX", "urn:li:dataHubAction:srcB"),
            attributedTerm("urn:li:glossaryTerm:termY", "urn:li:dataHubAction:srcC"));

    // Plain remove of termX — should delete the entire list at key termX.
    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "remove")
                        .add("path", "/terms/urn:li:glossaryTerm:termX"))
                .build());

    GlossaryTerms result = TEMPLATE.applyPatch(initial, patch);

    Assert.assertNotNull(result.getTerms());
    List<String> termUrns =
        result.getTerms().stream().map(t -> t.getUrn().toString()).collect(Collectors.toList());
    Assert.assertFalse(
        termUrns.contains("urn:li:glossaryTerm:termX"), "all termX entries should be gone");
    Assert.assertTrue(termUrns.contains("urn:li:glossaryTerm:termY"), "termY entry should survive");
    Assert.assertEquals(result.getTerms().size(), 1);
    GlossaryTermAssociation survivor = result.getTerms().get(0);
    Assert.assertNotNull(survivor.getAttribution());
    Assert.assertEquals(
        survivor.getAttribution().getSource().toString(), "urn:li:dataHubAction:srcC");
  }

  @Test
  public void testUnattributedAddToDuplicateKeyUpserts() throws Exception {
    // (srcA, termX), (srcB, termX) — plain add for termX replaces the whole list at that key.
    GlossaryTerms initial =
        initialGlossaryTerms(
            attributedTerm("urn:li:glossaryTerm:termX", "urn:li:dataHubAction:srcA"),
            attributedTerm("urn:li:glossaryTerm:termX", "urn:li:dataHubAction:srcB"));

    // Plain add of termX — replaces all entries at the termX level.
    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/terms/urn:li:glossaryTerm:termX")
                        .add(
                            "value",
                            Json.createArrayBuilder()
                                .add(
                                    Json.createObjectBuilder()
                                        .add("urn", "urn:li:glossaryTerm:termX"))))
                .build());

    GlossaryTerms result = TEMPLATE.applyPatch(initial, patch);

    Assert.assertNotNull(result.getTerms());
    List<String> termUrns =
        result.getTerms().stream().map(t -> t.getUrn().toString()).collect(Collectors.toList());
    // A plain add at the term level replaces the entire sub-map for termX.
    long termXCount = termUrns.stream().filter("urn:li:glossaryTerm:termX"::equals).count();
    Assert.assertEquals(termXCount, 1L, "plain add at term level replaces all entries");
  }
}

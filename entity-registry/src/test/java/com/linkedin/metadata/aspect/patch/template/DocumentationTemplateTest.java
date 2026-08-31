package com.linkedin.metadata.aspect.patch.template;

import com.linkedin.common.Documentation;
import com.linkedin.common.DocumentationAssociation;
import com.linkedin.common.DocumentationAssociationArray;
import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.template.common.DocumentationTemplate;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DocumentationTemplateTest {

  private static final String DOC_X = "Documentation for entity X";
  private static final String DOC_Y = "Documentation for entity Y";
  private static final String DOC_Z = "Documentation for entity Z";
  private static final String SRC_A = "urn:li:dataHubAction:srcA";
  private static final String SRC_B = "urn:li:dataHubAction:srcB";
  private static final String SRC_C = "urn:li:dataHubAction:srcC";

  /** Build a DocumentationAssociation with attribution. */
  private static DocumentationAssociation attributed(String docText, String sourceUrn) {
    return new DocumentationAssociation()
        .setDocumentation(docText)
        .setAttribution(
            new MetadataAttribution()
                .setSource(UrnUtils.getUrn(sourceUrn))
                .setActor(UrnUtils.getUrn("urn:li:corpuser:datahub"))
                .setTime(0L));
  }

  // ---------------------------------------------------------------------------
  // Basic add / remove
  // ---------------------------------------------------------------------------

  @Test
  public void testAddTwoDistinctEntries() throws Exception {
    DocumentationTemplate template = new DocumentationTemplate();
    Documentation initial = template.getDefault();

    JsonPatch addA =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/documentations/" + SRC_A)
                        .add(
                            "value",
                            Json.createObjectBuilder()
                                .add("documentation", DOC_X)
                                .add(
                                    "attribution",
                                    Json.createObjectBuilder()
                                        .add("source", SRC_A)
                                        .add("actor", "urn:li:corpuser:datahub")
                                        .add("time", 0))))
                .build());

    Documentation afterA = template.applyPatch(initial, addA);

    JsonPatch addB =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/documentations/" + SRC_B)
                        .add(
                            "value",
                            Json.createObjectBuilder()
                                .add("documentation", DOC_Y)
                                .add(
                                    "attribution",
                                    Json.createObjectBuilder()
                                        .add("source", SRC_B)
                                        .add("actor", "urn:li:corpuser:datahub")
                                        .add("time", 0))))
                .build());

    Documentation result = template.applyPatch(afterA, addB);

    Assert.assertEquals(result.getDocumentations().size(), 2);
    Map<String, String> sourceToDoc =
        result.getDocumentations().stream()
            .collect(
                Collectors.toMap(
                    d -> d.getAttribution().getSource().toString(),
                    DocumentationAssociation::getDocumentation));
    Assert.assertEquals(sourceToDoc.get(SRC_A), DOC_X);
    Assert.assertEquals(sourceToDoc.get(SRC_B), DOC_Y);
  }

  @Test
  public void testRemoveOneOfTwoEntries() throws Exception {
    DocumentationTemplate template = new DocumentationTemplate();
    Documentation initial =
        new Documentation()
            .setDocumentations(
                new DocumentationAssociationArray(
                    Arrays.asList(attributed(DOC_X, SRC_A), attributed(DOC_Y, SRC_B))));

    JsonPatch removeA =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "remove")
                        .add("path", "/documentations/" + SRC_A))
                .build());

    Documentation result = template.applyPatch(initial, removeA);

    Assert.assertEquals(result.getDocumentations().size(), 1);
    Assert.assertEquals(result.getDocumentations().get(0).getDocumentation(), DOC_Y);
    Assert.assertEquals(
        result.getDocumentations().get(0).getAttribution().getSource().toString(), SRC_B);
  }

  // ---------------------------------------------------------------------------
  // Attribution-aware duplicate preservation
  // ---------------------------------------------------------------------------

  @Test
  public void testUnattributedAddPreservesAttributedDuplicates() throws Exception {
    // Start with two attributed docs (srcA, srcB).
    // Add a third attributed doc (srcC) — srcA and srcB must be untouched.
    // Then add an *unattributed* doc — it lands under the "" key and must also coexist.
    DocumentationTemplate template = new DocumentationTemplate();
    Documentation initial =
        new Documentation()
            .setDocumentations(
                new DocumentationAssociationArray(
                    Arrays.asList(attributed(DOC_X, SRC_A), attributed(DOC_X, SRC_B))));

    // Now add an unattributed doc.  No attribution → key is "" → path is "/documentations/".
    JsonPatch addUnattributed =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/documentations/") // empty-string key
                        .add("value", Json.createObjectBuilder().add("documentation", DOC_Y)))
                .build());

    Documentation result = template.applyPatch(initial, addUnattributed);

    Assert.assertEquals(result.getDocumentations().size(), 3);

    // Build a map keyed by source (or "" for unattributed) for easy assertions.
    Map<String, String> sourceToDoc =
        result.getDocumentations().stream()
            .collect(
                Collectors.toMap(
                    d ->
                        d.getAttribution() != null ? d.getAttribution().getSource().toString() : "",
                    DocumentationAssociation::getDocumentation));
    Assert.assertEquals(sourceToDoc.get(SRC_A), DOC_X, "srcA must be preserved");
    Assert.assertEquals(sourceToDoc.get(SRC_B), DOC_X, "srcB must be preserved");
    Assert.assertEquals(
        sourceToDoc.get(""), DOC_Y, "unattributed entry must be present under \"\" key");
  }

  @Test
  public void testUnattributedRemoveDeletesAllEntriesForSource() throws Exception {
    DocumentationTemplate template = new DocumentationTemplate();
    Documentation initial =
        new Documentation()
            .setDocumentations(
                new DocumentationAssociationArray(
                    Arrays.asList(
                        attributed(DOC_X, SRC_A),
                        attributed(DOC_X, SRC_B),
                        attributed(DOC_Z, SRC_C))));

    // Remove the entire srcA bucket — only srcA's entry is deleted.
    JsonPatch removeA =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "remove")
                        .add("path", "/documentations/" + SRC_A))
                .build());

    Documentation result = template.applyPatch(initial, removeA);

    Assert.assertEquals(result.getDocumentations().size(), 2);
    Map<String, String> sourceToDoc =
        result.getDocumentations().stream()
            .collect(
                Collectors.toMap(
                    d -> d.getAttribution().getSource().toString(),
                    DocumentationAssociation::getDocumentation));
    Assert.assertFalse(sourceToDoc.containsKey(SRC_A), "srcA entry should be removed");
    Assert.assertEquals(sourceToDoc.get(SRC_B), DOC_X);
    Assert.assertEquals(sourceToDoc.get(SRC_C), DOC_Z);
  }

  @Test
  public void testUnattributedAddUpsertsSameSource() throws Exception {
    // Adding a new doc for the same source replaces the existing entry.
    DocumentationTemplate template = new DocumentationTemplate();
    Documentation initial =
        new Documentation()
            .setDocumentations(
                new DocumentationAssociationArray(
                    Arrays.asList(attributed(DOC_X, SRC_A), attributed(DOC_Y, SRC_B))));

    JsonPatch upsertA =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/documentations/" + SRC_A)
                        .add(
                            "value",
                            Json.createObjectBuilder()
                                .add("documentation", DOC_Z) // updated text
                                .add(
                                    "attribution",
                                    Json.createObjectBuilder()
                                        .add("source", SRC_A)
                                        .add("actor", "urn:li:corpuser:datahub")
                                        .add("time", 0))))
                .build());

    Documentation result = template.applyPatch(initial, upsertA);

    Assert.assertEquals(result.getDocumentations().size(), 2, "upsert must not add a third entry");
    Map<String, String> sourceToDoc =
        result.getDocumentations().stream()
            .collect(
                Collectors.toMap(
                    d -> d.getAttribution().getSource().toString(),
                    DocumentationAssociation::getDocumentation));
    Assert.assertEquals(sourceToDoc.get(SRC_A), DOC_Z, "srcA must show updated text");
    Assert.assertEquals(sourceToDoc.get(SRC_B), DOC_Y, "srcB must be unchanged");
  }
}

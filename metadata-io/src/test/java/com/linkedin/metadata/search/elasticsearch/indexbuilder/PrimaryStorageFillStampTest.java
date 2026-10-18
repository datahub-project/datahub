package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Map;
import java.util.Optional;
import org.testng.annotations.Test;

public class PrimaryStorageFillStampTest {

  @Test
  public void testStampKeepsTheDocumentIdSchemeAndReplacesOnlyTheFillState() {
    Map<String, Object> created =
        PrimaryStorageFillStamp.stamped(
            Map.of(
                V3DocumentIdScheme.META,
                Map.of(V3DocumentIdScheme.DOC_ID_SCHEME, "sha256-urn"),
                "properties",
                Map.of()),
            PrimaryStorageFillStamp.INCOMPLETE);

    assertTrue(PrimaryStorageFillStamp.incomplete(created));
    assertEquals(
        V3DocumentIdScheme.stampedOn(created), Optional.of("sha256-urn"), "the id scheme stays");

    Map<String, Object> finished =
        PrimaryStorageFillStamp.stamped(created, PrimaryStorageFillStamp.COMPLETE);
    assertFalse(PrimaryStorageFillStamp.incomplete(finished));
    assertTrue(PrimaryStorageFillStamp.complete(finished));
    assertEquals(
        PrimaryStorageFillStamp.read(finished), Optional.of(PrimaryStorageFillStamp.COMPLETE));
    assertEquals(V3DocumentIdScheme.stampedOn(finished), Optional.of("sha256-urn"));
  }

  @Test
  public void testAnIndexThatNeverRecordedAFillIsNotIncomplete() {
    assertFalse(PrimaryStorageFillStamp.incomplete(Map.of("properties", Map.of())));
    assertFalse(PrimaryStorageFillStamp.complete(Map.of("properties", Map.of())));
    assertFalse(PrimaryStorageFillStamp.incomplete(null));
    assertFalse(PrimaryStorageFillStamp.complete(null));
  }
}

package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Map;
import java.util.Optional;
import org.testng.annotations.Test;

public class IndexMigrationStampTest {

  @Test
  public void testMarkKeepsTheDocumentIdSchemeAndReplacesOnlyTheMigrationState() {
    Map<String, Object> created =
        IndexMigrationStamp.stamped(
            Map.of(
                V3DocumentIdScheme.META,
                Map.of(V3DocumentIdScheme.DOC_ID_SCHEME, "sha256-urn"),
                "properties",
                Map.of()),
            IndexMigrationStamp.INCOMPLETE);

    assertTrue(IndexMigrationStamp.incomplete(created));
    assertEquals(
        V3DocumentIdScheme.stampedOn(created), Optional.of("sha256-urn"), "the id scheme stays");

    Map<String, Object> finished =
        IndexMigrationStamp.stamped(created, IndexMigrationStamp.COMPLETE);
    assertFalse(IndexMigrationStamp.incomplete(finished));
    assertTrue(IndexMigrationStamp.complete(finished));
    assertEquals(IndexMigrationStamp.read(finished), Optional.of(IndexMigrationStamp.COMPLETE));
    assertEquals(V3DocumentIdScheme.stampedOn(finished), Optional.of("sha256-urn"));
  }

  @Test
  public void testCopyOntoKeepsTheLiveMigrationMarkOnADestination() {
    Map<String, Object> live =
        IndexMigrationStamp.stamped(
            Map.of(
                V3DocumentIdScheme.META,
                Map.of(V3DocumentIdScheme.DOC_ID_SCHEME, "sha256-urn"),
                "properties",
                Map.of("name", Map.of("type", "keyword"))),
            IndexMigrationStamp.COMPLETE);
    Map<String, Object> target =
        Map.of(
            V3DocumentIdScheme.META,
            Map.of(V3DocumentIdScheme.DOC_ID_SCHEME, "sha256-urn"),
            "properties",
            Map.of("name", Map.of("type", "text")));

    Map<String, Object> destination = IndexMigrationStamp.copyOnto(target, live);

    assertTrue(IndexMigrationStamp.complete(destination));
    assertEquals(V3DocumentIdScheme.stampedOn(destination), Optional.of("sha256-urn"));
    assertEquals(
        IndexMigrationStamp.copyOnto(target, Map.of("properties", Map.of())),
        target,
        "a live index with no mark leaves the target mappings unchanged");
    assertEquals(IndexMigrationStamp.copyOnto(target, null), target);
  }

  @Test
  public void testAnIndexThatNeverRecordedAMigrationIsNotIncomplete() {
    assertFalse(IndexMigrationStamp.incomplete(Map.of("properties", Map.of())));
    assertFalse(IndexMigrationStamp.complete(Map.of("properties", Map.of())));
    assertFalse(IndexMigrationStamp.incomplete(null));
    assertFalse(IndexMigrationStamp.complete(null));
  }
}

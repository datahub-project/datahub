package com.linkedin.metadata.search.elasticsearch.query.request;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.search.MatchedField;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class V3MatchedFieldsTest {

  @Test
  public void testReportsTheFirstMatchingValuePerFieldInFieldOrder() {
    Map<String, Object> source =
        Map.of(
            "description", "Daily orders by region",
            "name", "orders",
            "fieldPaths", List.of("region", "order_id", "order_total"),
            "platform", "urn:li:dataPlatform:hive");

    List<MatchedField> matched =
        new V3MatchedFields("order", 0)
            .find(source, List.of("name", "platform", "fieldPaths", "description"));

    assertEquals(names(matched), List.of("name", "fieldPaths", "description"));
    assertEquals(matched.get(1).getValue(), "order_id");
  }

  @Test
  public void testMatchesWordPrefixesIgnoringCaseAndAccents() {
    Map<String, Object> source = Map.of("name", "Café Revenue");

    assertEquals(
        names(new V3MatchedFields("CAFE rev", 0).find(source, List.of("name"))), List.of("name"));
    // Only the start of a word matches
    assertTrue(new V3MatchedFields("venue", 0).find(source, List.of("name")).isEmpty());
  }

  @Test
  public void testStopWordsAndUrnPartsNeverMatch() {
    Map<String, Object> source =
        Map.of("urn", "urn:li:corpuser:jdoe", "description", "the table of contents");

    assertTrue(new V3MatchedFields("li", 0).find(source, List.of("urn")).isEmpty());
    assertTrue(new V3MatchedFields("ur", 0).find(source, List.of("urn")).isEmpty());
    assertTrue(new V3MatchedFields("th", 0).find(source, List.of("description")).isEmpty());
    assertEquals(names(new V3MatchedFields("jdo", 0).find(source, List.of("urn"))), List.of("urn"));
  }

  @Test
  public void testQueryWordFindsWordsWithTheSameStem() {
    assertEquals(
        names(
            new V3MatchedFields("tables", 0).find(Map.of("name", "sales table"), List.of("name"))),
        List.of("name"));
    // Words that only share a prefix with the query word do not stem alike
    assertTrue(new V3MatchedFields("dbs", 0).find(Map.of("name", "db"), List.of("name")).isEmpty());
    assertTrue(
        new V3MatchedFields("card", 0)
            .find(Map.of("name", "car rental"), List.of("name"))
            .isEmpty());
  }

  /**
   * Only the last query word matches as a prefix, as the phrase prefix does; the others match whole
   * words or their stems. Words shorter than the minimum length are dropped, as the analyzers drop
   * them.
   */
  @Test
  public void testOnlyTheLastQueryWordMatchesAsAPrefix() {
    Map<String, Object> source = Map.of("name", "datasets");

    assertTrue(new V3MatchedFields("data platform", 0).find(source, List.of("name")).isEmpty());
    assertEquals(
        names(new V3MatchedFields("platform data", 0).find(source, List.of("name"))),
        List.of("name"));
    assertTrue(new V3MatchedFields("da", 3).find(source, List.of("name")).isEmpty());
    // A word the query excludes does not match
    assertTrue(new V3MatchedFields("orders -datasets", 0).find(source, List.of("name")).isEmpty());
  }

  /**
   * A snake_case identifier matches whole and by its parts, as the analyzers index it, even when
   * every part is too short to match on its own.
   */
  @Test
  public void testIdentifiersMatchWholeAndByTheirParts() {
    Map<String, Object> source = Map.of("fieldPaths", List.of("db_name", "db_id", "customer_id"));

    List<MatchedField> whole = new V3MatchedFields("db_id", 3).find(source, List.of("fieldPaths"));
    assertEquals(whole.get(0).getValue(), "db_id");
    List<MatchedField> part =
        new V3MatchedFields("customers report", 3).find(source, List.of("fieldPaths"));
    assertEquals(part.get(0).getValue(), "customer_id");
  }

  /** A query of only stop words or short words has no word a field could match. */
  @Test
  public void testQueryWithoutWordsHasNothingToMatch() {
    assertFalse(new V3MatchedFields("the of", 0).hasQueryWords());
    assertFalse(new V3MatchedFields("id", 3).hasQueryWords());
    assertTrue(new V3MatchedFields("orders", 3).hasQueryWords());
  }

  @Test
  public void testLongValuesAreCutAroundTheMatch() {
    String value = "x".repeat(300) + " revenue " + "y".repeat(300);

    String matched =
        new V3MatchedFields("revenue", 0)
            .find(Map.of("description", value), List.of("description"))
            .get(0)
            .getValue();

    assertEquals(matched.length(), 200);
    assertTrue(matched.contains("revenue"), matched);
  }

  @Test
  public void testLongValuesAreCutAroundTheMatchingWord() {
    // An accented word matches its folded query word
    String accented = "x".repeat(300) + " café " + "y".repeat(300);
    assertTrue(
        new V3MatchedFields("cafe", 0)
            .find(Map.of("description", accented), List.of("description"))
            .get(0)
            .getValue()
            .contains("café"));
    // The query word inside a longer word that does not match is not the anchor
    String embedded = "metadata " + "x".repeat(300) + " data " + "y".repeat(300);
    assertTrue(
        new V3MatchedFields("data", 0)
            .find(Map.of("description", embedded), List.of("description"))
            .get(0)
            .getValue()
            .contains(" data "));
  }

  private static List<String> names(List<MatchedField> matchedFields) {
    return matchedFields.stream().map(MatchedField::getName).toList();
  }
}

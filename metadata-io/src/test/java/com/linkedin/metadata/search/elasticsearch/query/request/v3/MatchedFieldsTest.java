package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.search.MatchedField;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class MatchedFieldsTest {

  @Test
  public void testCopiedPropertyReportsTheStrongestArrayValue() {
    Map<String, Object> nested =
        Map.of(
            "structuredProperties", Map.of("businessName", List.of("Orders", "Customer Orders")));
    List<MatchedField> matched =
        new MatchedFields("customer orders", 0)
            .find(nested, List.of("structuredProperties.businessName", "name"));

    assertEquals(names(matched), List.of("structuredProperties.businessName"));
    assertEquals(matched.get(0).getValue(), "Customer Orders");

    Map<String, Object> flat =
        Map.of("structuredProperties.businessName", List.of("Customer Orders Daily", "Orders"));
    assertEquals(
        new MatchedFields("orders", 0)
            .find(flat, List.of("structuredProperties.businessName"))
            .get(0)
            .getValue(),
        "Orders");
  }

  @Test
  public void testReportsTheFirstMatchingValuePerFieldInFieldOrder() {
    Map<String, Object> source =
        Map.of(
            "description", "Daily orders by region",
            "name", "orders",
            "fieldPaths", List.of("region", "order_id", "order_total"),
            "platform", "urn:li:dataPlatform:hive");

    List<MatchedField> matched =
        new MatchedFields("order", 0)
            .find(source, List.of("name", "platform", "fieldPaths", "description"));

    assertEquals(names(matched), List.of("name", "fieldPaths", "description"));
    assertEquals(matched.get(1).getValue(), "order_id");
  }

  @Test
  public void testMatchesWordPrefixesIgnoringCaseAndAccents() {
    Map<String, Object> source = Map.of("name", "Café Revenue");

    assertEquals(
        names(new MatchedFields("CAFE rev", 0).find(source, List.of("name"))), List.of("name"));
    // Only the start of a word matches
    assertTrue(new MatchedFields("venue", 0).find(source, List.of("name")).isEmpty());
  }

  @Test
  public void testStopWordsAndUrnPartsNeverMatch() {
    Map<String, Object> source =
        Map.of("urn", "urn:li:corpuser:jdoe", "description", "the table of contents");

    assertTrue(new MatchedFields("li", 0).find(source, List.of("urn")).isEmpty());
    assertTrue(new MatchedFields("ur", 0).find(source, List.of("urn")).isEmpty());
    assertTrue(new MatchedFields("th", 0).find(source, List.of("description")).isEmpty());
    assertEquals(names(new MatchedFields("jdo", 0).find(source, List.of("urn"))), List.of("urn"));
  }

  @Test
  public void testQueryWordFindsWordsWithTheSameStem() {
    assertEquals(
        names(new MatchedFields("tables", 0).find(Map.of("name", "sales table"), List.of("name"))),
        List.of("name"));
    // Words that only share a prefix with the query word do not stem alike
    assertTrue(new MatchedFields("dbs", 0).find(Map.of("name", "db"), List.of("name")).isEmpty());
    assertTrue(
        new MatchedFields("card", 0).find(Map.of("name", "car rental"), List.of("name")).isEmpty());
  }

  /**
   * Only the last query word matches as a prefix, as the phrase prefix does; the others match whole
   * words or their stems. Words shorter than the minimum length are dropped, as the analyzers drop
   * them.
   */
  @Test
  public void testOnlyTheLastQueryWordMatchesAsAPrefix() {
    Map<String, Object> source = Map.of("name", "datasets");

    assertTrue(new MatchedFields("data platform", 0).find(source, List.of("name")).isEmpty());
    assertEquals(
        names(new MatchedFields("platform data", 0).find(source, List.of("name"))),
        List.of("name"));
    assertTrue(new MatchedFields("da", 3).find(source, List.of("name")).isEmpty());
    // A minus is plain text, as in the query, so the word after it matches too
    assertEquals(
        names(new MatchedFields("orders -datasets", 0).find(source, List.of("name"))),
        List.of("name"));
  }

  /**
   * A snake_case identifier matches whole and by its parts, as the analyzers index it, even when
   * every part is too short to match on its own.
   */
  @Test
  public void testIdentifiersMatchWholeAndByTheirParts() {
    Map<String, Object> source = Map.of("fieldPaths", List.of("db_name", "db_id", "customer_id"));

    List<MatchedField> whole = new MatchedFields("db_id", 3).find(source, List.of("fieldPaths"));
    assertEquals(whole.get(0).getValue(), "db_id");
    List<MatchedField> part =
        new MatchedFields("customers report", 3).find(source, List.of("fieldPaths"));
    assertEquals(part.get(0).getValue(), "customer_id");
  }

  /**
   * Words fold as the analyzers fold them: ASCII folding keeps the vowel signs of an Indic word and
   * turns ß into ss.
   */
  @Test
  public void testWordsFoldAsTheAnalyzersFoldThem() {
    MatchedFields indic = new MatchedFields("नाम", 3);
    assertTrue(indic.hasQueryWords());
    assertEquals(names(indic.find(Map.of("name", "ग्राहक नाम"), List.of("name"))), List.of("name"));
    assertEquals(
        names(new MatchedFields("strasse", 0).find(Map.of("name", "Straße"), List.of("name"))),
        List.of("name"));
    assertEquals(
        names(new MatchedFields("Straße", 0).find(Map.of("name", "strasse"), List.of("name"))),
        List.of("name"));
  }

  /**
   * The stem overrides apply before stemming, as in the analyzers: customer does not stem to
   * custom.
   */
  @Test
  public void testStemOverridesApplyBeforeStemming() {
    assertTrue(
        new MatchedFields("customer", 0)
            .find(Map.of("name", "Custom fields"), List.of("name"))
            .isEmpty());
    assertEquals(
        names(new MatchedFields("customers", 0).find(Map.of("name", "customer"), List.of("name"))),
        List.of("name"));
  }

  /** A query of only stop words or short words has no word a field could match. */
  @Test
  public void testQueryWithoutWordsHasNothingToMatch() {
    assertFalse(new MatchedFields("the of", 0).hasQueryWords());
    assertFalse(new MatchedFields("id", 3).hasQueryWords());
    assertTrue(new MatchedFields("orders", 3).hasQueryWords());
  }

  @Test
  public void testLongValuesAreCutAroundTheMatch() {
    String value = "x".repeat(300) + " revenue " + "y".repeat(300);

    String matched =
        new MatchedFields("revenue", 0)
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
        new MatchedFields("cafe", 0)
            .find(Map.of("description", accented), List.of("description"))
            .get(0)
            .getValue()
            .contains("café"));
    // The query word inside a longer word that does not match is not the anchor
    String embedded = "metadata " + "x".repeat(300) + " data " + "y".repeat(300);
    assertTrue(
        new MatchedFields("data", 0)
            .find(Map.of("description", embedded), List.of("description"))
            .get(0)
            .getValue()
            .contains(" data "));
  }

  private static List<String> names(List<MatchedField> matchedFields) {
    return matchedFields.stream().map(MatchedField::getName).toList();
  }
}

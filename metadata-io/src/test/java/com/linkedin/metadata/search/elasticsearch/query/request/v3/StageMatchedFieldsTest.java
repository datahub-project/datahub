package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.search.MatchedField;
import com.linkedin.metadata.search.MatchedFieldArray;
import com.linkedin.metadata.search.SearchEntity;
import java.util.List;
import java.util.Map;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** A stage scan reads the fields it was given from the stashed source. */
public class StageMatchedFieldsTest {
  private static final String URN = "urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)";

  @BeforeMethod
  public void deferMatching() {
    HighlightCarry.begin();
  }

  @AfterMethod
  public void clearMatching() {
    HighlightCarry.clear();
  }

  @Test
  public void fillReportsOnlyTheRequestedFields() {
    StageMatchedFields.stash(
        URN,
        Map.of("name", "orders", "description", "orders catalog", "notes", "orders"),
        new MatchedFields("orders", 3),
        Map.of());
    SearchEntity hit = entity(URN);

    StageMatchedFields.fill(List.of(hit), List.of("name", "description"));

    assertEquals(names(hit), List.of("name", "description"));
  }

  @Test
  public void aCopiedPathIsReportedUnderItsLabel() {
    StageMatchedFields.stash(
        URN,
        Map.of("_aspects.doc.name", "orders"),
        new MatchedFields("orders", 3),
        Map.of("_aspects.doc.name", "name"));
    SearchEntity hit = entity(URN);

    StageMatchedFields.fill(List.of(hit), List.of("name"));

    assertEquals(hit.getMatchedFields().get(0).getName(), "name");
    assertEquals(hit.getMatchedFields().get(0).getValue(), "orders");
  }

  @Test
  public void aLabelStoredOnItsKeyAndACopiedPathReportsOneName() {
    StageMatchedFields.stash(
        URN,
        Map.of("name", "orders", "_aspects.doc.name", "orders catalog"),
        new MatchedFields("orders", 3),
        Map.of("_aspects.doc.name", "name"));
    SearchEntity hit = entity(URN);

    StageMatchedFields.fill(List.of(hit), List.of("name"));

    assertEquals(names(hit), List.of("name"));
    assertEquals(hit.getMatchedFields().get(0).getValue(), "orders");
  }

  @Test
  public void aRequestedPathKeepsItsOwnNameWhenItsLabelIsAlsoRequested() {
    StageMatchedFields.stash(
        URN,
        Map.of("_aspects.doc.name", "orders"),
        new MatchedFields("orders", 3),
        Map.of("_aspects.doc.name", "name"));
    SearchEntity hit = entity(URN);

    StageMatchedFields.fill(List.of(hit), List.of("_aspects.doc.name", "name"));

    assertEquals(names(hit), List.of("_aspects.doc.name"));
  }

  @Test
  public void aHitWithoutStashedSourceKeepsItsMatches() {
    SearchEntity injected =
        entity("urn:li:corpuser:orders").setMatchedFields(new MatchedFieldArray(name("orders")));

    StageMatchedFields.fill(List.of(injected), List.of("description"));

    assertEquals(injected.getMatchedFields().get(0).getName(), "name");
  }

  @Test
  public void anEmptyFieldListDoesNotScan() {
    StageMatchedFields.stash(
        URN, Map.of("name", "orders"), new MatchedFields("orders", 3), Map.of());
    SearchEntity hit = entity(URN);

    StageMatchedFields.fill(List.of(hit), List.of());

    assertFalse(hit.hasMatchedFields());
  }

  private static SearchEntity entity(String urn) {
    return new SearchEntity().setEntity(UrnUtils.getUrn(urn));
  }

  private static MatchedField name(String value) {
    return new MatchedField().setName("name").setValue(value);
  }

  private static List<String> names(SearchEntity hit) {
    assertTrue(hit.hasMatchedFields());
    return hit.getMatchedFields().stream().map(MatchedField::getName).toList();
  }
}

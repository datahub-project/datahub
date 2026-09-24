package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.Assert.*;

import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import java.sql.Timestamp;
import org.testng.annotations.Test;

public class DataProductPropertiesChangeEventGeneratorTest {

  private static final String ENTITY_URN = "urn:li:dataProduct:my-data-product";

  private static final String DATASET_A =
      "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.tableA,PROD)";
  private static final String DATASET_B =
      "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.tableB,PROD)";
  private static final String DATASET_C =
      "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.tableC,PROD)";

  private static EntityAspect makePropertiesAspect(String name, String description, long version) {
    return makePropertiesAspect(name, description, null, version);
  }

  private static EntityAspect makePropertiesAspect(
      String name, String description, String[] assetUrns, long version) {
    StringBuilder json = new StringBuilder("{");
    boolean hasField = false;
    if (name != null) {
      json.append(String.format("\"name\":\"%s\"", name));
      hasField = true;
    }
    if (description != null) {
      if (hasField) json.append(",");
      json.append(String.format("\"description\":\"%s\"", description));
      hasField = true;
    }
    if (assetUrns != null) {
      if (hasField) json.append(",");
      json.append("\"assets\":[");
      for (int i = 0; i < assetUrns.length; i++) {
        if (i > 0) json.append(",");
        json.append(String.format("{\"destinationUrn\":\"%s\"}", assetUrns[i]));
      }
      json.append("]");
    }
    json.append("}");

    EntityAspect aspect = new EntityAspect();
    aspect.setUrn(ENTITY_URN);
    aspect.setAspect("dataProductProperties");
    aspect.setVersion(version);
    aspect.setMetadata(json.toString());
    aspect.setCreatedOn(new Timestamp(1000L * (version + 1)));
    aspect.setCreatedBy("urn:li:corpuser:tester");
    return aspect;
  }

  @Test
  public void testNameAdded() {
    DataProductPropertiesChangeEventGenerator generator =
        new DataProductPropertiesChangeEventGenerator();

    EntityAspect previous = makePropertiesAspect(null, null, 0);
    EntityAspect current = makePropertiesAspect("My Product", null, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.DOCUMENTATION, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);

    ChangeEvent event = tx.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.DOCUMENTATION);
    assertEquals(event.getOperation(), ChangeOperation.ADD);
    assertEquals(event.getSemVerChange(), SemanticChangeType.MINOR);
    assertTrue(event.getDescription().contains("My Product"));
  }

  @Test
  public void testNameChanged() {
    DataProductPropertiesChangeEventGenerator generator =
        new DataProductPropertiesChangeEventGenerator();

    EntityAspect previous = makePropertiesAspect("Old Name", null, 0);
    EntityAspect current = makePropertiesAspect("New Name", null, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.DOCUMENTATION, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);

    ChangeEvent event = tx.getChangeEvents().get(0);
    assertEquals(event.getOperation(), ChangeOperation.MODIFY);
    assertTrue(event.getDescription().contains("Old Name"));
    assertTrue(event.getDescription().contains("New Name"));
  }

  @Test
  public void testDescriptionAdded() {
    DataProductPropertiesChangeEventGenerator generator =
        new DataProductPropertiesChangeEventGenerator();

    EntityAspect previous = makePropertiesAspect("Name", null, 0);
    EntityAspect current = makePropertiesAspect("Name", "A description", 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.DOCUMENTATION, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);

    ChangeEvent event = tx.getChangeEvents().get(0);
    assertEquals(event.getOperation(), ChangeOperation.ADD);
    assertTrue(event.getDescription().contains("A description"));
  }

  @Test
  public void testDescriptionRemoved() {
    DataProductPropertiesChangeEventGenerator generator =
        new DataProductPropertiesChangeEventGenerator();

    EntityAspect previous = makePropertiesAspect("Name", "Old desc", 0);
    EntityAspect current = makePropertiesAspect("Name", null, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.DOCUMENTATION, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);
    assertEquals(tx.getChangeEvents().get(0).getOperation(), ChangeOperation.REMOVE);
  }

  @Test
  public void testDescriptionChanged() {
    DataProductPropertiesChangeEventGenerator generator =
        new DataProductPropertiesChangeEventGenerator();

    EntityAspect previous = makePropertiesAspect("Name", "Old desc", 0);
    EntityAspect current = makePropertiesAspect("Name", "New desc", 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.DOCUMENTATION, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);
    assertEquals(tx.getChangeEvents().get(0).getOperation(), ChangeOperation.MODIFY);
  }

  @Test
  public void testNoChange() {
    DataProductPropertiesChangeEventGenerator generator =
        new DataProductPropertiesChangeEventGenerator();

    EntityAspect previous = makePropertiesAspect("Name", "Desc", 0);
    EntityAspect current = makePropertiesAspect("Name", "Desc", 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.DOCUMENTATION, null, false);

    assertNotNull(tx);
    assertTrue(tx.getChangeEvents().isEmpty());
    assertEquals(tx.getSemVerChange(), SemanticChangeType.NONE);
  }

  @Test
  public void testBothNameAndDescriptionChanged() {
    DataProductPropertiesChangeEventGenerator generator =
        new DataProductPropertiesChangeEventGenerator();

    EntityAspect previous = makePropertiesAspect("Old Name", "Old Desc", 0);
    EntityAspect current = makePropertiesAspect("New Name", "New Desc", 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.DOCUMENTATION, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 2);
    assertEquals(tx.getSemVerChange(), SemanticChangeType.MINOR);
  }

  @Test
  public void testNonDocumentationCategoryIgnored() {
    DataProductPropertiesChangeEventGenerator generator =
        new DataProductPropertiesChangeEventGenerator();

    EntityAspect previous = makePropertiesAspect("Old Name", null, 0);
    EntityAspect current = makePropertiesAspect("New Name", null, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.TAG, null, false);

    assertNotNull(tx);
    assertTrue(tx.getChangeEvents().isEmpty());
  }

  // ---- Asset membership diff tests ----

  @Test
  public void testAssetAdded() {
    DataProductPropertiesChangeEventGenerator generator =
        new DataProductPropertiesChangeEventGenerator();

    EntityAspect previous = makePropertiesAspect("Name", null, new String[] {}, 0);
    EntityAspect current = makePropertiesAspect("Name", null, new String[] {DATASET_A}, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.ASSET_MEMBERSHIP, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);
    assertEquals(tx.getChangeEvents().get(0).getOperation(), ChangeOperation.ADD);
    assertEquals(tx.getChangeEvents().get(0).getCategory(), ChangeCategory.ASSET_MEMBERSHIP);
    assertTrue(tx.getChangeEvents().get(0).getDescription().contains("tableA"));
  }

  @Test
  public void testAssetRemoved() {
    DataProductPropertiesChangeEventGenerator generator =
        new DataProductPropertiesChangeEventGenerator();

    EntityAspect previous = makePropertiesAspect("Name", null, new String[] {DATASET_A}, 0);
    EntityAspect current = makePropertiesAspect("Name", null, new String[] {}, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.ASSET_MEMBERSHIP, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);
    assertEquals(tx.getChangeEvents().get(0).getOperation(), ChangeOperation.REMOVE);
  }

  @Test
  public void testAssetSwapped() {
    DataProductPropertiesChangeEventGenerator generator =
        new DataProductPropertiesChangeEventGenerator();

    EntityAspect previous = makePropertiesAspect("Name", null, new String[] {DATASET_A}, 0);
    EntityAspect current = makePropertiesAspect("Name", null, new String[] {DATASET_B}, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.ASSET_MEMBERSHIP, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 2);

    long addCount =
        tx.getChangeEvents().stream().filter(e -> e.getOperation() == ChangeOperation.ADD).count();
    long removeCount =
        tx.getChangeEvents().stream()
            .filter(e -> e.getOperation() == ChangeOperation.REMOVE)
            .count();
    assertEquals(addCount, 1);
    assertEquals(removeCount, 1);
  }

  @Test
  public void testAssetNoChange() {
    DataProductPropertiesChangeEventGenerator generator =
        new DataProductPropertiesChangeEventGenerator();

    // Same assets in different order should produce no events after sorting
    EntityAspect previous =
        makePropertiesAspect("Name", null, new String[] {DATASET_B, DATASET_A}, 0);
    EntityAspect current =
        makePropertiesAspect("Name", null, new String[] {DATASET_A, DATASET_B}, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.ASSET_MEMBERSHIP, null, false);

    assertNotNull(tx);
    assertTrue(tx.getChangeEvents().isEmpty());
    assertEquals(tx.getSemVerChange(), SemanticChangeType.NONE);
  }

  @Test
  public void testAssetLargeDiff() {
    DataProductPropertiesChangeEventGenerator generator =
        new DataProductPropertiesChangeEventGenerator();

    // 3 assets → 2 assets (remove A, keep B and C)
    EntityAspect previous =
        makePropertiesAspect("Name", null, new String[] {DATASET_A, DATASET_B, DATASET_C}, 0);
    EntityAspect current =
        makePropertiesAspect("Name", null, new String[] {DATASET_B, DATASET_C}, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.ASSET_MEMBERSHIP, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);
    assertEquals(tx.getChangeEvents().get(0).getOperation(), ChangeOperation.REMOVE);
    assertTrue(tx.getChangeEvents().get(0).getDescription().contains("tableA"));
  }

  @Test
  public void testAssetNullToAssets() {
    DataProductPropertiesChangeEventGenerator generator =
        new DataProductPropertiesChangeEventGenerator();

    // No assets field at all → 2 assets
    EntityAspect previous = makePropertiesAspect("Name", null, null, 0);
    EntityAspect current =
        makePropertiesAspect("Name", null, new String[] {DATASET_A, DATASET_B}, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.ASSET_MEMBERSHIP, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 2);
    assertTrue(
        tx.getChangeEvents().stream().allMatch(e -> e.getOperation() == ChangeOperation.ADD));
  }
}

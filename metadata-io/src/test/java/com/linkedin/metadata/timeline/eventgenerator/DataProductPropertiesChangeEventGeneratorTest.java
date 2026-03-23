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

  private static EntityAspect makePropertiesAspect(String name, String description, long version) {
    StringBuilder json = new StringBuilder("{");
    boolean hasField = false;
    if (name != null) {
      json.append(String.format("\"name\":\"%s\"", name));
      hasField = true;
    }
    if (description != null) {
      if (hasField) json.append(",");
      json.append(String.format("\"description\":\"%s\"", description));
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
}

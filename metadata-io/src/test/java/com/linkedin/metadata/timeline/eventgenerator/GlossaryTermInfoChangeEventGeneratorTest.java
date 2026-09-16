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

public class GlossaryTermInfoChangeEventGeneratorTest {

  private static final String ENTITY_URN = "urn:li:glossaryTerm:SavingsAccount.balance";

  private static EntityAspect makeTermInfoAspect(String name, String definition, long version) {
    StringBuilder json = new StringBuilder("{");
    json.append(String.format("\"definition\":\"%s\"", definition != null ? definition : ""));
    if (name != null) {
      json.append(String.format(",\"name\":\"%s\"", name));
    }
    json.append("}");

    EntityAspect aspect = new EntityAspect();
    aspect.setUrn(ENTITY_URN);
    aspect.setAspect("glossaryTermInfo");
    aspect.setVersion(version);
    aspect.setMetadata(json.toString());
    aspect.setCreatedOn(new Timestamp(1000L * (version + 1)));
    aspect.setCreatedBy("urn:li:corpuser:tester");
    return aspect;
  }

  private static EntityAspect makeEmptyAspect(long version) {
    EntityAspect aspect = new EntityAspect();
    aspect.setUrn(ENTITY_URN);
    aspect.setAspect("glossaryTermInfo");
    aspect.setVersion(version);
    aspect.setMetadata(null);
    aspect.setCreatedOn(new Timestamp(1000L * (version + 1)));
    aspect.setCreatedBy("urn:li:corpuser:tester");
    return aspect;
  }

  @Test
  public void testNameAdded() {
    GlossaryTermInfoChangeEventGenerator generator = new GlossaryTermInfoChangeEventGenerator();

    EntityAspect previous = makeTermInfoAspect(null, "A definition", 0);
    EntityAspect current = makeTermInfoAspect("Balance", "A definition", 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.DOCUMENTATION, null, false);

    assertEquals(tx.getChangeEvents().size(), 1);
    ChangeEvent event = tx.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.DOCUMENTATION);
    assertEquals(event.getOperation(), ChangeOperation.ADD);
    assertTrue(event.getDescription().contains("Balance"));
  }

  @Test
  public void testNameChanged() {
    GlossaryTermInfoChangeEventGenerator generator = new GlossaryTermInfoChangeEventGenerator();

    EntityAspect previous = makeTermInfoAspect("Old Name", "def", 0);
    EntityAspect current = makeTermInfoAspect("New Name", "def", 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.DOCUMENTATION, null, false);

    assertEquals(tx.getChangeEvents().size(), 1);
    ChangeEvent event = tx.getChangeEvents().get(0);
    assertEquals(event.getOperation(), ChangeOperation.MODIFY);
    assertTrue(event.getDescription().contains("Old Name"));
    assertTrue(event.getDescription().contains("New Name"));
  }

  @Test
  public void testDefinitionChanged() {
    GlossaryTermInfoChangeEventGenerator generator = new GlossaryTermInfoChangeEventGenerator();

    EntityAspect previous = makeTermInfoAspect("Name", "Old def", 0);
    EntityAspect current = makeTermInfoAspect("Name", "New def", 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.DOCUMENTATION, null, false);

    assertEquals(tx.getChangeEvents().size(), 1);
    assertEquals(tx.getChangeEvents().get(0).getOperation(), ChangeOperation.MODIFY);
  }

  @Test
  public void testBothNameAndDefinitionChanged() {
    GlossaryTermInfoChangeEventGenerator generator = new GlossaryTermInfoChangeEventGenerator();

    EntityAspect previous = makeTermInfoAspect("Old Name", "Old def", 0);
    EntityAspect current = makeTermInfoAspect("New Name", "New def", 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.DOCUMENTATION, null, false);

    assertEquals(tx.getChangeEvents().size(), 2);
    assertEquals(tx.getSemVerChange(), SemanticChangeType.MINOR);
  }

  @Test
  public void testNoChange() {
    GlossaryTermInfoChangeEventGenerator generator = new GlossaryTermInfoChangeEventGenerator();

    EntityAspect previous = makeTermInfoAspect("Name", "Def", 0);
    EntityAspect current = makeTermInfoAspect("Name", "Def", 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.DOCUMENTATION, null, false);

    assertTrue(tx.getChangeEvents().isEmpty());
    assertEquals(tx.getSemVerChange(), SemanticChangeType.NONE);
  }

  @Test
  public void testNonDocumentationCategoryIgnored() {
    GlossaryTermInfoChangeEventGenerator generator = new GlossaryTermInfoChangeEventGenerator();

    EntityAspect previous = makeTermInfoAspect("Old Name", "def", 0);
    EntityAspect current = makeTermInfoAspect("New Name", "def", 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.TAG, null, false);

    assertTrue(tx.getChangeEvents().isEmpty());
  }
}

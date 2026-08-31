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

public class ApplicationsChangeEventGeneratorTest {

  private static final String ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)";

  private static final String APP_URN_1 = "urn:li:application:myApp1";
  private static final String APP_URN_2 = "urn:li:application:myApp2";

  private static EntityAspect makeApplicationsAspect(String[] appUrns, long version) {
    StringBuilder json = new StringBuilder("{\"applications\":[");
    if (appUrns != null) {
      for (int i = 0; i < appUrns.length; i++) {
        if (i > 0) json.append(",");
        json.append("\"").append(appUrns[i]).append("\"");
      }
    }
    json.append("]}");

    EntityAspect aspect = new EntityAspect();
    aspect.setUrn(ENTITY_URN);
    aspect.setAspect("applications");
    aspect.setVersion(version);
    aspect.setMetadata(json.toString());
    aspect.setCreatedOn(new Timestamp(1000L * (version + 1)));
    aspect.setCreatedBy("urn:li:corpuser:tester");
    return aspect;
  }

  @Test
  public void testApplicationAdded() {
    ApplicationsChangeEventGenerator generator = new ApplicationsChangeEventGenerator();

    EntityAspect previous = makeApplicationsAspect(new String[] {}, 0);
    EntityAspect current = makeApplicationsAspect(new String[] {APP_URN_1}, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.APPLICATION, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);

    ChangeEvent event = tx.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.APPLICATION);
    assertEquals(event.getOperation(), ChangeOperation.ADD);
    assertEquals(event.getSemVerChange(), SemanticChangeType.MINOR);
    assertTrue(event.getDescription().contains("myApp1"));
    assertTrue(event.getDescription().contains(ENTITY_URN));
  }

  @Test
  public void testApplicationRemoved() {
    ApplicationsChangeEventGenerator generator = new ApplicationsChangeEventGenerator();

    EntityAspect previous = makeApplicationsAspect(new String[] {APP_URN_1}, 0);
    EntityAspect current = makeApplicationsAspect(new String[] {}, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.APPLICATION, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);
    assertEquals(tx.getChangeEvents().get(0).getOperation(), ChangeOperation.REMOVE);
    assertTrue(tx.getChangeEvents().get(0).getDescription().contains("myApp1"));
  }

  @Test
  public void testApplicationSwapped() {
    ApplicationsChangeEventGenerator generator = new ApplicationsChangeEventGenerator();

    EntityAspect previous = makeApplicationsAspect(new String[] {APP_URN_1}, 0);
    EntityAspect current = makeApplicationsAspect(new String[] {APP_URN_2}, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.APPLICATION, null, false);

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
  public void testNoChange() {
    ApplicationsChangeEventGenerator generator = new ApplicationsChangeEventGenerator();

    EntityAspect previous = makeApplicationsAspect(new String[] {APP_URN_1}, 0);
    EntityAspect current = makeApplicationsAspect(new String[] {APP_URN_1}, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.APPLICATION, null, false);

    assertNotNull(tx);
    assertTrue(tx.getChangeEvents().isEmpty());
    assertEquals(tx.getSemVerChange(), SemanticChangeType.NONE);
  }

  @Test
  public void testMultipleAppsAdded() {
    ApplicationsChangeEventGenerator generator = new ApplicationsChangeEventGenerator();

    EntityAspect previous = makeApplicationsAspect(new String[] {}, 0);
    EntityAspect current = makeApplicationsAspect(new String[] {APP_URN_1, APP_URN_2}, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.APPLICATION, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 2);
    assertTrue(
        tx.getChangeEvents().stream().allMatch(e -> e.getOperation() == ChangeOperation.ADD));
  }

  @Test
  public void testMultipleAppsToEmpty() {
    ApplicationsChangeEventGenerator generator = new ApplicationsChangeEventGenerator();

    EntityAspect previous = makeApplicationsAspect(new String[] {APP_URN_1, APP_URN_2}, 0);
    EntityAspect current = makeApplicationsAspect(new String[] {}, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.APPLICATION, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 2);
    assertTrue(
        tx.getChangeEvents().stream().allMatch(e -> e.getOperation() == ChangeOperation.REMOVE));
  }

  @Test
  public void testNonApplicationCategoryIgnored() {
    ApplicationsChangeEventGenerator generator = new ApplicationsChangeEventGenerator();

    EntityAspect previous = makeApplicationsAspect(new String[] {}, 0);
    EntityAspect current = makeApplicationsAspect(new String[] {APP_URN_1}, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.TAG, null, false);

    assertNotNull(tx);
    assertTrue(tx.getChangeEvents().isEmpty());
  }
}

package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.Assert.*;

import com.datahub.util.RecordUtils;
import com.linkedin.common.Status;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import java.sql.Timestamp;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LifecycleStageChangeEventGeneratorTest {

  private static final String TEST_URN = "urn:li:document:test-doc";
  private static final String TEST_USER = "urn:li:corpuser:testUser";
  private static final long TEST_TIME = 1234567890000L;

  private LifecycleStageChangeEventGenerator generator;

  @BeforeMethod
  public void setup() {
    generator = new LifecycleStageChangeEventGenerator();
  }

  @Test
  public void testLifecycleStageSet() {
    Status oldStatus = new Status();
    Status newStatus = new Status();
    newStatus.setLifecycleStage(UrnUtils.getUrn("urn:li:lifecycleStageType:PUBLISHED"));

    EntityAspect previousAspect = createEntityAspect(oldStatus, TEST_TIME - 1000);
    EntityAspect currentAspect = createEntityAspect(newStatus, TEST_TIME);

    ChangeTransaction transaction =
        generator.getSemanticDiff(
            previousAspect, currentAspect, ChangeCategory.LIFECYCLE, null, false);

    assertNotNull(transaction);
    assertEquals(transaction.getChangeEvents().size(), 1);

    ChangeEvent event = transaction.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.LIFECYCLE);
    assertEquals(event.getOperation(), ChangeOperation.MODIFY);
    assertTrue(event.getDescription().contains("state changed"));
    assertTrue(event.getDescription().contains("published"));
    assertEquals(event.getParameters().get("oldState"), "");
    assertEquals(event.getParameters().get("newState"), "published");
  }

  @Test
  public void testLifecycleStageTransition() {
    Status oldStatus = new Status();
    oldStatus.setLifecycleStage(UrnUtils.getUrn("urn:li:lifecycleStageType:DRAFT"));

    Status newStatus = new Status();
    newStatus.setLifecycleStage(UrnUtils.getUrn("urn:li:lifecycleStageType:PUBLISHED"));

    EntityAspect previousAspect = createEntityAspect(oldStatus, TEST_TIME - 1000);
    EntityAspect currentAspect = createEntityAspect(newStatus, TEST_TIME);

    ChangeTransaction transaction =
        generator.getSemanticDiff(
            previousAspect, currentAspect, ChangeCategory.LIFECYCLE, null, false);

    assertNotNull(transaction);
    assertEquals(transaction.getChangeEvents().size(), 1);

    ChangeEvent event = transaction.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.LIFECYCLE);
    assertEquals(event.getOperation(), ChangeOperation.MODIFY);
    assertTrue(event.getDescription().contains("draft"));
    assertTrue(event.getDescription().contains("published"));
    assertEquals(event.getParameters().get("oldState"), "draft");
    assertEquals(event.getParameters().get("newState"), "published");
  }

  @Test
  public void testLifecycleStageCleared() {
    Status oldStatus = new Status();
    oldStatus.setLifecycleStage(UrnUtils.getUrn("urn:li:lifecycleStageType:DRAFT"));

    Status newStatus = new Status();

    EntityAspect previousAspect = createEntityAspect(oldStatus, TEST_TIME - 1000);
    EntityAspect currentAspect = createEntityAspect(newStatus, TEST_TIME);

    ChangeTransaction transaction =
        generator.getSemanticDiff(
            previousAspect, currentAspect, ChangeCategory.LIFECYCLE, null, false);

    assertNotNull(transaction);
    assertEquals(transaction.getChangeEvents().size(), 1);

    ChangeEvent event = transaction.getChangeEvents().get(0);
    assertTrue(event.getDescription().contains("draft"));
    assertTrue(event.getDescription().contains("removed"));
    assertEquals(event.getParameters().get("oldState"), "draft");
    assertEquals(event.getParameters().get("newState"), "");
  }

  @Test
  public void testNoLifecycleStageChange() {
    Status oldStatus = new Status();
    oldStatus.setLifecycleStage(UrnUtils.getUrn("urn:li:lifecycleStageType:PUBLISHED"));

    Status newStatus = new Status();
    newStatus.setLifecycleStage(UrnUtils.getUrn("urn:li:lifecycleStageType:PUBLISHED"));

    EntityAspect previousAspect = createEntityAspect(oldStatus, TEST_TIME - 1000);
    EntityAspect currentAspect = createEntityAspect(newStatus, TEST_TIME);

    ChangeTransaction transaction =
        generator.getSemanticDiff(
            previousAspect, currentAspect, ChangeCategory.LIFECYCLE, null, false);

    assertNotNull(transaction);
    assertEquals(transaction.getChangeEvents().size(), 0);
  }

  @Test
  public void testNonLifecycleCategoryIgnored() {
    Status oldStatus = new Status();
    Status newStatus = new Status();
    newStatus.setLifecycleStage(UrnUtils.getUrn("urn:li:lifecycleStageType:PUBLISHED"));

    EntityAspect previousAspect = createEntityAspect(oldStatus, TEST_TIME - 1000);
    EntityAspect currentAspect = createEntityAspect(newStatus, TEST_TIME);

    ChangeTransaction transaction =
        generator.getSemanticDiff(
            previousAspect, currentAspect, ChangeCategory.DOCUMENTATION, null, false);

    assertNotNull(transaction);
    assertEquals(transaction.getChangeEvents().size(), 0);
  }

  @Test
  public void testNoPreviousValue() {
    Status newStatus = new Status();
    newStatus.setLifecycleStage(UrnUtils.getUrn("urn:li:lifecycleStageType:DRAFT"));

    EntityAspect currentAspect = createEntityAspect(newStatus, TEST_TIME);

    ChangeTransaction transaction =
        generator.getSemanticDiff(null, currentAspect, ChangeCategory.LIFECYCLE, null, false);

    assertNotNull(transaction);
    assertEquals(transaction.getChangeEvents().size(), 1);

    ChangeEvent event = transaction.getChangeEvents().get(0);
    assertTrue(event.getDescription().contains("draft"));
    assertEquals(event.getParameters().get("oldState"), "");
    assertEquals(event.getParameters().get("newState"), "draft");
  }

  private EntityAspect createEntityAspect(Status status, long timestamp) {
    EntityAspect aspect = new EntityAspect();
    aspect.setUrn(TEST_URN);
    aspect.setAspect("status");
    aspect.setVersion(1L);
    aspect.setMetadata(RecordUtils.toJsonString(status));
    aspect.setCreatedOn(new Timestamp(timestamp));
    aspect.setCreatedBy(TEST_USER);
    return aspect;
  }
}

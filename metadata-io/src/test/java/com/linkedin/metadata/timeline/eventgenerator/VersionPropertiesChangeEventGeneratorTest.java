package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.VERSION_PROPERTIES_ASPECT_NAME;
import static org.testng.Assert.*;

import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import java.sql.Timestamp;
import org.testng.annotations.Test;

public class VersionPropertiesChangeEventGeneratorTest {

  private static final String ENTITY_URN = "urn:li:glossaryTerm:example.term";
  private static final String VERSION_SET_URN = "urn:li:versionSet:(12345,glossaryTerm)";

  private static EntityAspect makeVersionAspect(
      String versionTag, String comment, boolean isLatest, long version) {
    StringBuilder json = new StringBuilder("{");
    json.append(String.format("\"versionSet\":\"%s\"", VERSION_SET_URN));
    json.append(String.format(",\"version\":{\"versionTag\":\"%s\"}", versionTag));
    json.append(",\"sortId\":\"AAAAAAAA\"");
    if (comment != null) {
      json.append(String.format(",\"comment\":\"%s\"", comment));
    }
    json.append(String.format(",\"isLatest\":%s", isLatest));
    json.append("}");

    EntityAspect aspect = new EntityAspect();
    aspect.setUrn(ENTITY_URN);
    aspect.setAspect(VERSION_PROPERTIES_ASPECT_NAME);
    aspect.setVersion(version);
    aspect.setMetadata(json.toString());
    aspect.setCreatedOn(new Timestamp(1000L * (version + 1)));
    aspect.setCreatedBy("urn:li:corpuser:tester");
    return aspect;
  }

  /** Null metadata simulates the absence of the aspect before the first version write. */
  private static EntityAspect makeEmptyAspect(long version) {
    EntityAspect aspect = new EntityAspect();
    aspect.setUrn(ENTITY_URN);
    aspect.setAspect(VERSION_PROPERTIES_ASPECT_NAME);
    aspect.setVersion(version);
    aspect.setMetadata(null);
    aspect.setCreatedOn(new Timestamp(1000L * (version + 1)));
    aspect.setCreatedBy("urn:li:corpuser:tester");
    return aspect;
  }

  @Test
  public void testVersionCreated() {
    VersionPropertiesChangeEventGenerator generator = new VersionPropertiesChangeEventGenerator();

    EntityAspect previous = makeEmptyAspect(0);
    EntityAspect current = makeVersionAspect("1.0.0", null, true, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.VERSIONING, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);

    ChangeEvent event = tx.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.VERSIONING);
    assertEquals(event.getOperation(), ChangeOperation.ADD);
    assertEquals(event.getSemVerChange(), SemanticChangeType.MINOR);
    assertTrue(event.getDescription().contains("1.0.0"));
    assertEquals(event.getParameters().get("versionTag"), "1.0.0");
    assertEquals(event.getParameters().get("versionSetUrn"), VERSION_SET_URN);
    assertEquals(event.getParameters().get("isLatest"), "true");
  }

  @Test
  public void testVersionCreatedWithComment() {
    VersionPropertiesChangeEventGenerator generator = new VersionPropertiesChangeEventGenerator();

    EntityAspect previous = makeEmptyAspect(0);
    EntityAspect current = makeVersionAspect("1.0.0", "initial release", true, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.VERSIONING, null, false);

    ChangeEvent event = tx.getChangeEvents().get(0);
    assertTrue(event.getDescription().contains("initial release"));
    assertEquals(event.getParameters().get("comment"), "initial release");
  }

  @Test
  public void testVersionTagChanged() {
    VersionPropertiesChangeEventGenerator generator = new VersionPropertiesChangeEventGenerator();

    EntityAspect previous = makeVersionAspect("1.0.0", null, true, 0);
    EntityAspect current = makeVersionAspect("2.0.0", null, true, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.VERSIONING, null, false);

    assertNotNull(tx);
    assertEquals(tx.getChangeEvents().size(), 1);

    ChangeEvent event = tx.getChangeEvents().get(0);
    assertEquals(event.getOperation(), ChangeOperation.MODIFY);
    assertEquals(event.getSemVerChange(), SemanticChangeType.PATCH);
    assertTrue(event.getDescription().contains("1.0.0"));
    assertTrue(event.getDescription().contains("2.0.0"));
    assertEquals(event.getParameters().get("versionTag"), "2.0.0");
    assertEquals(event.getParameters().get("previousVersionTag"), "1.0.0");
  }

  @Test
  public void testNoChangeWhenTagUnchanged() {
    VersionPropertiesChangeEventGenerator generator = new VersionPropertiesChangeEventGenerator();

    EntityAspect previous = makeVersionAspect("1.0.0", null, true, 0);
    EntityAspect current = makeVersionAspect("1.0.0", null, true, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.VERSIONING, null, false);

    assertNotNull(tx);
    assertTrue(tx.getChangeEvents().isEmpty());
    assertEquals(tx.getSemVerChange(), SemanticChangeType.NONE);
  }

  @Test
  public void testNonVersioningCategoryIgnored() {
    VersionPropertiesChangeEventGenerator generator = new VersionPropertiesChangeEventGenerator();

    EntityAspect previous = makeEmptyAspect(0);
    EntityAspect current = makeVersionAspect("1.0.0", null, true, 1);

    ChangeTransaction tx =
        generator.getSemanticDiff(previous, current, ChangeCategory.TAG, null, false);

    assertNotNull(tx);
    assertTrue(tx.getChangeEvents().isEmpty());
  }
}

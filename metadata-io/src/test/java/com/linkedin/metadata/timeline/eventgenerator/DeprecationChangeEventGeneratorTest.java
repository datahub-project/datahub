package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.DEPRECATION_ASPECT_NAME;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import java.sql.Timestamp;
import java.util.List;
import org.testng.annotations.Test;

public class DeprecationChangeEventGeneratorTest {

  private static final String ENTITY_URN = "urn:li:glossaryTerm:term";

  private static EntityAspect stored(Boolean deprecated, String note, long version) {
    EntityAspect aspect = new EntityAspect();
    aspect.setUrn(ENTITY_URN);
    aspect.setAspect(DEPRECATION_ASPECT_NAME);
    aspect.setVersion(version);
    aspect.setMetadata(
        deprecated == null
            ? null
            : String.format(
                "{\"deprecated\":%s,\"note\":\"%s\",\"decommissionTime\":42,"
                    + "\"actor\":\"urn:li:corpuser:admin\"}",
                deprecated, note));
    aspect.setCreatedOn(new Timestamp(1000L * (version + 1)));
    aspect.setCreatedBy("urn:li:corpuser:admin");
    return aspect;
  }

  private static List<ChangeEvent> diff(
      EntityAspect previous, EntityAspect current, ChangeCategory element) {
    return new DeprecationChangeEventGenerator()
        .getSemanticDiff(previous, current, element, null, false)
        .getChangeEvents();
  }

  @Test
  public void testDeprecatedEmitsEventWithNoteAndTimestamp() {
    List<ChangeEvent> events =
        diff(stored(false, "", 0), stored(true, "use x", 1), ChangeCategory.DEPRECATION);
    assertEquals(events.size(), 1);
    ChangeEvent event = events.get(0);
    assertEquals(event.getCategory(), ChangeCategory.DEPRECATION);
    assertEquals(event.getParameters().get("status"), "DEPRECATED");
    assertEquals(event.getParameters().get("note"), "use x");
    assertEquals(event.getParameters().get("timestamp"), "42");
  }

  @Test
  public void testUndeprecatedEmitsActive() {
    List<ChangeEvent> events =
        diff(stored(true, "use x", 0), stored(false, "", 1), ChangeCategory.DEPRECATION);
    assertEquals(events.size(), 1);
    assertEquals(events.get(0).getParameters().get("status"), "ACTIVE");
  }

  @Test
  public void testNoteOnlyEditEmitsNothing() {
    assertEquals(
        diff(stored(true, "a", 0), stored(true, "b", 1), ChangeCategory.DEPRECATION).size(), 0);
  }

  @Test
  public void testUndeprecatedWriteOnNeverDeprecatedEmitsNothing() {
    assertEquals(
        diff(stored(null, null, -1), stored(false, "", 0), ChangeCategory.DEPRECATION).size(), 0);
  }

  @Test
  public void testFirstDeprecationWithNoPriorAspect() {
    List<ChangeEvent> events =
        diff(stored(null, null, -1), stored(true, "use x", 0), ChangeCategory.DEPRECATION);
    assertEquals(events.size(), 1);
    assertEquals(events.get(0).getParameters().get("status"), "DEPRECATED");
  }

  @Test
  public void testOtherCategoryReturnsEmpty() {
    assertEquals(diff(stored(false, "", 0), stored(true, "x", 1), ChangeCategory.TAG).size(), 0);
  }
}

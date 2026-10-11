package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.AssertJUnit.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.mxe.SystemMetadata;
import java.util.List;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public class EditableDatasetPropertiesChangeEventGeneratorTest
    extends AbstractTestNGSpringContextTests {

  private static final Urn DATASET_URN;
  private static final AuditStamp AUDIT_STAMP;

  static {
    try {
      DATASET_URN =
          Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,testDataset,PROD)");
      AUDIT_STAMP =
          new AuditStamp()
              .setActor(Urn.createFromString("urn:li:corpuser:testUser"))
              .setTime(1683829509553L);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testNoChange() throws Exception {
    EditableDatasetPropertiesChangeEventGenerator gen =
        new EditableDatasetPropertiesChangeEventGenerator();

    EditableDatasetProperties props = new EditableDatasetProperties();
    props.setDescription("Same description");

    Aspect<EditableDatasetProperties> from = new Aspect<>(props, new SystemMetadata());
    Aspect<EditableDatasetProperties> to = new Aspect<>(props, new SystemMetadata());

    List<ChangeEvent> events =
        gen.getChangeEvents(
            DATASET_URN, "dataset", "editableDatasetProperties", from, to, AUDIT_STAMP);

    assertEquals(0, events.size());
  }

  @Test
  public void testDescriptionAdded() throws Exception {
    EditableDatasetPropertiesChangeEventGenerator gen =
        new EditableDatasetPropertiesChangeEventGenerator();

    EditableDatasetProperties toProps = new EditableDatasetProperties();
    toProps.setDescription("New description");

    Aspect<EditableDatasetProperties> from = new Aspect<>(null, new SystemMetadata());
    Aspect<EditableDatasetProperties> to = new Aspect<>(toProps, new SystemMetadata());

    List<ChangeEvent> events =
        gen.getChangeEvents(
            DATASET_URN, "dataset", "editableDatasetProperties", from, to, AUDIT_STAMP);

    assertEquals(1, events.size());
    assertEquals("ADD", events.get(0).getOperation().toString());
    assertEquals("New description", events.get(0).getParameters().get("description"));
    assertNull(events.get(0).getParameters().get("previousDescription"));
  }

  @Test
  public void testDescriptionRemoved() throws Exception {
    EditableDatasetPropertiesChangeEventGenerator gen =
        new EditableDatasetPropertiesChangeEventGenerator();

    EditableDatasetProperties fromProps = new EditableDatasetProperties();
    fromProps.setDescription("Old description");

    EditableDatasetProperties toProps = new EditableDatasetProperties();
    // no description set

    Aspect<EditableDatasetProperties> from = new Aspect<>(fromProps, new SystemMetadata());
    Aspect<EditableDatasetProperties> to = new Aspect<>(toProps, new SystemMetadata());

    List<ChangeEvent> events =
        gen.getChangeEvents(
            DATASET_URN, "dataset", "editableDatasetProperties", from, to, AUDIT_STAMP);

    assertEquals(1, events.size());
    assertEquals("REMOVE", events.get(0).getOperation().toString());
    assertEquals("Old description", events.get(0).getParameters().get("description"));
  }

  @Test
  public void testDescriptionModifiedIncludesPreviousDescription() throws Exception {
    EditableDatasetPropertiesChangeEventGenerator gen =
        new EditableDatasetPropertiesChangeEventGenerator();

    EditableDatasetProperties fromProps = new EditableDatasetProperties();
    fromProps.setDescription("Old description");

    EditableDatasetProperties toProps = new EditableDatasetProperties();
    toProps.setDescription("New description");

    Aspect<EditableDatasetProperties> from = new Aspect<>(fromProps, new SystemMetadata());
    Aspect<EditableDatasetProperties> to = new Aspect<>(toProps, new SystemMetadata());

    List<ChangeEvent> events =
        gen.getChangeEvents(
            DATASET_URN, "dataset", "editableDatasetProperties", from, to, AUDIT_STAMP);

    assertEquals(1, events.size());
    assertEquals("MODIFY", events.get(0).getOperation().toString());
    assertEquals("New description", events.get(0).getParameters().get("description"));
    assertEquals("Old description", events.get(0).getParameters().get("previousDescription"));
  }

  @Test
  public void testNullFromAspectCountsAsAdd() throws Exception {
    EditableDatasetPropertiesChangeEventGenerator gen =
        new EditableDatasetPropertiesChangeEventGenerator();

    EditableDatasetProperties toProps = new EditableDatasetProperties();
    toProps.setDescription("Added description");

    Aspect<EditableDatasetProperties> from = new Aspect<>(null, new SystemMetadata());
    Aspect<EditableDatasetProperties> to = new Aspect<>(toProps, new SystemMetadata());

    List<ChangeEvent> events =
        gen.getChangeEvents(
            DATASET_URN, "dataset", "editableDatasetProperties", from, to, AUDIT_STAMP);

    assertEquals(1, events.size());
    assertEquals("ADD", events.get(0).getOperation().toString());
  }

  @Test
  public void testNullToAspectCountsAsRemove() throws Exception {
    EditableDatasetPropertiesChangeEventGenerator gen =
        new EditableDatasetPropertiesChangeEventGenerator();

    EditableDatasetProperties fromProps = new EditableDatasetProperties();
    fromProps.setDescription("Removed description");

    Aspect<EditableDatasetProperties> from = new Aspect<>(fromProps, new SystemMetadata());
    Aspect<EditableDatasetProperties> to = new Aspect<>(null, new SystemMetadata());

    List<ChangeEvent> events =
        gen.getChangeEvents(
            DATASET_URN, "dataset", "editableDatasetProperties", from, to, AUDIT_STAMP);

    assertEquals(1, events.size());
    assertEquals("REMOVE", events.get(0).getOperation().toString());
    assertEquals("Removed description", events.get(0).getParameters().get("description"));
  }
}

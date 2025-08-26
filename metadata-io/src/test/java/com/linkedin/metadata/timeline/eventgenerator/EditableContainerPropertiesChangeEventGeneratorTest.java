package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.AssertJUnit.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.EditableContainerProperties;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.mxe.SystemMetadata;
import java.util.List;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public class EditableContainerPropertiesChangeEventGeneratorTest
    extends AbstractTestNGSpringContextTests {

  @Test
  public void testNoChange() throws Exception {
    EditableContainerPropertiesChangeEventGenerator test =
        new EditableContainerPropertiesChangeEventGenerator();

    Urn urn = Urn.createFromString("urn:li:container:testContainer");
    String entity = "container";
    String aspect = "editableContainerProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    EditableContainerProperties editableContainerProperties = new EditableContainerProperties();
    editableContainerProperties.setDescription("Sample container description");

    Aspect<EditableContainerProperties> from =
        new Aspect<>(editableContainerProperties, new SystemMetadata());
    Aspect<EditableContainerProperties> to =
        new Aspect<>(editableContainerProperties, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(0, actual.size());
  }

  @Test
  public void testDescriptionAdded() throws Exception {
    EditableContainerPropertiesChangeEventGenerator test =
        new EditableContainerPropertiesChangeEventGenerator();

    Urn urn = Urn.createFromString("urn:li:container:testContainer");
    String entity = "container";
    String aspect = "editableContainerProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    EditableContainerProperties editableContainerPropertiesTo = new EditableContainerProperties();
    editableContainerPropertiesTo.setDescription("New container description");

    Aspect<EditableContainerProperties> from = new Aspect<>(null, new SystemMetadata());
    Aspect<EditableContainerProperties> to =
        new Aspect<>(editableContainerPropertiesTo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(1, actual.size());
    assertEquals("ADD", actual.get(0).getOperation().toString());
    assertEquals("New container description", actual.get(0).getParameters().get("description"));
    assertTrue(actual.get(0).getDescription().contains("New container description"));
  }

  @Test
  public void testDescriptionAddedFromEmpty() throws Exception {
    EditableContainerPropertiesChangeEventGenerator test =
        new EditableContainerPropertiesChangeEventGenerator();

    Urn urn = Urn.createFromString("urn:li:container:testContainer");
    String entity = "container";
    String aspect = "editableContainerProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    EditableContainerProperties editableContainerPropertiesFrom = new EditableContainerProperties();
    // No description set (null)

    EditableContainerProperties editableContainerPropertiesTo = new EditableContainerProperties();
    editableContainerPropertiesTo.setDescription("Added container description");

    Aspect<EditableContainerProperties> from =
        new Aspect<>(editableContainerPropertiesFrom, new SystemMetadata());
    Aspect<EditableContainerProperties> to =
        new Aspect<>(editableContainerPropertiesTo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(1, actual.size());
    assertEquals("ADD", actual.get(0).getOperation().toString());
    assertEquals("Added container description", actual.get(0).getParameters().get("description"));
    assertTrue(actual.get(0).getDescription().contains("Added container description"));
  }

  @Test
  public void testDescriptionModified() throws Exception {
    EditableContainerPropertiesChangeEventGenerator test =
        new EditableContainerPropertiesChangeEventGenerator();

    Urn urn = Urn.createFromString("urn:li:container:testContainer");
    String entity = "container";
    String aspect = "editableContainerProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    EditableContainerProperties editableContainerPropertiesFrom = new EditableContainerProperties();
    editableContainerPropertiesFrom.setDescription("Original container description");

    EditableContainerProperties editableContainerPropertiesTo = new EditableContainerProperties();
    editableContainerPropertiesTo.setDescription("Updated container description");

    Aspect<EditableContainerProperties> from =
        new Aspect<>(editableContainerPropertiesFrom, new SystemMetadata());
    Aspect<EditableContainerProperties> to =
        new Aspect<>(editableContainerPropertiesTo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(1, actual.size());
    assertEquals("MODIFY", actual.get(0).getOperation().toString());
    assertEquals("Updated container description", actual.get(0).getParameters().get("description"));
    assertTrue(actual.get(0).getDescription().contains("Original container description"));
    assertTrue(actual.get(0).getDescription().contains("Updated container description"));
  }

  @Test
  public void testDescriptionRemoved() throws Exception {
    EditableContainerPropertiesChangeEventGenerator test =
        new EditableContainerPropertiesChangeEventGenerator();

    Urn urn = Urn.createFromString("urn:li:container:testContainer");
    String entity = "container";
    String aspect = "editableContainerProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    EditableContainerProperties editableContainerPropertiesFrom = new EditableContainerProperties();
    editableContainerPropertiesFrom.setDescription("Description to be removed");

    EditableContainerProperties editableContainerPropertiesTo = new EditableContainerProperties();
    // No description set (null)

    Aspect<EditableContainerProperties> from =
        new Aspect<>(editableContainerPropertiesFrom, new SystemMetadata());
    Aspect<EditableContainerProperties> to =
        new Aspect<>(editableContainerPropertiesTo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(1, actual.size());
    assertEquals("REMOVE", actual.get(0).getOperation().toString());
    assertEquals("Description to be removed", actual.get(0).getParameters().get("description"));
    assertTrue(actual.get(0).getDescription().contains("Description to be removed"));
  }

  @Test
  public void testDescriptionRemovedToNull() throws Exception {
    EditableContainerPropertiesChangeEventGenerator test =
        new EditableContainerPropertiesChangeEventGenerator();

    Urn urn = Urn.createFromString("urn:li:container:testContainer");
    String entity = "container";
    String aspect = "editableContainerProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    EditableContainerProperties editableContainerPropertiesFrom = new EditableContainerProperties();
    editableContainerPropertiesFrom.setDescription("Description to be completely removed");

    Aspect<EditableContainerProperties> from =
        new Aspect<>(editableContainerPropertiesFrom, new SystemMetadata());
    Aspect<EditableContainerProperties> to = new Aspect<>(null, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(1, actual.size());
    assertEquals("REMOVE", actual.get(0).getOperation().toString());
    assertEquals(
        "Description to be completely removed", actual.get(0).getParameters().get("description"));
    assertTrue(actual.get(0).getDescription().contains("Description to be completely removed"));
  }

  @Test
  public void testDescriptionChangeEventFormatting() throws Exception {
    EditableContainerPropertiesChangeEventGenerator test =
        new EditableContainerPropertiesChangeEventGenerator();

    Urn urn = Urn.createFromString("urn:li:container:testContainer");
    String entity = "container";
    String aspect = "editableContainerProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    // Test ADD format
    EditableContainerProperties addedProperties = new EditableContainerProperties();
    addedProperties.setDescription("Test description");

    Aspect<EditableContainerProperties> fromNull = new Aspect<>(null, new SystemMetadata());
    Aspect<EditableContainerProperties> toAdded =
        new Aspect<>(addedProperties, new SystemMetadata());

    List<ChangeEvent> addEvents =
        test.getChangeEvents(urn, entity, aspect, fromNull, toAdded, auditStamp);
    assertEquals(1, addEvents.size());
    assertTrue(addEvents.get(0).getDescription().startsWith("Documentation for"));
    assertTrue(addEvents.get(0).getDescription().contains("has been added"));

    // Test MODIFY format
    EditableContainerProperties originalProperties = new EditableContainerProperties();
    originalProperties.setDescription("Original");

    EditableContainerProperties modifiedProperties = new EditableContainerProperties();
    modifiedProperties.setDescription("Modified");

    Aspect<EditableContainerProperties> fromOriginal =
        new Aspect<>(originalProperties, new SystemMetadata());
    Aspect<EditableContainerProperties> toModified =
        new Aspect<>(modifiedProperties, new SystemMetadata());

    List<ChangeEvent> modifyEvents =
        test.getChangeEvents(urn, entity, aspect, fromOriginal, toModified, auditStamp);
    assertEquals(1, modifyEvents.size());
    assertTrue(modifyEvents.get(0).getDescription().contains("has been changed from"));
    assertTrue(modifyEvents.get(0).getDescription().contains("Original"));
    assertTrue(modifyEvents.get(0).getDescription().contains("Modified"));

    // Test REMOVE format
    Aspect<EditableContainerProperties> toNull = new Aspect<>(null, new SystemMetadata());

    List<ChangeEvent> removeEvents =
        test.getChangeEvents(urn, entity, aspect, fromOriginal, toNull, auditStamp);
    assertEquals(1, removeEvents.size());
    assertTrue(removeEvents.get(0).getDescription().contains("has been removed"));
    assertTrue(removeEvents.get(0).getDescription().contains("Original"));
  }
}

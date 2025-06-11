package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.AssertJUnit.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.ContainerProperties;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.mxe.SystemMetadata;
import java.util.List;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public class ContainerPropertiesChangeEventGeneratorTest extends AbstractTestNGSpringContextTests {

  @Test
  public void testNoChange() throws Exception {
    ContainerPropertiesChangeEventGenerator test = new ContainerPropertiesChangeEventGenerator();

    Urn urn = Urn.createFromString("urn:li:container:testContainer");
    String entity = "container";
    String aspect = "containerProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    ContainerProperties containerProperties = new ContainerProperties();
    containerProperties.setDescription("Sample container description");

    Aspect<ContainerProperties> from = new Aspect<>(containerProperties, new SystemMetadata());
    Aspect<ContainerProperties> to = new Aspect<>(containerProperties, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(0, actual.size());
  }

  @Test
  public void testDescriptionAdded() throws Exception {
    ContainerPropertiesChangeEventGenerator test = new ContainerPropertiesChangeEventGenerator();

    Urn urn = Urn.createFromString("urn:li:container:testContainer");
    String entity = "container";
    String aspect = "containerProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    ContainerProperties containerPropertiesTo = new ContainerProperties();
    containerPropertiesTo.setDescription("New container description");

    Aspect<ContainerProperties> from = new Aspect<>(null, new SystemMetadata());
    Aspect<ContainerProperties> to = new Aspect<>(containerPropertiesTo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(1, actual.size());
    assertEquals("ADD", actual.get(0).getOperation().toString());
    assertEquals("New container description", actual.get(0).getParameters().get("description"));
    assertTrue(actual.get(0).getDescription().contains("New container description"));
  }

  @Test
  public void testDescriptionAddedFromEmpty() throws Exception {
    ContainerPropertiesChangeEventGenerator test = new ContainerPropertiesChangeEventGenerator();

    Urn urn = Urn.createFromString("urn:li:container:testContainer");
    String entity = "container";
    String aspect = "containerProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    ContainerProperties containerPropertiesFrom = new ContainerProperties();
    // No description set (null)

    ContainerProperties containerPropertiesTo = new ContainerProperties();
    containerPropertiesTo.setDescription("Added container description");

    Aspect<ContainerProperties> from = new Aspect<>(containerPropertiesFrom, new SystemMetadata());
    Aspect<ContainerProperties> to = new Aspect<>(containerPropertiesTo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(1, actual.size());
    assertEquals("ADD", actual.get(0).getOperation().toString());
    assertEquals("Added container description", actual.get(0).getParameters().get("description"));
    assertTrue(actual.get(0).getDescription().contains("Added container description"));
  }

  @Test
  public void testDescriptionModified() throws Exception {
    ContainerPropertiesChangeEventGenerator test = new ContainerPropertiesChangeEventGenerator();

    Urn urn = Urn.createFromString("urn:li:container:testContainer");
    String entity = "container";
    String aspect = "containerProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    ContainerProperties containerPropertiesFrom = new ContainerProperties();
    containerPropertiesFrom.setDescription("Original container description");

    ContainerProperties containerPropertiesTo = new ContainerProperties();
    containerPropertiesTo.setDescription("Updated container description");

    Aspect<ContainerProperties> from = new Aspect<>(containerPropertiesFrom, new SystemMetadata());
    Aspect<ContainerProperties> to = new Aspect<>(containerPropertiesTo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(1, actual.size());
    assertEquals("MODIFY", actual.get(0).getOperation().toString());
    assertEquals("Updated container description", actual.get(0).getParameters().get("description"));
    assertTrue(actual.get(0).getDescription().contains("Original container description"));
    assertTrue(actual.get(0).getDescription().contains("Updated container description"));
  }

  @Test
  public void testDescriptionRemoved() throws Exception {
    ContainerPropertiesChangeEventGenerator test = new ContainerPropertiesChangeEventGenerator();

    Urn urn =
        Urn.createFromString(
            "urn:li:container:(urn:li:dataPlatform:hdfs,SampleHdfsContainer,PROD)");
    String entity = "container";
    String aspect = "containerProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    ContainerProperties containerPropertiesFrom = new ContainerProperties();
    containerPropertiesFrom.setDescription("Description to be removed");

    ContainerProperties containerPropertiesTo = new ContainerProperties();
    // No description set (null)

    Aspect<ContainerProperties> from = new Aspect<>(containerPropertiesFrom, new SystemMetadata());
    Aspect<ContainerProperties> to = new Aspect<>(containerPropertiesTo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(1, actual.size());
    assertEquals("REMOVE", actual.get(0).getOperation().toString());
    assertEquals("Description to be removed", actual.get(0).getParameters().get("description"));
    assertTrue(actual.get(0).getDescription().contains("Description to be removed"));
  }

  @Test
  public void testDescriptionRemovedToNull() throws Exception {
    ContainerPropertiesChangeEventGenerator test = new ContainerPropertiesChangeEventGenerator();

    Urn urn =
        Urn.createFromString(
            "urn:li:container:(urn:li:dataPlatform:hdfs,SampleHdfsContainer,PROD)");
    String entity = "container";
    String aspect = "containerProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    ContainerProperties containerPropertiesFrom = new ContainerProperties();
    containerPropertiesFrom.setDescription("Description to be completely removed");

    Aspect<ContainerProperties> from = new Aspect<>(containerPropertiesFrom, new SystemMetadata());
    Aspect<ContainerProperties> to = new Aspect<>(null, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(1, actual.size());
    assertEquals("REMOVE", actual.get(0).getOperation().toString());
    assertEquals(
        "Description to be completely removed", actual.get(0).getParameters().get("description"));
    assertTrue(actual.get(0).getDescription().contains("Description to be completely removed"));
  }
}

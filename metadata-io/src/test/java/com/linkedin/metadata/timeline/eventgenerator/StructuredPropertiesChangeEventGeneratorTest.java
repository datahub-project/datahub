package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.AssertJUnit.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import java.util.List;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public class StructuredPropertiesChangeEventGeneratorTest extends AbstractTestNGSpringContextTests {

  @Test
  public void testNoChange() throws Exception {
    StructuredPropertyChangeEventGenerator test = new StructuredPropertyChangeEventGenerator();

    Urn urn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    String entity = "dataset";
    String aspect = "structedProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    StructuredProperties structuredPropertiesTo = new StructuredProperties();
    StructuredPropertyValueAssignmentArray a = new StructuredPropertyValueAssignmentArray();
    StructuredPropertyValueAssignment prop1 = new StructuredPropertyValueAssignment();
    prop1.setPropertyUrn(new Urn("urn:li:structuredProperty:io.acryl.privacy.retentionTime"));
    PrimitivePropertyValueArray value = new PrimitivePropertyValueArray();
    value.add(PrimitivePropertyValue.create("90"));
    prop1.setValues(value);
    a.add(prop1);
    structuredPropertiesTo.setProperties(a);

    Aspect<StructuredProperties> from =
        new Aspect<>(new StructuredProperties(), new SystemMetadata());

    Aspect<StructuredProperties> to = new Aspect<>(structuredPropertiesTo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, to, to, auditStamp);

    assertEquals(0, actual.size());
  }

  @Test
  public void testFirstItemAdded() throws Exception {
    StructuredPropertyChangeEventGenerator test = new StructuredPropertyChangeEventGenerator();

    Urn urn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    String entity = "dataset";
    String aspect = "structedProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    StructuredProperties structuredPropertiesTo = new StructuredProperties();
    StructuredPropertyValueAssignmentArray a = new StructuredPropertyValueAssignmentArray();
    StructuredPropertyValueAssignment prop1 = new StructuredPropertyValueAssignment();
    prop1.setPropertyUrn(new Urn("urn:li:structuredProperty:io.acryl.privacy.retentionTime"));
    PrimitivePropertyValueArray value = new PrimitivePropertyValueArray();
    value.add(PrimitivePropertyValue.create("90"));
    prop1.setValues(value);
    a.add(prop1);
    structuredPropertiesTo.setProperties(a);

    Aspect<StructuredProperties> from = new Aspect<>(null, new SystemMetadata());

    Aspect<StructuredProperties> to = new Aspect<>(structuredPropertiesTo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    String expectedValue = ("[\"90\"]");

    assertEquals(1, actual.size());
    assertEquals(
        "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
        actual.get(0).getParameters().get("propertyUrn").toString());

    String ea = (String) actual.get(0).getParameters().get("propertyValues");
    assertEquals("ADD", actual.get(0).getOperation().toString());
    assertEquals(expectedValue, ea);
  }

  @Test
  public void testMultipleItemAdded() throws Exception {
    StructuredPropertyChangeEventGenerator test = new StructuredPropertyChangeEventGenerator();

    Urn urn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    String entity = "dataset";
    String aspect = "structedProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    StructuredProperties structuredPropertiesTo = new StructuredProperties();
    StructuredPropertyValueAssignmentArray a = new StructuredPropertyValueAssignmentArray();
    StructuredPropertyValueAssignment prop1 = new StructuredPropertyValueAssignment();
    prop1.setPropertyUrn(new Urn("urn:li:structuredProperty:io.acryl.privacy.retentionTime"));
    PrimitivePropertyValueArray value = new PrimitivePropertyValueArray();
    value.add(PrimitivePropertyValue.create("90"));
    prop1.setValues(value);
    a.add(prop1);

    StructuredPropertyValueAssignment prop2 = new StructuredPropertyValueAssignment();
    prop2.setPropertyUrn(new Urn("urn:li:structuredProperty:io.acryl.privacy.retentionTimeNumber"));
    PrimitivePropertyValueArray value2 = new PrimitivePropertyValueArray();
    value2.add(PrimitivePropertyValue.create(12.0));
    prop2.setValues(value2);
    a.add(prop2);

    structuredPropertiesTo.setProperties(a);

    Aspect<StructuredProperties> from = new Aspect<>(null, new SystemMetadata());

    Aspect<StructuredProperties> to = new Aspect<>(structuredPropertiesTo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    String expectedValue = "[\"90\"]";

    String expectedValue2 = "[12.0]";

    assertEquals(2, actual.size());
    assertEquals(
        "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
        actual.get(0).getParameters().get("propertyUrn").toString());

    String ea = (String) actual.get(0).getParameters().get("propertyValues");
    assertEquals("ADD", actual.get(0).getOperation().toString());
    assertEquals(expectedValue, ea);

    assertEquals(
        "urn:li:structuredProperty:io.acryl.privacy.retentionTimeNumber",
        actual.get(1).getParameters().get("propertyUrn").toString());

    String ea2 = (String) actual.get(1).getParameters().get("propertyValues");

    assertEquals("ADD", actual.get(1).getOperation().toString());

    assertEquals(expectedValue2, ea2);
  }

  @Test
  public void testAddedNewPropertyChange() throws Exception {
    StructuredPropertyChangeEventGenerator test = new StructuredPropertyChangeEventGenerator();

    Urn urn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    String entity = "dataset";
    String aspect = "structedProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    StructuredProperties structuredPropertiesTo = new StructuredProperties();
    StructuredPropertyValueAssignmentArray a = new StructuredPropertyValueAssignmentArray();
    StructuredPropertyValueAssignment prop1 = new StructuredPropertyValueAssignment();
    prop1.setPropertyUrn(new Urn("urn:li:structuredProperty:io.acryl.privacy.retentionTime"));
    PrimitivePropertyValueArray value = new PrimitivePropertyValueArray();
    value.add(PrimitivePropertyValue.create("90"));
    prop1.setValues(value);
    a.add(prop1);
    structuredPropertiesTo.setProperties(a);

    StructuredProperties fromProperties = new StructuredProperties();
    fromProperties.setProperties(new StructuredPropertyValueAssignmentArray());

    Aspect<StructuredProperties> from = new Aspect<>(fromProperties, new SystemMetadata());

    Aspect<StructuredProperties> to = new Aspect<>(structuredPropertiesTo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    String expectedValue = ("[\"90\"]");

    assertEquals(1, actual.size());
    assertEquals(
        "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
        actual.get(0).getParameters().get("propertyUrn").toString());

    String ea = (String) actual.get(0).getParameters().get("propertyValues");
    assertEquals("ADD", actual.get(0).getOperation().toString());
    assertEquals(expectedValue, ea);
  }

  @Test
  public void testChangeValuePropertyChange() throws Exception {
    StructuredPropertyChangeEventGenerator test = new StructuredPropertyChangeEventGenerator();

    Urn urn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    String entity = "dataset";
    String aspect = "structedProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    StructuredProperties structuredPropertiesTo = new StructuredProperties();
    StructuredPropertyValueAssignmentArray a = new StructuredPropertyValueAssignmentArray();
    StructuredPropertyValueAssignment prop1 = new StructuredPropertyValueAssignment();
    prop1.setPropertyUrn(new Urn("urn:li:structuredProperty:io.acryl.privacy.retentionTime"));
    PrimitivePropertyValueArray value = new PrimitivePropertyValueArray();
    value.add(PrimitivePropertyValue.create("90"));
    prop1.setValues(value);
    a.add(prop1);
    structuredPropertiesTo.setProperties(a);

    StructuredProperties fromProperties = new StructuredProperties();
    StructuredPropertyValueAssignmentArray fromAssignmentArray =
        new StructuredPropertyValueAssignmentArray();
    StructuredPropertyValueAssignment propChanged = new StructuredPropertyValueAssignment();
    propChanged.setPropertyUrn(new Urn("urn:li:structuredProperty:io.acryl.privacy.retentionTime"));
    PrimitivePropertyValueArray fromValues = new PrimitivePropertyValueArray();
    fromValues.add(PrimitivePropertyValue.create("80"));
    propChanged.setValues(fromValues);
    fromAssignmentArray.add(propChanged);

    fromProperties.setProperties(fromAssignmentArray);

    Aspect<StructuredProperties> from = new Aspect<>(fromProperties, new SystemMetadata());

    Aspect<StructuredProperties> to = new Aspect<>(structuredPropertiesTo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    String expectedValue = ("[\"90\"]");

    assertEquals(1, actual.size());
    assertEquals(
        "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
        actual.get(0).getParameters().get("propertyUrn").toString());

    String ea = (String) actual.get(0).getParameters().get("propertyValues");
    assertEquals("MODIFY", actual.get(0).getOperation().toString());
    assertEquals(expectedValue, ea);
  }

  @Test
  public void testDeleteValuePropertyChange() throws Exception {
    StructuredPropertyChangeEventGenerator test = new StructuredPropertyChangeEventGenerator();

    Urn urn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    String entity = "dataset";
    String aspect = "structedProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    StructuredProperties structuredPropertiesTo = new StructuredProperties();
    StructuredPropertyValueAssignmentArray a = new StructuredPropertyValueAssignmentArray();
    StructuredPropertyValueAssignment prop1 = new StructuredPropertyValueAssignment();
    prop1.setPropertyUrn(new Urn("urn:li:structuredProperty:io.acryl.privacy.retentionTime"));
    PrimitivePropertyValueArray value = new PrimitivePropertyValueArray();
    value.add(PrimitivePropertyValue.create("90"));
    prop1.setValues(value);

    a.add(prop1);

    structuredPropertiesTo.setProperties(a);

    StructuredProperties fromProperties = new StructuredProperties();
    StructuredPropertyValueAssignmentArray fromAssignmentArray =
        new StructuredPropertyValueAssignmentArray();
    StructuredPropertyValueAssignment propOld = new StructuredPropertyValueAssignment();
    propOld.setPropertyUrn(new Urn("urn:li:structuredProperty:io.acryl.privacy.retentionTime"));
    PrimitivePropertyValueArray fromValues = new PrimitivePropertyValueArray();
    fromValues.add(PrimitivePropertyValue.create("90"));
    propOld.setValues(fromValues);
    fromAssignmentArray.add(propOld);

    StructuredPropertyValueAssignment prop2 = new StructuredPropertyValueAssignment();
    prop2.setPropertyUrn(new Urn("urn:li:structuredProperty:io.acryl.privacy.retentionTime2"));
    PrimitivePropertyValueArray value2 = new PrimitivePropertyValueArray();
    value2.add(PrimitivePropertyValue.create("30"));
    prop2.setValues(value2);
    fromAssignmentArray.add(prop2);

    fromProperties.setProperties(fromAssignmentArray);

    Aspect<StructuredProperties> from = new Aspect<>(fromProperties, new SystemMetadata());

    Aspect<StructuredProperties> to = new Aspect<>(structuredPropertiesTo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    String expectedValue = ("[\"30\"]");

    assertEquals(1, actual.size());
    assertEquals(
        "urn:li:structuredProperty:io.acryl.privacy.retentionTime2",
        actual.get(0).getParameters().get("propertyUrn").toString());

    String ea = (String) actual.get(0).getParameters().get("propertyValues");
    assertEquals("REMOVE", actual.get(0).getOperation().toString());
    assertEquals(expectedValue, ea);
  }
}

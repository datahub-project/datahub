package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.AssertJUnit.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public class StructuredPropertiesChangeEventGeneratorTest extends AbstractTestNGSpringContextTests {

  private static StructuredPropertyValueAssignment buildAssignment(
      String propertyUrn, String... values) throws Exception {
    StructuredPropertyValueAssignment assignment = new StructuredPropertyValueAssignment();
    assignment.setPropertyUrn(new Urn(propertyUrn));
    PrimitivePropertyValueArray valueArray = new PrimitivePropertyValueArray();
    for (String value : values) {
      valueArray.add(PrimitivePropertyValue.create(value));
    }
    assignment.setValues(valueArray);
    return assignment;
  }

  private static StructuredProperties buildStructuredProperties(
      StructuredPropertyValueAssignment... assignments) {
    StructuredProperties properties = new StructuredProperties();
    properties.setProperties(
        new StructuredPropertyValueAssignmentArray(Arrays.asList(assignments)));
    return properties;
  }

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
  public void testAddedPropertyWithAttributionEmitsSourceDetails() throws Exception {
    StructuredPropertyChangeEventGenerator test = new StructuredPropertyChangeEventGenerator();
    ObjectMapper objectMapper = new ObjectMapper();

    Urn urn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    StringMap sourceDetail =
        new StringMap(
            ImmutableMap.of(
                "propagated",
                "true",
                "propagation_direction",
                "downstream",
                "propagation_relationship",
                "lineage",
                "propagation_depth",
                "1"));

    StructuredProperties structuredPropertiesTo = new StructuredProperties();
    StructuredPropertyValueAssignmentArray a = new StructuredPropertyValueAssignmentArray();
    StructuredPropertyValueAssignment prop1 = new StructuredPropertyValueAssignment();
    prop1.setPropertyUrn(new Urn("urn:li:structuredProperty:io.acryl.privacy.retentionTime"));
    PrimitivePropertyValueArray value = new PrimitivePropertyValueArray();
    value.add(PrimitivePropertyValue.create("90"));
    prop1.setValues(value);
    prop1.setAttribution(
        new MetadataAttribution()
            .setTime(auditStamp.getTime())
            .setActor(auditStamp.getActor())
            .setSource(Urn.createFromString("urn:li:dataHubAction:test"))
            .setSourceDetail(sourceDetail));
    a.add(prop1);
    structuredPropertiesTo.setProperties(a);

    Aspect<StructuredProperties> from = new Aspect<>(null, new SystemMetadata());
    Aspect<StructuredProperties> to = new Aspect<>(structuredPropertiesTo, new SystemMetadata());

    List<ChangeEvent> actual =
        test.getChangeEvents(urn, "dataset", "structuredProperties", from, to, auditStamp);

    assertEquals(1, actual.size());
    assertEquals("ADD", actual.get(0).getOperation().toString());

    // sourceDetails is a JSON-stringified copy of the assignment's attribution sourceDetail map.
    Map<String, String> parsedSourceDetails =
        objectMapper.readValue(
            (String) actual.get(0).getParameters().get("sourceDetails"),
            objectMapper.getTypeFactory().constructMapType(Map.class, String.class, String.class));
    assertEquals(sourceDetail, parsedSourceDetails);
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

  @Test
  public void testDeletePropertySortingBeforeRemainingProperty() throws Exception {
    StructuredPropertyChangeEventGenerator test = new StructuredPropertyChangeEventGenerator();

    Urn urn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    String entity = "dataset";
    String aspect = "structedProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    // Removed property sorts lexicographically BEFORE the property that remains.
    StructuredPropertyValueAssignment removedProp = new StructuredPropertyValueAssignment();
    removedProp.setPropertyUrn(new Urn("urn:li:structuredProperty:io.acryl.aRemovedProperty"));
    PrimitivePropertyValueArray removedValues = new PrimitivePropertyValueArray();
    removedValues.add(PrimitivePropertyValue.create("42"));
    removedProp.setValues(removedValues);

    StructuredPropertyValueAssignment remainingProp = new StructuredPropertyValueAssignment();
    remainingProp.setPropertyUrn(
        new Urn("urn:li:structuredProperty:io.acryl.privacy.retentionTime"));
    PrimitivePropertyValueArray remainingValues = new PrimitivePropertyValueArray();
    remainingValues.add(PrimitivePropertyValue.create("90"));
    remainingProp.setValues(remainingValues);

    StructuredProperties fromProperties = new StructuredProperties();
    StructuredPropertyValueAssignmentArray fromAssignmentArray =
        new StructuredPropertyValueAssignmentArray();
    fromAssignmentArray.add(removedProp);
    fromAssignmentArray.add(remainingProp);
    fromProperties.setProperties(fromAssignmentArray);

    StructuredProperties structuredPropertiesTo = new StructuredProperties();
    StructuredPropertyValueAssignmentArray toAssignmentArray =
        new StructuredPropertyValueAssignmentArray();
    toAssignmentArray.add(remainingProp);
    structuredPropertiesTo.setProperties(toAssignmentArray);

    Aspect<StructuredProperties> from = new Aspect<>(fromProperties, new SystemMetadata());
    Aspect<StructuredProperties> to = new Aspect<>(structuredPropertiesTo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(1, actual.size());
    assertEquals("REMOVE", actual.get(0).getOperation().toString());
    assertNotNull(actual.get(0).getParameters());
    assertEquals(
        "urn:li:structuredProperty:io.acryl.aRemovedProperty",
        actual.get(0).getParameters().get("propertyUrn").toString());
    assertEquals("[\"42\"]", actual.get(0).getParameters().get("propertyValues"));
  }

  @Test
  public void testAllPropertiesRemoved() throws Exception {
    StructuredPropertyChangeEventGenerator test = new StructuredPropertyChangeEventGenerator();

    Urn urn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    String entity = "dataset";
    String aspect = "structedProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    StructuredProperties fromProperties =
        buildStructuredProperties(
            buildAssignment("urn:li:structuredProperty:io.acryl.propertyA", "1"),
            buildAssignment("urn:li:structuredProperty:io.acryl.propertyB", "2"));

    // Aspect deletion: the new value is null.
    Aspect<StructuredProperties> from = new Aspect<>(fromProperties, new SystemMetadata());
    Aspect<StructuredProperties> to = new Aspect<>(null, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(2, actual.size());

    assertEquals("REMOVE", actual.get(0).getOperation().toString());
    assertNotNull(actual.get(0).getParameters());
    assertEquals(
        "urn:li:structuredProperty:io.acryl.propertyA",
        actual.get(0).getParameters().get("propertyUrn").toString());
    assertEquals("[\"1\"]", actual.get(0).getParameters().get("propertyValues"));

    assertEquals("REMOVE", actual.get(1).getOperation().toString());
    assertNotNull(actual.get(1).getParameters());
    assertEquals(
        "urn:li:structuredProperty:io.acryl.propertyB",
        actual.get(1).getParameters().get("propertyUrn").toString());
    assertEquals("[\"2\"]", actual.get(1).getParameters().get("propertyValues"));
  }

  @Test
  public void testAddRemoveAndModifyInSingleChange() throws Exception {
    StructuredPropertyChangeEventGenerator test = new StructuredPropertyChangeEventGenerator();

    Urn urn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    String entity = "dataset";
    String aspect = "structedProperties";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    StructuredProperties fromProperties =
        buildStructuredProperties(
            buildAssignment("urn:li:structuredProperty:io.acryl.propertyA", "1"),
            buildAssignment("urn:li:structuredProperty:io.acryl.propertyC", "3"));

    StructuredProperties toProperties =
        buildStructuredProperties(
            buildAssignment("urn:li:structuredProperty:io.acryl.propertyB", "2"),
            buildAssignment("urn:li:structuredProperty:io.acryl.propertyC", "3", "4"));

    Aspect<StructuredProperties> from = new Aspect<>(fromProperties, new SystemMetadata());
    Aspect<StructuredProperties> to = new Aspect<>(toProperties, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    // Events are ordered by property urn: A removed, B added, C modified.
    assertEquals(3, actual.size());

    assertEquals("REMOVE", actual.get(0).getOperation().toString());
    assertEquals(
        "urn:li:structuredProperty:io.acryl.propertyA",
        actual.get(0).getParameters().get("propertyUrn").toString());
    assertEquals("[\"1\"]", actual.get(0).getParameters().get("propertyValues"));

    assertEquals("ADD", actual.get(1).getOperation().toString());
    assertEquals(
        "urn:li:structuredProperty:io.acryl.propertyB",
        actual.get(1).getParameters().get("propertyUrn").toString());
    assertEquals("[\"2\"]", actual.get(1).getParameters().get("propertyValues"));

    // MODIFY carries the new (multi-)values.
    assertEquals("MODIFY", actual.get(2).getOperation().toString());
    assertEquals(
        "urn:li:structuredProperty:io.acryl.propertyC",
        actual.get(2).getParameters().get("propertyUrn").toString());
    assertEquals("[\"3\",\"4\"]", actual.get(2).getParameters().get("propertyValues"));
  }
}

package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.AssertJUnit.assertEquals;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.restli.internal.server.util.DataMapUtils;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public class SchemaMetadataChangeEventGeneratorTest extends AbstractTestNGSpringContextTests {

  private static Urn getTestUrn() throws URISyntaxException {
    return Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
  }

  private static AuditStamp getTestAuditStamp() throws URISyntaxException {
    return new AuditStamp()
        .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
        .setTime(1683829509553L);
  }

  private static void compareDescriptions(
      Set<String> expectedDescriptions, List<ChangeEvent> actual) {
    Set<String> actualDescriptions = new HashSet<>();
    actual.forEach(
        changeEvent -> {
          actualDescriptions.add(changeEvent.getDescription());
        });
    assertEquals(expectedDescriptions, actualDescriptions);
  }

  private static Aspect<SchemaMetadata> getSchemaMetadata(List<SchemaField> schemaFieldList) {
    return new Aspect<>(
        new SchemaMetadata().setFields(new SchemaFieldArray(schemaFieldList)),
        new SystemMetadata());
  }

  @Test
  public void testNativeSchemaBackwardIncompatibleChange() throws Exception {
    SchemaMetadataChangeEventGenerator test = new SchemaMetadataChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "schemaMetadata";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<SchemaMetadata> from =
        getSchemaMetadata(
            List.of(new SchemaField().setFieldPath("ID").setNativeDataType("NUMBER(16,1)")));
    Aspect<SchemaMetadata> to =
        getSchemaMetadata(
            List.of(new SchemaField().setFieldPath("ID").setNativeDataType("NUMBER(10,1)")));
    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);
    // Test single field going from NUMBER(16,1) -> NUMBER(10,1)
    assertEquals(1, actual.size());
    compareDescriptions(
        Set.of(
            "A backwards incompatible change due to native datatype of the field 'ID' changed from 'NUMBER(16,1)' to 'NUMBER(10,1)'."),
        actual);

    List<ChangeEvent> actual2 = test.getChangeEvents(urn, entity, aspect, to, from, auditStamp);
    // Test single field going from NUMBER(10,1) -> NUMBER(16,1)
    assertEquals(1, actual2.size());
    compareDescriptions(
        Set.of(
            "A backwards incompatible change due to native datatype of the field 'ID' changed from 'NUMBER(10,1)' to 'NUMBER(16,1)'."),
        actual2);
  }

  @Test
  public void testNativeSchemaFieldAddition() throws Exception {
    SchemaMetadataChangeEventGenerator test = new SchemaMetadataChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "schemaMetadata";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<SchemaMetadata> from =
        getSchemaMetadata(
            List.of(new SchemaField().setFieldPath("ID").setNativeDataType("NUMBER(16,1)")));
    Aspect<SchemaMetadata> to3 =
        getSchemaMetadata(
            List.of(
                new SchemaField().setFieldPath("aa").setNativeDataType("NUMBER(10,1)"),
                new SchemaField().setFieldPath("ID").setNativeDataType("NUMBER(10,1)")));
    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to3, auditStamp);
    assertEquals(2, actual.size());
    compareDescriptions(
        Set.of(
            "A backwards incompatible change due to native datatype of the field 'ID' changed from 'NUMBER(16,1)' to 'NUMBER(10,1)'.",
            "A forwards & backwards compatible change due to the newly added field 'aa'."),
        actual);
  }

  @Test
  public void testSchemaFieldRename() throws Exception {
    SchemaMetadataChangeEventGenerator test = new SchemaMetadataChangeEventGenerator();

    Urn urn = getTestUrn();
    String entity = "dataset";
    String aspect = "schemaMetadata";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<SchemaMetadata> from =
        getSchemaMetadata(
            List.of(new SchemaField().setFieldPath("ID").setNativeDataType("NUMBER(16,1)")));
    Aspect<SchemaMetadata> to3 =
        getSchemaMetadata(
            List.of(new SchemaField().setFieldPath("ID2").setNativeDataType("NUMBER(16,1)")));
    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to3, auditStamp);
    compareDescriptions(
        Set.of(
            "A forwards & backwards compatible change due to renaming of the field 'ID to ID2'."),
        actual);
    assertEquals(1, actual.size());
  }

  @Test
  public void testDelete() throws Exception {
    SchemaMetadataChangeEventGenerator test = new SchemaMetadataChangeEventGenerator();

    Urn urn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    String entity = "dataset";
    String aspect = "schemaMetadata";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);
    Aspect<SchemaMetadata> from =
        new Aspect<>(
            DataMapUtils.read(
                IOUtils.toInputStream(TEST_OBJECT, StandardCharsets.UTF_8),
                SchemaMetadata.class,
                Map.of()),
            new SystemMetadata());
    Aspect<SchemaMetadata> to = new Aspect<>(null, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(14, actual.size());
  }

  // CHECKSTYLE:OFF
  private static final String TEST_OBJECT =
      "{\"platformSchema\":{\"com.linkedin.schema.KafkaSchema\":{\"documentSchema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"SampleHdfsSchema\\\",\\\"namespace\\\":\\\"com.linkedin.dataset\\\",\\\"doc\\\":\\\"Sample HDFS dataset\\\",\\\"fields\\\":[{\\\"name\\\":\\\"field_foo\\\",\\\"type\\\":[\\\"string\\\"]},{\\\"name\\\":\\\"field_bar\\\",\\\"type\\\":[\\\"boolean\\\"]}]}\"}},\"created\":{\"actor\":\"urn:li:corpuser:jdoe\",\"time\":1674291843000},\"lastModified\":{\"actor\":\"urn:li:corpuser:jdoe\",\"time\":1674291843000},\"fields\":[{\"nullable\":false,\"fieldPath\":\"shipment_info\",\"description\":\"Shipment info description\",\"isPartOfKey\":false,\"type\":{\"type\":{\"com.linkedin.schema.RecordType\":{}}},\"recursive\":false,\"nativeDataType\":\"varchar(100)\"},{\"nullable\":false,\"fieldPath\":\"shipment_info.date\",\"description\":\"Shipment info date description\",\"isPartOfKey\":false,\"type\":{\"type\":{\"com.linkedin.schema.DateType\":{}}},\"recursive\":false,\"nativeDataType\":\"Date\"},{\"nullable\":false,\"fieldPath\":\"shipment_info.target\",\"description\":\"Shipment info target description\",\"isPartOfKey\":false,\"type\":{\"type\":{\"com.linkedin.schema.StringType\":{}}},\"recursive\":false,\"nativeDataType\":\"text\"},{\"nullable\":false,\"fieldPath\":\"shipment_info.destination\",\"description\":\"Shipment info destination description\",\"isPartOfKey\":false,\"type\":{\"type\":{\"com.linkedin.schema.StringType\":{}}},\"recursive\":false,\"nativeDataType\":\"varchar(100)\"},{\"nullable\":false,\"fieldPath\":\"shipment_info.geo_info\",\"description\":\"Shipment info geo_info description\",\"isPartOfKey\":false,\"type\":{\"type\":{\"com.linkedin.schema.RecordType\":{}}},\"recursive\":false,\"nativeDataType\":\"varchar(100)\"},{\"nullable\":false,\"fieldPath\":\"shipment_info.geo_info.lat\",\"description\":\"Shipment info geo_info lat\",\"isPartOfKey\":false,\"type\":{\"type\":{\"com.linkedin.schema.NumberType\":{}}},\"recursive\":false,\"nativeDataType\":\"float\"},{\"nullable\":false,\"fieldPath\":\"shipment_info.geo_info.lng\",\"description\":\"Shipment info geo_info lng\",\"isPartOfKey\":false,\"type\":{\"type\":{\"com.linkedin.schema.NumberType\":{}}},\"recursive\":false,\"nativeDataType\":\"float\"}],\"schemaName\":\"SampleHdfsSchema\",\"version\":0,\"hash\":\"\",\"platform\":\"urn:li:dataPlatform:hdfs\"}";
  // CHECKSTYLE:ON
}

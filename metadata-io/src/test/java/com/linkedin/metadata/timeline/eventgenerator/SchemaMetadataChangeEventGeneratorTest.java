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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public class SchemaMetadataChangeEventGeneratorTest extends AbstractTestNGSpringContextTests {

  @Test
  public void testNativeSchemaBackwardIncompatibleChange() throws Exception {
    SchemaMetadataChangeEventGenerator test = new SchemaMetadataChangeEventGenerator();

    Urn urn =
            Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    String entity = "dataset";
    String aspect = "schemaMetadata";
    AuditStamp auditStamp =
            new AuditStamp()
                    .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
                    .setTime(1683829509553L);

    List<SchemaField> schemaFields1 = new ArrayList<>();
    schemaFields1.add(new SchemaField().setFieldPath("ID").setNativeDataType("NUMBER(16,1)"));
    Aspect<SchemaMetadata> from =
            new Aspect<>(
                    new SchemaMetadata().setFields(new SchemaFieldArray(schemaFields1)),
                    new SystemMetadata());

    List<SchemaField> schemaFields2 = new ArrayList<>();
    schemaFields2.add(new SchemaField().setFieldPath("ID").setNativeDataType("NUMBER(10,1)"));
    Aspect<SchemaMetadata> to =
            new Aspect<>(
                    new SchemaMetadata().setFields(new SchemaFieldArray(schemaFields2)),
                    new SystemMetadata());
    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);
    // Test single field going from NUMBER(16,1) -> NUMBER(10,1)
    assertEquals(1, actual.size());

    List<ChangeEvent> actual2 = test.getChangeEvents(urn, entity, aspect, to, from, auditStamp);
    // Test single field going from NUMBER(10,1) -> NUMBER(16,1)
    assertEquals(1, actual2.size());

    List<SchemaField> schemaFields3 = new ArrayList<>();
    schemaFields2.add(new SchemaField().setFieldPath("aa").setNativeDataType("NUMBER(10,1)"));
    schemaFields2.add(new SchemaField().setFieldPath("ID").setNativeDataType("NUMBER(10,1)"));
    Aspect<SchemaMetadata> to3 =
            new Aspect<>(
                    new SchemaMetadata().setFields(new SchemaFieldArray(schemaFields3)),
                    new SystemMetadata());
    List<ChangeEvent> actual3 = test.getChangeEvents(urn, entity, aspect, from, to3, auditStamp);
    ChangeEvent actual3Event = actual3.get(0);
    System.out.println("actual3Event description is " + actual3Event.getDescription());
    // Test single field going from NUMBER(10,1) to 2 fields each of NUMBER(16,1). One new field aa added
    assertEquals(2, actual3.size());
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

package com.linkedin.metadata;

import static com.datahub.utils.TestUtils.*;
import static org.testng.Assert.*;

import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.mxe.FailedMetadataChangeEvent;
import com.linkedin.mxe.MetadataAuditEvent;
import com.linkedin.mxe.MetadataChangeEvent;
import java.io.IOException;
import java.io.InputStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.testng.annotations.Test;

public class EventUtilsTests {

  @Test
  public void testAvroToPegasusMAE() throws IOException {
    GenericRecord record =
        genericRecordFromResource(
            "test-avro2pegasus-mae.json", com.linkedin.pegasus2avro.mxe.MetadataAuditEvent.SCHEMA$);

    MetadataAuditEvent mae = EventUtils.avroToPegasusMAE(record);

    assertEquals(
        mae.getNewSnapshot()
            .getDatasetSnapshot()
            .getAspects()
            .get(0)
            .getOwnership()
            .getOwners()
            .get(0)
            .getOwner(),
        new CorpuserUrn("foobar"));
  }

  @Test
  public void testAvroToPegasusMCE() throws IOException {
    GenericRecord record =
        genericRecordFromResource(
            "test-avro2pegasus-mce.json",
            com.linkedin.pegasus2avro.mxe.MetadataChangeEvent.SCHEMA$);

    MetadataChangeEvent mce = EventUtils.avroToPegasusMCE(record);

    assertEquals(
        mce.getProposedSnapshot()
            .getDatasetSnapshot()
            .getAspects()
            .get(0)
            .getOwnership()
            .getOwners()
            .get(0)
            .getOwner(),
        new CorpuserUrn("foobar"));
  }

  @Test
  public void testPegasusToAvroMAE() throws IOException {
    MetadataAuditEvent event =
        recordTemplateFromResource("test-pegasus2avro-mae.json", MetadataAuditEvent.class);

    GenericRecord record = EventUtils.pegasusToAvroMAE(event);

    assertEquals(record.getSchema(), com.linkedin.pegasus2avro.mxe.MetadataAuditEvent.SCHEMA$);
    assertNotNull(record.get("newSnapshot"));
  }

  @Test
  public void testPegasusToAvroMCE() throws IOException {
    MetadataChangeEvent event =
        recordTemplateFromResource("test-pegasus2avro-mce.json", MetadataChangeEvent.class);

    GenericRecord record = EventUtils.pegasusToAvroMCE(event);

    assertEquals(record.getSchema(), com.linkedin.pegasus2avro.mxe.MetadataChangeEvent.SCHEMA$);
    assertNotNull(record.get("proposedSnapshot"));
  }

  @Test
  public void testPegasusToAvroFailedMCE() throws IOException {
    FailedMetadataChangeEvent event =
        recordTemplateFromResource("test-pegasus2avro-fmce.json", FailedMetadataChangeEvent.class);

    GenericRecord record = EventUtils.pegasusToAvroFailedMCE(event);

    assertEquals(
        record.getSchema(), com.linkedin.pegasus2avro.mxe.FailedMetadataChangeEvent.SCHEMA$);
    assertNotNull(record.get("error"));
    assertNotNull(record.get("metadataChangeEvent"));
  }

  private GenericRecord genericRecordFromResource(String resourcePath, Schema schema)
      throws IOException {
    InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath);
    JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, is);
    DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    return reader.read(null, decoder);
  }

  private <T extends RecordTemplate> T recordTemplateFromResource(
      String resourcePath, Class<? extends RecordTemplate> clazz) throws IOException {
    String json = loadJsonFromResource(resourcePath);
    return (T) RecordUtils.toRecordTemplate(clazz, json);
  }
}

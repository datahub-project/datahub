package com.linkedin.metadata;

import static org.testng.Assert.assertEquals;

import com.linkedin.common.FabricType;
import com.linkedin.mxe.MetadataChangeEvent;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.testng.annotations.Test;

/**
 * Legacy MCEs written by a producer that still has the schema from before env / origin became the
 * FabricTypeName string typeref (an enum in Avro), read by this version's consumer path.
 */
public class FabricTypeMceCompatTest {

  private static final Schema MASTER_MCE =
      load("fabric-type-compat/master-MetadataChangeEvent.avsc");

  @Test
  public void testMceWithDataFlowInfoEnvFromPreviousSchema() throws IOException {
    GenericRecord info =
        new GenericRecordBuilder(named("com.linkedin.pegasus2avro.datajob.DataFlowInfo"))
            .set("name", "flow")
            .set("env", enumSymbol("PROD"))
            .build();
    GenericRecord snapshot =
        new GenericRecordBuilder(
                named("com.linkedin.pegasus2avro.metadata.snapshot.DataFlowSnapshot"))
            .set("urn", "urn:li:dataFlow:(airflow,flow,PROD)")
            .set("aspects", java.util.List.of(info))
            .build();

    MetadataChangeEvent mce = EventUtils.avroToPegasusMCE(asConsumedFromKafka(mce(snapshot)));

    assertEquals(
        mce.getProposedSnapshot()
            .getDataFlowSnapshot()
            .getAspects()
            .get(0)
            .getDataFlowInfo()
            .getEnv(),
        FabricType.PROD);
  }

  @Test
  public void testMceWithDatasetKeyOriginFromPreviousSchema() throws IOException {
    GenericRecord key =
        new GenericRecordBuilder(named("com.linkedin.pegasus2avro.metadata.key.DatasetKey"))
            .set("platform", "urn:li:dataPlatform:hive")
            .set("name", "db.t")
            .set("origin", enumSymbol("PROD"))
            .build();
    GenericRecord snapshot =
        new GenericRecordBuilder(
                named("com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot"))
            .set("urn", "urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)")
            .set("aspects", java.util.List.of(key))
            .build();

    MetadataChangeEvent mce = EventUtils.avroToPegasusMCE(asConsumedFromKafka(mce(snapshot)));

    assertEquals(
        mce.getProposedSnapshot()
            .getDatasetSnapshot()
            .getAspects()
            .get(0)
            .getDatasetKey()
            .getOrigin(),
        FabricType.PROD);
  }

  private static GenericRecord mce(GenericRecord snapshot) {
    return new GenericRecordBuilder(MASTER_MCE).set("proposedSnapshot", snapshot).build();
  }

  /** Binary round trip with the producer's schema, as KafkaAvroDeserializer returns it. */
  private static GenericRecord asConsumedFromKafka(GenericRecord record) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(os, null);
    new GenericDatumWriter<GenericRecord>(MASTER_MCE).write(record, encoder);
    encoder.flush();
    return new GenericDatumReader<GenericRecord>(MASTER_MCE)
        .read(
            null,
            DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(os.toByteArray()), null));
  }

  private static GenericData.EnumSymbol enumSymbol(String symbol) {
    return new GenericData.EnumSymbol(named("com.linkedin.pegasus2avro.common.FabricType"), symbol);
  }

  private static Schema named(String fullName) {
    Schema found = find(MASTER_MCE, fullName, new java.util.HashSet<>());
    if (found == null) {
      throw new IllegalStateException(fullName + " not in the master MCE schema");
    }
    return found;
  }

  private static Schema find(Schema schema, String fullName, java.util.Set<String> seen) {
    switch (schema.getType()) {
      case RECORD:
      case ENUM:
      case FIXED:
        if (schema.getFullName().equals(fullName)) {
          return schema;
        }
        if (schema.getType() != Schema.Type.RECORD || !seen.add(schema.getFullName())) {
          return null;
        }
        for (Schema.Field field : schema.getFields()) {
          Schema hit = find(field.schema(), fullName, seen);
          if (hit != null) {
            return hit;
          }
        }
        return null;
      case UNION:
        for (Schema branch : schema.getTypes()) {
          Schema hit = find(branch, fullName, seen);
          if (hit != null) {
            return hit;
          }
        }
        return null;
      case ARRAY:
        return find(schema.getElementType(), fullName, seen);
      case MAP:
        return find(schema.getValueType(), fullName, seen);
      default:
        return null;
    }
  }

  private static Schema load(String resource) {
    try (InputStream in =
        FabricTypeMceCompatTest.class.getClassLoader().getResourceAsStream(resource)) {
      return new Schema.Parser().parse(in);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}

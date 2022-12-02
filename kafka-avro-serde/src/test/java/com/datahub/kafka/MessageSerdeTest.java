package com.datahub.kafka;
/*
import com.datahub.kafka.avro.deserializer.KafkaAvroDeserializer;
import com.datahub.kafka.avro.serializer.KafkaAvroSerializer;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EventUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;


public class MessageSerdeTest {

  @Test
  public void testSerialization() throws IOException {
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)");
    DatasetProperties datasetProperties = new DatasetProperties();
    datasetProperties.setName("Foo Bar");
    com.linkedin.mxe.MetadataChangeProposal gmce = new MetadataChangeProposal();
    gmce.setEntityUrn(entityUrn);
    gmce.setChangeType(ChangeType.UPSERT);
    gmce.setEntityType("dataset");
    gmce.setAspectName("datasetProperties");
    JacksonDataTemplateCodec dataTemplateCodec = new JacksonDataTemplateCodec();
    byte[] datasetPropertiesSerialized = dataTemplateCodec.dataTemplateToBytes(datasetProperties);
    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setValue(ByteString.unsafeWrap(datasetPropertiesSerialized));
    genericAspect.setContentType("application/json");
    gmce.setAspect(genericAspect);

    GenericRecord genericRecord = EventUtils.pegasusToAvroMCP(gmce);

    KafkaAvroSerializer serializer = new KafkaAvroSerializer();
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    byte[] bytes =
        serializer.serialize(com.linkedin.pegasus2avro.mxe.MetadataChangeProposal.class.getName(), genericRecord);

    Object deserialize =
        deserializer.deserialize(com.linkedin.pegasus2avro.mxe.MetadataChangeProposal.class.getName(), bytes);

    assertEquals(genericRecord, deserialize);

    MetadataChangeProposal metadataChangeProposal = EventUtils.avroToPegasusMCP((GenericRecord) deserialize);

    assertEquals(metadataChangeProposal, gmce);
  }
}
*/
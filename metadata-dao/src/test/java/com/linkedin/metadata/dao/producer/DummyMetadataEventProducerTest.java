package com.linkedin.metadata.dao.producer;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import org.testng.annotations.Test;


public class DummyMetadataEventProducerTest {

  // ensure the producer can be created since it has default snapshot and aspect validation
  @Test
  public void testCreateDummyMetadataEventProducer() {
    DummyMetadataEventProducer<CorpuserUrn> producer = new DummyMetadataEventProducer<>();
    CorpuserUrn userUrn = new CorpuserUrn("fakeUser");
    CorpUserSnapshot val = new CorpUserSnapshot();

    producer.produceMetadataAuditEvent(userUrn, val, val);
  }
}

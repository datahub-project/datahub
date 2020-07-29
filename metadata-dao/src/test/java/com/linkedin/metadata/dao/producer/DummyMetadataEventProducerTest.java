package com.linkedin.metadata.dao.producer;

import com.linkedin.common.urn.Urn;
import com.linkedin.testing.AspectFoo;
import org.testng.annotations.Test;

import static com.linkedin.testing.TestUtils.*;


public class DummyMetadataEventProducerTest {

  // ensure the producer can be created since it has default snapshot and aspect validation
  @Test
  public void testCreateDummyMetadataEventProducer() {
    DummyMetadataEventProducer<Urn> producer = new DummyMetadataEventProducer<>();
    Urn urn = makeUrn(1);
    AspectFoo oldValue = new AspectFoo().setValue("old");
    AspectFoo newValue = new AspectFoo().setValue("new");

    producer.produceMetadataAuditEvent(urn, oldValue, newValue);
    producer.produceAspectSpecificMetadataAuditEvent(urn, oldValue, newValue);
  }
}

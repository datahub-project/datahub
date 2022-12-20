package io.datahubproject.openapi.schema.registry;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.openapi.test.KafkaTestConsumer;
import io.datahubproject.openapi.test.OpenAPISpringTestServer;
import io.datahubproject.openapi.test.OpenAPISpringTestServerConfiguration;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


@ActiveProfiles("test")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
    classes = {OpenAPISpringTestServer.class, OpenAPISpringTestServerConfiguration.class})
@EnableKafka
public class SchemaRegistryControllerTest extends AbstractTestNGSpringContextTests {

  @Autowired
  EventProducer _producer;

  @Autowired
  KafkaTestConsumer _consumer;

  @Test
  public void testConsumption() throws IOException, InterruptedException {
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)");
    DatasetProperties datasetProperties = new DatasetProperties();
    datasetProperties.setName("Foo Bar");
    MetadataChangeProposal gmce = new MetadataChangeProposal();
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

    _producer.produceMetadataChangeProposal(gmce);
    Thread.sleep(10000);
    boolean messageConsumed = _consumer.getLatch().await(10, TimeUnit.SECONDS);
    assertTrue(messageConsumed);
  }
}
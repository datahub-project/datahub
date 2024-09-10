package com.linkedin.datahub.upgrade;

import static com.linkedin.metadata.EventUtils.RENAMED_MCL_AVRO_SCHEMA;
import static com.linkedin.metadata.boot.kafka.MockSystemUpdateSerializer.topicToSubjectName;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.datahub.upgrade.system.SystemUpdate;
import com.linkedin.metadata.boot.kafka.MockSystemUpdateDeserializer;
import com.linkedin.metadata.boot.kafka.MockSystemUpdateSerializer;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.mxe.Topics;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@ActiveProfiles("test")
@SpringBootTest(
    classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class},
    properties = {
      "kafka.schemaRegistry.type=INTERNAL",
      "DATAHUB_UPGRADE_HISTORY_TOPIC_NAME=" + Topics.DATAHUB_UPGRADE_HISTORY_TOPIC_NAME,
      "METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME=" + Topics.METADATA_CHANGE_LOG_VERSIONED,
    },
    args = {"-u", "SystemUpdate"})
public class DatahubUpgradeNoSchemaRegistryTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Named("systemUpdate")
  private SystemUpdate systemUpdate;

  @Autowired
  @Named("kafkaEventProducer")
  private KafkaEventProducer kafkaEventProducer;

  @Autowired
  @Named("duheKafkaEventProducer")
  private KafkaEventProducer duheKafkaEventProducer;

  @Autowired private EntityServiceImpl entityService;

  @Autowired
  @Named("schemaRegistryConfig")
  private KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig;

  @Test
  public void testSystemUpdateInit() {
    assertNotNull(systemUpdate);
  }

  @Test
  public void testSystemUpdateKafkaProducerOverride() throws RestClientException, IOException {
    assertEquals(
        schemaRegistryConfig.getValue().getDeserializer(),
        MockSystemUpdateDeserializer.class.getName());
    assertEquals(
        schemaRegistryConfig.getValue().getSerializer(),
        MockSystemUpdateSerializer.class.getName());
    assertEquals(kafkaEventProducer, duheKafkaEventProducer);
    assertEquals(entityService.getProducer(), duheKafkaEventProducer);

    MockSystemUpdateSerializer serializer = new MockSystemUpdateSerializer();
    serializer.configure(schemaRegistryConfig.getProperties(null), false);
    SchemaRegistryClient registry = serializer.getSchemaRegistryClient();
    assertEquals(
        registry.getId(
            topicToSubjectName(Topics.METADATA_CHANGE_LOG_VERSIONED), RENAMED_MCL_AVRO_SCHEMA),
        2);
  }

  @Test
  public void testSystemUpdateSend() {
    DataHubUpgradeState result =
        systemUpdate.steps().stream()
            .filter(s -> s.id().equals("DataHubStartupStep"))
            .findFirst()
            .get()
            .executable()
            .apply(
                new UpgradeContext() {
                  @Override
                  public Upgrade upgrade() {
                    return null;
                  }

                  @Override
                  public List<UpgradeStepResult> stepResults() {
                    return null;
                  }

                  @Override
                  public UpgradeReport report() {
                    return null;
                  }

                  @Override
                  public List<String> args() {
                    return null;
                  }

                  @Override
                  public Map<String, Optional<String>> parsedArgs() {
                    return null;
                  }

                  @Override
                  public OperationContext opContext() {
                    return mock(OperationContext.class);
                  }
                })
            .result();
    assertEquals("SUCCEEDED", result.toString());
  }
}

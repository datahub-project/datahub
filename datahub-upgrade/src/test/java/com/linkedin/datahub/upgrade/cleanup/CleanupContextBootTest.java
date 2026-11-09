package com.linkedin.datahub.upgrade.cleanup;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeCliApplication;
import com.linkedin.datahub.upgrade.UpgradeCliApplicationTestConfiguration;
import com.linkedin.datahub.upgrade.shared.NoOpKafkaEventProducer;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.event.EventProducer;
import jakarta.inject.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * Boots the {@code -u Cleanup} context without overriding {@code kafkaEventProducer}.
 * EntityServiceFactory depends on that bean, so the Cleanup context must supply its own or the
 * upgrade fails to start.
 */
@ActiveProfiles({"upgrade", "test"})
@SpringBootTest(
    classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class},
    args = {"-u", "Cleanup"})
public class CleanupContextBootTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Named("cleanup")
  private Cleanup cleanup;

  @Autowired
  @Named("kafkaEventProducer")
  private EventProducer kafkaEventProducer;

  @Autowired
  @Named("entityService")
  private EntityService<?> entityService;

  @Test
  public void testContextBoots() {
    assertNotNull(cleanup);
  }

  @Test
  public void testKafkaEventProducerIsNoOp() {
    assertTrue(
        kafkaEventProducer instanceof NoOpKafkaEventProducer,
        "Cleanup must not publish to Kafka; got " + kafkaEventProducer.getClass());
    assertSame(((EntityServiceImpl) entityService).getProducer(), kafkaEventProducer);
  }
}

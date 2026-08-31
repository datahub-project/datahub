package com.linkedin.metadata.kafka;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;

import com.datahub.event.PlatformEventProcessor;
import com.datahub.event.hook.BusinessAttributeUpdateHook;
import com.datahub.event.hook.PlatformEventHook;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.kafka.hook.UpdateIndicesHook;
import com.linkedin.metadata.kafka.hook.event.PlatformEventGeneratorHook;
import com.linkedin.metadata.kafka.hook.form.FormAssignmentHook;
import com.linkedin.metadata.kafka.hook.incident.IncidentsSummaryHook;
import com.linkedin.metadata.kafka.hook.ingestion.IngestionSchedulerHook;
import com.linkedin.metadata.kafka.hook.siblings.SiblingAssociationHook;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.service.FormService;
import io.datahubproject.metadata.jobs.common.health.kafka.KafkaHealthIndicator;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@ActiveProfiles("test")
@SpringBootTest(
    classes = {MaeConsumerApplication.class, MaeConsumerApplicationTestConfiguration.class},
    properties = {"PE_CONSUMER_ENABLED=true", "spring.main.allow-bean-definition-overriding=true"})
public class MaeConsumerApplicationTest extends AbstractTestNGSpringContextTests {

  @Autowired private KafkaHealthIndicator kafkaHealthIndicator;

  @Autowired private FormService formService;

  @Autowired private EntitySearchService entitySearchService;

  @Autowired private List<MetadataChangeLogHook> mclHooks;

  @Autowired private List<PlatformEventHook> platformEventHooks;

  @Autowired private PlatformEventProcessor platformEventProcessor;

  @Test
  public void testMaeConsumerAutoWiring() {
    assertNotNull(kafkaHealthIndicator);
    assertNotNull(formService);
    assertNotNull(platformEventProcessor);
  }

  @Test
  public void testMCLHooks() {
    List<Class<?>> expectedHooks =
        List.of(
            UpdateIndicesHook.class,
            IngestionSchedulerHook.class,
            PlatformEventGeneratorHook.class,
            SiblingAssociationHook.class,
            FormAssignmentHook.class,
            IncidentsSummaryHook.class,
            SiblingAssociationHook.class,
            IncidentsSummaryHook.class);

    for (Class<?> hookClazz : expectedHooks) {
      assertTrue(
          mclHooks.stream().anyMatch(hookClazz::isInstance),
          "Expected hook " + hookClazz.getSimpleName());
    }
  }

  @Test
  public void testPlatformHooks() {
    List<Class<?>> expectedHooks = List.of(BusinessAttributeUpdateHook.class);

    for (Class<?> hookClazz : expectedHooks) {
      assertTrue(
          platformEventHooks.stream().anyMatch(hookClazz::isInstance),
          "Expected hook " + hookClazz.getSimpleName());
    }
  }
}

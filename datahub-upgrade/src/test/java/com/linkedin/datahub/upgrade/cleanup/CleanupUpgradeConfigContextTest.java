package com.linkedin.datahub.upgrade.cleanup;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeCliApplication;
import com.linkedin.datahub.upgrade.UpgradeCliApplicationTestConfiguration;
import com.linkedin.datahub.upgrade.config.GeneralUpgradeConfiguration;
import com.linkedin.datahub.upgrade.loadindices.LoadIndicesUpgradeConfig;
import com.linkedin.datahub.upgrade.shared.NoOpKafkaEventProducer;
import com.linkedin.datahub.upgrade.sqlsetup.SqlSetupUpgradeConfig;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entity.RetentionBufferFactory;
import com.linkedin.gms.factory.entity.RetentionBufferSchedulingConfig;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.event.EventProducer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * Boots the {@code -u Cleanup} context ({@link CleanupUpgradeConfig} via the configuration
 * selector) and pins what its component scan may and may not bring in.
 */
@ActiveProfiles({"upgrade", "test"})
@SpringBootTest(
    classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class},
    args = {"-u", "Cleanup"})
public class CleanupUpgradeConfigContextTest extends AbstractTestNGSpringContextTests {

  /**
   * The pgAnalytics runtime factories live here. Cleanup only tears down ES/Kafka/SQL and must not
   * open pgAnalytics pools, so nothing from this package may be registered.
   */
  private static final String PG_ANALYTICS_FACTORY_PACKAGE = "com.linkedin.gms.factory.analytics.";

  /** Kafka producer/consumer factories; Cleanup builds its own admin client from properties. */
  private static final String KAFKA_FACTORY_PACKAGE = "com.linkedin.gms.factory.kafka.";

  // No kafkaEventProducer override on purpose: the Cleanup context must supply its own (via
  // NoOpKafkaEventProducerConfig), because EntityServiceFactory depends on it by name.

  @Autowired private ConfigurableApplicationContext context;

  @Test
  public void testCleanupUpgradeIsRegistered() {
    assertTrue(context.containsBean("cleanup"));
    assertNotNull(context.getBean("cleanup", Cleanup.class));
    assertEquals(beanNamesForType(CleanupUpgradeConfig.class).size(), 1);
  }

  @Test
  public void testKafkaEventProducerIsNoOp() {
    EventProducer producer = context.getBean("kafkaEventProducer", EventProducer.class);
    assertTrue(
        producer instanceof NoOpKafkaEventProducer,
        "Cleanup must not publish to Kafka; got " + producer.getClass());
    EntityService<?> entityService = context.getBean("entityService", EntityService.class);
    assertSame(((EntityServiceImpl) entityService).getProducer(), producer);
  }

  @Test
  public void testScannedFactoriesAreLoaded() {
    // Guards the negative assertions below against passing on an empty or misconfigured context.
    assertFalse(beanNamesForType(ConfigurationProvider.class).isEmpty());
    assertFalse(
        beanNamesForType(BaseElasticSearchComponentsFactory.BaseElasticSearchComponents.class)
            .isEmpty());
  }

  @Test
  public void testOtherUpgradeConfigurationsAreNotLoaded() {
    assertTrue(beanNamesForType(GeneralUpgradeConfiguration.class).isEmpty());
    assertTrue(beanNamesForType(LoadIndicesUpgradeConfig.class).isEmpty());
    assertTrue(beanNamesForType(SqlSetupUpgradeConfig.class).isEmpty());
    for (String name :
        List.of(
            "loadIndices",
            "restoreIndices",
            "restoreBackup",
            "removeUnknownAspects",
            "systemUpdate",
            "systemUpdateBlocking",
            "systemUpdateNonBlocking")) {
      assertFalse(context.containsBean(name), "Unexpected upgrade bean in Cleanup: " + name);
    }
  }

  @Test
  public void testPgAnalyticsFactoriesAreNotLoaded() {
    List<String> offending =
        beanOrigins().stream()
            .filter(
                origin ->
                    origin.contains(PG_ANALYTICS_FACTORY_PACKAGE)
                        || origin.toLowerCase(Locale.ROOT).contains("pganalytics"))
            .collect(Collectors.toList());
    assertTrue(offending.isEmpty(), "Cleanup must not load pgAnalytics beans: " + offending);
  }

  @Test
  public void testKafkaFactoriesAreNotScanned() {
    List<String> offending =
        beanOrigins().stream()
            .filter(origin -> origin.contains(KAFKA_FACTORY_PACKAGE))
            .collect(Collectors.toList());
    assertTrue(offending.isEmpty(), "Cleanup must not scan Kafka factories: " + offending);
  }

  @Test
  public void testRetentionBufferIsExcluded() {
    assertTrue(beanNamesForType(RetentionBufferFactory.class).isEmpty());
    assertTrue(beanNamesForType(RetentionBufferSchedulingConfig.class).isEmpty());
  }

  private List<String> beanNamesForType(Class<?> type) {
    return List.of(context.getBeanFactory().getBeanNamesForType(type, true, false));
  }

  /**
   * One {@code "name -> class"} entry per bean definition, where class is the bean class or, for
   * {@code @Bean} methods, the declaring configuration class.
   */
  private List<String> beanOrigins() {
    ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
    List<String> origins = new ArrayList<>();
    for (String name : beanFactory.getBeanDefinitionNames()) {
      BeanDefinition definition = beanFactory.getBeanDefinition(name);
      String origin = definition.getBeanClassName();
      if (origin == null && definition.getFactoryBeanName() != null) {
        origin =
            beanFactory.getBeanDefinition(definition.getFactoryBeanName()).getBeanClassName()
                + "#"
                + definition.getFactoryMethodName();
      }
      origins.add(name + " -> " + origin);
    }
    return origins;
  }
}

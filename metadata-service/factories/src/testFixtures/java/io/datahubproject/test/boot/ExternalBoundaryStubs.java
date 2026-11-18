package io.datahubproject.test.boot;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.SearchClientShims;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.mockito.Mockito;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.core.Ordered;
import org.springframework.kafka.config.AbstractKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

/**
 * Replaces the few beans that open network connections to infrastructure a boot test does not run,
 * and nothing else.
 *
 * <ul>
 *   <li>{@code searchClientShims}: the Elasticsearch/OpenSearch client map. The production factory
 *       connects to each cluster to detect its engine. It becomes one Mockito client per configured
 *       cluster; the production {@code searchClientShim}, {@code searchClusterRegistry}, bulk
 *       processor and index builders are still built from it by their real factories.
 *   <li>{@code kafkaProducer} / {@code dataHubUsageProducer}: replaced by {@link MockProducer},
 *       only if the process defined them (so a Kafka-disabled boot never gains Kafka beans from
 *       here).
 *   <li>Each {@link DefaultKafkaConsumerFactory} that feeds listener containers keeps its
 *       production configuration but hands out {@link MockConsumer}s, so the containers start
 *       without a broker.
 * </ul>
 *
 * <p>Bean definitions are swapped after configuration-class parsing, so every condition
 * ({@code @Conditional}, {@code @ConditionalOnProperty}, ...) has already been evaluated against
 * the real production definitions. Internal services are never replaced.
 */
public class ExternalBoundaryStubs
    implements BeanDefinitionRegistryPostProcessor, BeanPostProcessor, Ordered {

  public static final String BEAN_NAME = "bootTestExternalBoundaryStubs";

  public static final String SEARCH_CLIENT_SHIMS = "searchClientShims";

  public static final String KAFKA_PRODUCER = "kafkaProducer";
  public static final String USAGE_PRODUCER = "dataHubUsageProducer";

  /**
   * Consumer factories whose consumers are cast to {@code KafkaConsumer} (the external-events
   * consumer pool). Left as-is: constructing a {@code KafkaConsumer} opens no connection, and the
   * pool only polls when an external-events request arrives.
   */
  private static final Set<String> CONCRETE_CONSUMER_FACTORIES =
      Set.of("kafkaConsumerPoolConsumerFactory");

  @Override
  public int getOrder() {
    // After ConfigurationClassPostProcessor (PriorityOrdered), so all scanned definitions exist.
    return Ordered.LOWEST_PRECEDENCE;
  }

  @Override
  public void postProcessBeanDefinitionRegistry(@Nonnull BeanDefinitionRegistry registry)
      throws BeansException {
    // Built lazily from the production search configuration, so every configured cluster (not
    // only primary) gets a client and multi-cluster routing is wired as in production.
    BeanFactory beanFactory = (BeanFactory) registry;
    replaceIfPresent(
        registry,
        SEARCH_CLIENT_SHIMS,
        SearchClientShims.class,
        () -> searchShims(beanFactory.getBean(ConfigurationProvider.class).getElasticSearch()));
    replaceIfPresent(registry, KAFKA_PRODUCER, Producer.class, ExternalBoundaryStubs::producer);
    replaceIfPresent(registry, USAGE_PRODUCER, Producer.class, ExternalBoundaryStubs::producer);
  }

  @Override
  public void postProcessBeanFactory(@Nonnull ConfigurableListableBeanFactory beanFactory)
      throws BeansException {}

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Object postProcessAfterInitialization(@Nonnull Object bean, @Nonnull String beanName)
      throws BeansException {
    if (bean instanceof DefaultKafkaConsumerFactory<?, ?> factory
        && !(bean instanceof BrokerlessConsumerFactory)
        && !CONCRETE_CONSUMER_FACTORIES.contains(beanName)) {
      return new BrokerlessConsumerFactory(factory.getConfigurationProperties());
    }
    // Container factories often build their own DefaultKafkaConsumerFactory inline (per-consumer
    // overrides such as auto.offset.reset), so swapping the shared consumer-factory beans is not
    // enough: swap whatever consumer factory each container factory ended up with.
    if (bean instanceof AbstractKafkaListenerContainerFactory<?, ?, ?> containerFactory
        && containerFactory.getConsumerFactory() != null
        && !(containerFactory.getConsumerFactory() instanceof BrokerlessConsumerFactory)) {
      ((AbstractKafkaListenerContainerFactory) containerFactory)
          .setConsumerFactory(
              new BrokerlessConsumerFactory<>(
                  containerFactory.getConsumerFactory().getConfigurationProperties()));
    }
    return bean;
  }

  private static <T> void replaceIfPresent(
      BeanDefinitionRegistry registry, String name, Class<T> type, Supplier<? extends T> stub) {
    if (!registry.containsBeanDefinition(name)) {
      return;
    }
    BeanDefinition original = registry.getBeanDefinition(name);
    RootBeanDefinition replacement = new RootBeanDefinition(type, stub::get);
    // Keep what injection points resolve against: generic type, primary/fallback, laziness.
    if (original.getResolvableType().resolve() != null) {
      replacement.setTargetType(original.getResolvableType());
    }
    replacement.setPrimary(original.isPrimary());
    replacement.setFallback(original.isFallback());
    replacement.setLazyInit(original.isLazyInit());
    replacement.setDependsOn(original.getDependsOn());
    replacement.setDescription("boot-test stub for external client " + name);
    registry.removeBeanDefinition(name);
    registry.registerBeanDefinition(name, replacement);
  }

  /**
   * One mock client per cluster that has a uri, the set {@code SearchClientShimFactory} builds
   * clients for. Unlike the factory it does not share one client between clusters on the same
   * endpoint, and it skips the factory's uri, IAM and TLS validation (no real connection is made).
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static SearchClientShims searchShims(ElasticSearchConfiguration esConfig) {
    Map<String, SearchClientShim<?>> shims = new LinkedHashMap<>();
    for (String cluster : configuredClusters(esConfig)) {
      SearchClientShim shim = Mockito.mock(SearchClientShim.class);
      Mockito.when(shim.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
      shims.put(cluster, shim);
    }
    return new SearchClientShims(shims);
  }

  /** Names of the clusters the production search configuration defines with a uri. */
  private static Set<String> configuredClusters(ElasticSearchConfiguration esConfig) {
    Set<String> names = new LinkedHashSet<>();
    esConfig
        .resolvedClusters()
        .forEach(
            (name, settings) -> {
              if (settings.isConfigured()) {
                names.add(name);
              }
            });
    return names;
  }

  /** Completes every send immediately; serializers ignore the payload. */
  private static <K, V> MockProducer<K, V> producer() {
    return new MockProducer<>(
        true, null, (topic, key) -> new byte[0], (topic, value) -> new byte[0]);
  }

  /** True when {@code bean} is one of the stand-ins this class hands out. */
  public static boolean isStub(Object bean) {
    if (bean instanceof MockProducer || bean instanceof BrokerlessConsumerFactory) {
      return true;
    }
    return bean instanceof SearchClientShims shims
        && !shims.byCluster().isEmpty()
        && shims.byCluster().values().stream()
            .allMatch(shim -> Mockito.mockingDetails(shim).isMock());
  }

  /** Production consumer configuration, but no broker connection. */
  static final class BrokerlessConsumerFactory<K, V> extends DefaultKafkaConsumerFactory<K, V> {
    BrokerlessConsumerFactory(Map<String, Object> configs) {
      super(configs);
    }

    @Override
    protected Consumer<K, V> createRawConsumer(Map<String, Object> configProps) {
      return new IdleMockConsumer<>();
    }
  }

  /**
   * {@link MockConsumer#poll} returns at once when nothing is assigned; listener threads would
   * spin. Wait out the poll timeout (capped) like a real consumer with no records would.
   */
  static final class IdleMockConsumer<K, V> extends MockConsumer<K, V> {
    IdleMockConsumer() {
      super("latest");
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
      // Not synchronized: sleep outside MockConsumer's lock so wakeup()/close() are not delayed.
      ConsumerRecords<K, V> records = super.poll(timeout);
      if (records.isEmpty()) {
        try {
          Thread.sleep(Math.min(timeout.toMillis(), 500L));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return records;
    }
  }
}

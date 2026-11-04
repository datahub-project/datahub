package io.datahubproject.test.boot;

import com.linkedin.gms.factory.search.SearchClientShims;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.mockito.Mockito;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

/**
 * Replaces the few beans that open network connections to infrastructure a boot test does not run,
 * and nothing else.
 *
 * <ul>
 *   <li>{@code searchClientShims}: the Elasticsearch/OpenSearch client map. The production factory
 *       connects to the cluster to detect its engine. It becomes a single Mockito client for the
 *       primary cluster; the production {@code searchClientShim}, {@code searchClusterRegistry},
 *       bulk processor and index builders are still built from it by their real factories.
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
    replaceIfPresent(
        registry, SEARCH_CLIENT_SHIMS, SearchClientShims.class, ExternalBoundaryStubs::searchShims);
    replaceIfPresent(registry, KAFKA_PRODUCER, Producer.class, MockProducer::new);
    replaceIfPresent(registry, USAGE_PRODUCER, Producer.class, MockProducer::new);
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
    return bean;
  }

  private static <T> void replaceIfPresent(
      BeanDefinitionRegistry registry, String name, Class<T> type, Supplier<? extends T> stub) {
    if (!registry.containsBeanDefinition(name)) {
      return;
    }
    BeanDefinition original = registry.getBeanDefinition(name);
    BeanDefinition replacement =
        BeanDefinitionBuilder.genericBeanDefinition(type, stub::get).getBeanDefinition();
    replacement.setPrimary(original.isPrimary());
    replacement.setDescription("boot-test stub for external client " + name);
    registry.removeBeanDefinition(name);
    registry.registerBeanDefinition(name, replacement);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static SearchClientShims searchShims() {
    SearchClientShim shim = Mockito.mock(SearchClientShim.class);
    Mockito.when(shim.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    return new SearchClientShims(Map.of(ElasticSearchConfiguration.PRIMARY_CLUSTER, shim));
  }

  /** Production consumer configuration, but no broker connection. */
  static final class BrokerlessConsumerFactory<K, V> extends DefaultKafkaConsumerFactory<K, V> {
    BrokerlessConsumerFactory(Map<String, Object> configs) {
      super(configs);
    }

    @Override
    protected Consumer<K, V> createRawConsumer(Map<String, Object> configProps) {
      return new MockConsumer<>("latest");
    }
  }
}

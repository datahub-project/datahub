package io.datahubproject.test.boot;

import com.linkedin.metadata.config.messaging.MessagingTransport;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;

/**
 * Starts a DataHub process the way its {@code main} does: same primary sources, same profiles, the
 * production {@code application.yaml}, auto-configuration on. The only differences are
 *
 * <ul>
 *   <li>the entity store (and the pgQueue pool) is an in-memory H2 database built by the production
 *       Ebean factories, with DDL generation on;
 *   <li>{@link ExternalBoundaryStubs} swaps the Elasticsearch/OpenSearch and Kafka clients;
 *   <li>Kafka bootstrap servers point at a closed loopback port;
 *   <li>HTTP ports are random.
 * </ul>
 *
 * <p>What is checked is the wiring, not runtime behavior. Background workers that start after the
 * context is up are allowed to fail: e.g. pgQueue poll workers log {@code Schema "QUEUE" not
 * found}, because the queue schema is created by system-update's SqlSetup, not by Ebean DDL.
 */
public final class ProcessBoot {

  /** Metadata messaging transport, i.e. the {@code kafka} vs no-Kafka deployment profiles. */
  public enum Messaging {
    KAFKA,
    PGQUEUE
  }

  /** Port 1 on loopback: connection refused immediately, nothing is ever reached. */
  private static final String UNREACHABLE_BROKER = "127.0.0.1:1";

  private final List<Class<?>> sources = new ArrayList<>();
  private final List<String> profiles = new ArrayList<>();
  private final Map<String, Object> properties = new LinkedHashMap<>();
  private WebApplicationType webApplicationType = WebApplicationType.NONE;
  private String[] args = new String[0];
  private Messaging messaging = Messaging.KAFKA;

  private ProcessBoot(Class<?>... primarySources) {
    sources.addAll(List.of(primarySources));
  }

  public static ProcessBoot of(@Nonnull Class<?>... primarySources) {
    return new ProcessBoot(primarySources);
  }

  public ProcessBoot profiles(@Nonnull String... activeProfiles) {
    profiles.addAll(List.of(activeProfiles));
    return this;
  }

  public ProcessBoot web(@Nonnull WebApplicationType type) {
    this.webApplicationType = type;
    return this;
  }

  public ProcessBoot args(@Nonnull String... commandLineArgs) {
    this.args = commandLineArgs;
    return this;
  }

  public ProcessBoot messaging(@Nonnull Messaging transport) {
    this.messaging = transport;
    return this;
  }

  public ProcessBoot property(@Nonnull String key, @Nonnull Object value) {
    properties.put(key, value);
    return this;
  }

  @Nonnull
  public ConfigurableApplicationContext start() {
    String db = "boot_" + UUID.randomUUID().toString().replace("-", "");
    // No DB_CLOSE_DELAY: the database lives while the context's pools hold connections and is
    // dropped when the context closes.
    String h2Url = "jdbc:h2:mem:" + db + ";IGNORECASE=TRUE;MODE=MySQL";

    Map<String, Object> props = new LinkedHashMap<>();
    props.put("spring.main.banner-mode", "off");
    props.put("server.port", "0");
    props.put("management.server.port", "0");

    props.put("ebean.url", h2Url);
    props.put("ebean.driver", "org.h2.Driver");
    props.put("ebean.username", "sa");
    props.put("ebean.password", "");
    props.put("ebean.autoCreateDdl", "true");
    props.put("postgres.pgQueue.pool.url", h2Url);
    props.put("postgres.pgQueue.pool.driver", "org.h2.Driver");
    props.put("postgres.pgQueue.pool.username", "sa");
    props.put("postgres.pgQueue.pool.password", "");

    // Production requires these to be set by the deployment; any value boots.
    props.put("authentication.tokenService.signingKey", "boot-test-signing-key");
    props.put("authentication.tokenService.salt", "boot-test-salt");

    // Safety net for any Kafka client the stubs do not cover (admin clients, producers built inside
    // services): point it at a closed port, never at a developer's local quickstart broker.
    props.put("kafka.bootstrapServers", UNREACHABLE_BROKER);
    props.put("spring.kafka.bootstrap-servers", UNREACHABLE_BROKER);
    // No broker exists. Kafka clients that are not ours to stub (admin clients, ad-hoc producers
    // built inside services) must fail fast instead of blocking a context refresh for 60s.
    props.put("spring.kafka.producer.properties.max.block.ms", "1000");
    props.put("spring.kafka.properties.request.timeout.ms", "1000");
    props.put("spring.kafka.properties.default.api.timeout.ms", "2000");
    props.put("spring.kafka.properties.socket.connection.setup.timeout.ms", "500");
    props.put("spring.kafka.properties.socket.connection.setup.timeout.max.ms", "1000");
    props.put("spring.kafka.properties.reconnect.backoff.max.ms", "1000");

    if (messaging == Messaging.PGQUEUE) {
      props.put(MessagingTransport.PROPERTY, MessagingTransport.PGQUEUE);
      // Deployments that select pgqueue always enable the SqlSetup-managed queue catalog.
      props.put("postgres.pgQueue.enabled", "true");
    } else {
      props.put(MessagingTransport.PROPERTY, MessagingTransport.KAFKA);
    }
    props.putAll(properties);

    SpringApplicationBuilder builder =
        new SpringApplicationBuilder(sources.toArray(new Class<?>[0]))
            .web(webApplicationType)
            .initializers(
                ctx -> {
                  // Highest precedence: application.yaml must not win over the test datastore.
                  ctx.getEnvironment()
                      .getPropertySources()
                      .addFirst(new MapPropertySource("processBootTest", props));
                  if (!(ctx.getBeanFactory() instanceof BeanDefinitionRegistry registry)) {
                    // Booting without the stubs would connect to real infrastructure.
                    throw new IllegalStateException(
                        "Cannot register boot-test stubs on " + ctx.getBeanFactory());
                  }
                  registry.registerBeanDefinition(
                      ExternalBoundaryStubs.BEAN_NAME,
                      new RootBeanDefinition(ExternalBoundaryStubs.class));
                });
    if (!profiles.isEmpty()) {
      builder.profiles(profiles.toArray(new String[0]));
    }
    return builder.run(args);
  }
}

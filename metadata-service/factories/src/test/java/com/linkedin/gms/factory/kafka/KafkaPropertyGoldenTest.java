package com.linkedin.gms.factory.kafka;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.common.AdminClientFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.AwsGlueSchemaRegistryFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.InternalSchemaRegistryFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.KafkaSchemaRegistryFactory;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.SpringFactoriesLoader;
import org.springframework.kafka.config.AbstractKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Golden tests for the Kafka producer, consumer, admin client and schema-registry properties that
 * the production factories build from the production {@code application.yaml}.
 *
 * <p>Each scenario starts a Spring context over {@code application.yaml} from {@code
 * metadata-service/configuration}, with environment variables applied through a {@link
 * SystemEnvironmentPropertySource} exactly as a deployment's container env would be. The real
 * factory beans are then resolved; Kafka client construction is intercepted so no broker is
 * contacted. The full resulting property maps are compared against checked-in golden files under
 * {@code src/test/resources/kafka-property-golden/}.
 *
 * <p>Any renamed, dropped, added or changed property fails with a per-key diff. If a change is
 * intentional, regenerate the goldens with:
 *
 * <pre>
 * UPDATE_KAFKA_GOLDEN=true ./gradlew :metadata-service:factories:test --tests '*KafkaPropertyGoldenTest'
 * </pre>
 *
 * and review the resulting diff of the golden files.
 */
public class KafkaPropertyGoldenTest {

  private static final String GOLDEN_DIR = "kafka-property-golden";
  private static final String UPDATE_ENV = "UPDATE_KAFKA_GOLDEN";
  private static final Path GOLDEN_SOURCE_DIR = Paths.get("src", "test", "resources", GOLDEN_DIR);

  /** Admin client id used by the GMS trace reader; other callers only differ in client.id. */
  private static final String ADMIN_CLIENT_ID = "trace-reader";

  /** Placeholder secret values: these are fake and only exist to show where secrets flow. */
  private static final String FAKE_SECRET = "golden-test-secret";

  @DataProvider(name = "scenarios")
  public Object[][] scenarios() {
    return new Object[][] {
      // application.yaml defaults with no env overrides: PLAINTEXT, external Confluent registry.
      {"plaintext-defaults", Map.of()},
      // PLAINTEXT with the documented tuning knobs and split producer/consumer overrides.
      {
        "plaintext-overrides",
        envOf(
            "KAFKA_BOOTSTRAP_SERVER", "broker-1:9092,broker-2:9092",
            "KAFKA_SCHEMAREGISTRY_URL", "http://schema-registry:8081",
            "KAFKA_PRODUCER_BOOTSTRAP_SERVER", "producer-broker:9092",
            "KAFKA_PRODUCER_SECURITY_PROTOCOL", "SSL",
            "KAFKA_PRODUCER_SCHEMAREGISTRY_URL", "http://producer-registry:8081",
            "KAFKA_PRODUCER_RETRY_COUNT", "7",
            "KAFKA_PRODUCER_DELIVERY_TIMEOUT", "45000",
            "KAFKA_PRODUCER_REQUEST_TIMEOUT", "4000",
            "KAFKA_PRODUCER_BACKOFF_TIMEOUT", "750",
            "KAFKA_PRODUCER_COMPRESSION_TYPE", "zstd",
            "KAFKA_PRODUCER_MAX_REQUEST_SIZE", "10485760",
            "KAFKA_CONSUMER_BOOTSTRAP_SERVER", "consumer-broker:9092",
            "KAFKA_CONSUMER_SECURITY_PROTOCOL", "SASL_PLAINTEXT",
            "KAFKA_CONSUMER_SCHEMAREGISTRY_URL", "http://consumer-registry:8081",
            "KAFKA_CONSUMER_MAX_PARTITION_FETCH_BYTES", "10485760",
            "KAFKA_CONSUMER_MCP_AUTO_OFFSET_RESET", "latest",
            "KAFKA_CONSUMER_MCL_AUTO_OFFSET_RESET", "latest",
            "KAFKA_CONSUMER_PE_AUTO_OFFSET_RESET", "earliest")
      },
      // SASL/SSL (e.g. Confluent Cloud / self-managed SASL_SSL with SCRAM and a truststore).
      {
        "sasl-ssl",
        envOf(
            "KAFKA_BOOTSTRAP_SERVER", "broker.example.com:9093",
            "KAFKA_PROPERTIES_SECURITY_PROTOCOL", "SASL_SSL",
            "SPRING_KAFKA_PROPERTIES_SASL_MECHANISM", "SCRAM-SHA-512",
            "SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG",
                "org.apache.kafka.common.security.scram.ScramLoginModule required"
                    + " username=\"datahub\" password=\""
                    + FAKE_SECRET
                    + "\";",
            "SPRING_KAFKA_PROPERTIES_SSL_TRUSTSTORE_LOCATION", "/mnt/certs/truststore.jks",
            "SPRING_KAFKA_PROPERTIES_SSL_TRUSTSTORE_PASSWORD", FAKE_SECRET,
            "SPRING_KAFKA_PROPERTIES_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "https")
      },
      // AWS MSK IAM, configured the way the docs and Helm chart do it.
      {
        "aws-msk-iam",
        envOf(
            "KAFKA_BOOTSTRAP_SERVER", "b-1.msk.example.amazonaws.com:9098",
            "SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL", "SASL_SSL",
            "SPRING_KAFKA_PROPERTIES_SASL_MECHANISM", "AWS_MSK_IAM",
            "SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG",
                "software.amazon.msk.auth.iam.IAMLoginModule required;",
            "SPRING_KAFKA_PROPERTIES_SASL_CLIENT_CALLBACK_HANDLER_CLASS",
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
      },
      // External Confluent schema registry with basic auth and mTLS to the registry.
      {
        "schema-registry-basic-auth",
        envOf(
            "SCHEMA_REGISTRY_TYPE", "KAFKA",
            "KAFKA_SCHEMAREGISTRY_URL", "https://registry.example.com",
            "SPRING_KAFKA_PROPERTIES_BASIC_AUTH_CREDENTIALS_SOURCE", "USER_INFO",
            "SPRING_KAFKA_PROPERTIES_BASIC_AUTH_USER_INFO", "registry-user:" + FAKE_SECRET,
            "KAFKA_SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION", "/mnt/certs/registry-truststore.p12",
            "KAFKA_SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD", FAKE_SECRET,
            "KAFKA_SCHEMA_REGISTRY_SSL_TRUSTSTORE_TYPE", "PKCS12",
            "KAFKA_SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION", "/mnt/certs/registry-keystore.p12",
            "KAFKA_SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD", FAKE_SECRET,
            "KAFKA_SCHEMA_REGISTRY_SSL_KEYSTORE_TYPE", "PKCS12",
            "KAFKA_SCHEMA_REGISTRY_SECURITY_PROTOCOL", "SSL")
      },
      // AWS Glue schema registry.
      {
        "schema-registry-aws-glue",
        envOf(
            "SCHEMA_REGISTRY_TYPE", "AWS_GLUE",
            "AWS_GLUE_SCHEMA_REGISTRY_REGION", "eu-west-1",
            "AWS_GLUE_SCHEMA_REGISTRY_NAME", "datahub-registry")
      },
      // GMS-internal schema registry.
      {
        "schema-registry-internal",
        envOf(
            "SCHEMA_REGISTRY_TYPE", "INTERNAL",
            "KAFKA_SCHEMAREGISTRY_URL", "http://datahub-gms:8080/schema-registry/api/")
      },
    };
  }

  @Test(dataProvider = "scenarios")
  public void testKafkaPropertiesMatchGolden(String scenario, Map<String, Object> env)
      throws IOException {
    Map<String, Map<String, String>> actual = buildAllClientProperties(env);
    String rendered = render(scenario, env, actual);

    String resource = GOLDEN_DIR + "/" + scenario + ".golden";
    String expected = readResource(resource);
    if (Boolean.parseBoolean(System.getenv(UPDATE_ENV)) && !rendered.equals(expected)) {
      // Gradle runs tests from the module dir; refuse to write anywhere else (e.g. an IDE run
      // from the repo root) rather than silently creating a stray copy.
      if (!Files.isDirectory(GOLDEN_SOURCE_DIR.getParent())) {
        fail(
            "Run from metadata-service/factories to update goldens; cwd="
                + Paths.get("").toAbsolutePath());
      }
      Path target = GOLDEN_SOURCE_DIR.resolve(scenario + ".golden");
      Files.createDirectories(target.getParent());
      Files.writeString(target, rendered, StandardCharsets.UTF_8);
      // Fail so an update run never passes silently; rerun without the flag after reviewing.
      fail("Updated golden " + target + "; review the diff and rerun without " + UPDATE_ENV);
    }
    if (expected == null) {
      fail(
          "Missing golden file "
              + resource
              + ". Generate it with "
              + UPDATE_ENV
              + "=true and review the result.");
    }
    List<String> diff = diff(parse(expected), parse(rendered));
    if (!diff.isEmpty()) {
      fail(
          "Kafka/schema-registry properties for scenario '"
              + scenario
              + "' differ from "
              + resource
              + " ("
              + diff.size()
              + " difference(s)):\n  "
              + String.join("\n  ", diff)
              + "\nIf this change is intentional, regenerate with "
              + UPDATE_ENV
              + "=true and review the golden diff.");
    }
    // Also catches drift in the header/env comments and line ordering, which parse() ignores.
    assertEquals(rendered, expected, "Golden file " + resource + " is not normalized");
  }

  /** Sanity check that the diff produces readable output for renamed, dropped and added keys. */
  @Test
  public void testDiffReportsRenamedDroppedAndAddedKeys() {
    Map<String, String> expected = new TreeMap<>();
    expected.put("producer.kafkaProducer|retries", "3 (Integer)");
    expected.put("producer.kafkaProducer|max.request.size", "5242880 (Integer)");
    expected.put("producer.kafkaProducer|compression.type", "snappy");
    Map<String, String> actual = new TreeMap<>();
    actual.put("producer.kafkaProducer|retries", "3 (Integer)");
    actual.put("producer.kafkaProducer|max.request.bytes", "5242880 (Integer)");
    actual.put("producer.kafkaProducer|compression.type", "zstd");
    actual.put("producer.kafkaProducer|linger.ms", "5 (Integer)");

    assertEquals(
        diff(expected, actual),
        List.of(
            "~ [producer.kafkaProducer] compression.type: expected <snappy> but was <zstd>",
            "+ [producer.kafkaProducer] linger.ms=5 (Integer)  (unexpected property)",
            "+ [producer.kafkaProducer] max.request.bytes=5242880 (Integer)  (unexpected property)",
            "- [producer.kafkaProducer] max.request.size=5242880 (Integer)  (missing property)"));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Map<String, String>> buildAllClientProperties(
      Map<String, Object> env) {
    Map<String, Map<String, String>> sections = new LinkedHashMap<>();
    try (AnnotationConfigApplicationContext ctx = newContext(env)) {
      KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig =
          ctx.getBean("schemaRegistryConfig", KafkaConfiguration.SerDeKeyValueConfig.class);
      sections.put("schemaRegistry.schemaRegistryConfig", flattenSerDe(schemaRegistryConfig));

      // Producers: intercept KafkaProducer construction so nothing connects to a broker. Beans are
      // lazy, so each getBean constructs exactly one producer and the capture is tied to its bean.
      // Request order matters: kafkaProducer mutates the shared KafkaProperties that
      // dataHubUsageProducer then reads; this order is the one the goldens pin.
      List<Map<String, Object>> producerConfigs = new ArrayList<>();
      try (var ignored =
          mockConstruction(
              KafkaProducer.class,
              (mockProducer, context) ->
                  producerConfigs.add(
                      new LinkedHashMap<>((Map<String, Object>) context.arguments().get(0))))) {
        for (String producerBean : List.of("kafkaProducer", "dataHubUsageProducer")) {
          int before = producerConfigs.size();
          ctx.getBean(producerBean);
          assertEquals(
              producerConfigs.size(),
              before + 1,
              "expected " + producerBean + " to construct exactly one KafkaProducer");
          sections.put(
              "producer." + producerBean,
              stringify(producerConfigs.get(producerConfigs.size() - 1)));
        }
      }

      // Consumers: DefaultKafkaConsumerFactory only holds the config; it never connects.
      Map<String, Object> consumer =
          ctx.getBean("kafkaConsumerFactory", DefaultKafkaConsumerFactory.class)
              .getConfigurationProperties();
      sections.put("consumer.kafkaConsumerFactory", stringify(consumer));
      sections.put(
          "consumer.kafkaConsumerPoolConsumerFactory",
          stringify(
              ctx.getBean("kafkaConsumerPoolConsumerFactory", DefaultKafkaConsumerFactory.class)
                  .getConfigurationProperties()));
      for (String listener :
          List.of(
              KafkaConfiguration.MCP_EVENT_CONSUMER_NAME,
              KafkaConfiguration.MCL_EVENT_CONSUMER_NAME,
              KafkaConfiguration.PE_EVENT_CONSUMER_NAME,
              KafkaConfiguration.MCP_BATCH_EVENT_CONSUMER_NAME,
              KafkaConfiguration.MCL_BATCH_EVENT_CONSUMER_NAME,
              KafkaConfiguration.DEFAULT_EVENT_CONSUMER_NAME)) {
        AbstractKafkaListenerContainerFactory<?, ?, ?> containerFactory =
            ctx.getBean(listener, AbstractKafkaListenerContainerFactory.class);
        Map<String, Object> listenerProps =
            containerFactory.getConsumerFactory().getConfigurationProperties();
        // Only record what each listener changes relative to the shared consumer factory.
        sections.put(
            "consumer." + listener + " (delta vs kafkaConsumerFactory)",
            delta(stringify(consumer), stringify(listenerProps)));
      }

      // Admin client: intercept AdminClient.create so nothing connects to a broker.
      ConfigurationProvider provider = ctx.getBean(ConfigurationProvider.class);
      KafkaProperties kafkaProperties = ctx.getBean(KafkaProperties.class);
      List<Map<String, Object>> adminConfigs = new ArrayList<>();
      try (var adminStatic = mockStatic(AdminClient.class)) {
        adminStatic
            .when(() -> AdminClient.create(anyMap()))
            .thenAnswer(
                invocation -> {
                  adminConfigs.add(new LinkedHashMap<>(invocation.getArgument(0)));
                  return mock(AdminClient.class);
                });
        AdminClientFactory.buildKafkaAdminClient(
            provider.getKafka(), kafkaProperties, ADMIN_CLIENT_ID);
      }
      assertEquals(adminConfigs.size(), 1, "expected exactly one AdminClient creation");
      sections.put("admin." + ADMIN_CLIENT_ID, stringify(adminConfigs.get(0)));
    }
    return sections;
  }

  private static AnnotationConfigApplicationContext newContext(Map<String, Object> env) {
    AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
    StandardEnvironment environment = new StandardEnvironment();
    MutablePropertySources sources = environment.getPropertySources();
    // Hermetic: the only env/system inputs are the scenario's. The scenario env is exposed as a
    // real system-environment source so Spring's env-var name mapping applies, as in deployments.
    sources.remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
    sources.replace(
        StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
        new SystemEnvironmentPropertySource(
            StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME, env));
    for (PropertySource<?> yaml : loadProductionApplicationYaml()) {
      sources.addLast(yaml);
    }
    ConfigurationPropertySources.attach(environment);
    ctx.setEnvironment(environment);

    applyTestInitializers(ctx);
    // Lazy beans so only what we ask for (and its dependencies) gets built.
    ctx.addBeanFactoryPostProcessor(lazyInit());
    ctx.registerBean(
        PropertySourcesPlaceholderConfigurer.class, PropertySourcesPlaceholderConfigurer::new);
    ctx.register(
        ConfigurationProvider.class,
        KafkaSchemaRegistryFactory.class,
        InternalSchemaRegistryFactory.class,
        AwsGlueSchemaRegistryFactory.class,
        DataHubKafkaProducerFactory.class,
        KafkaEventConsumerFactory.class);
    ctx.refresh();
    return ctx;
  }

  /**
   * Applies the repo's own test-side {@code ApplicationContextInitializer}s from {@code
   * META-INF/spring.factories}, as {@code SpringApplication} would, so a test-classpath recorder
   * registered that way (the Spring wiring-coverage ratchet) also sees this hand-built context. A
   * no-op when none is registered. Third-party initializers are skipped to keep the context
   * minimal.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void applyTestInitializers(AnnotationConfigApplicationContext ctx) {
    for (ApplicationContextInitializer initializer :
        SpringFactoriesLoader.forDefaultResourceLocation()
            .load(ApplicationContextInitializer.class)) {
      if (initializer.getClass().getName().startsWith("io.datahubproject.")) {
        initializer.initialize(ctx);
      }
    }
  }

  private static BeanFactoryPostProcessor lazyInit() {
    return beanFactory -> {
      for (String name : beanFactory.getBeanDefinitionNames()) {
        BeanDefinition definition = beanFactory.getBeanDefinition(name);
        if (definition.getRole() == BeanDefinition.ROLE_APPLICATION) {
          definition.setLazyInit(true);
        }
      }
    };
  }

  private static List<PropertySource<?>> loadProductionApplicationYaml() {
    try {
      // Several jars on the test classpath (e.g. the shaded auth-api jar) carry their own copy of
      // application.yaml. Bind the one from metadata-service/configuration explicitly.
      List<URL> candidates =
          Collections.list(
              KafkaPropertyGoldenTest.class.getClassLoader().getResources("application.yaml"));
      URL url =
          candidates.stream()
              .filter(
                  u ->
                      u.toString().contains("metadata-service/configuration/")
                          || u.toString()
                              .matches(".*/configuration-[^/]*\\.jar!/application\\.yaml"))
              .findFirst()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "application.yaml from metadata-service/configuration not found among "
                              + candidates));
      return new YamlPropertySourceLoader().load("application.yaml", new UrlResource(url));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Map<String, String> flattenSerDe(KafkaConfiguration.SerDeKeyValueConfig config) {
    Map<String, Object> flat = new LinkedHashMap<>();
    flat.put("key.serializer", config.getKey().getSerializer());
    flat.put("key.deserializer", config.getKey().getDeserializer());
    flat.put("key.delegateDeserializer", config.getKey().getDelegateDeserializer());
    flat.put("value.serializer", config.getValue().getSerializer());
    flat.put("value.deserializer", config.getValue().getDeserializer());
    flat.put("value.delegateDeserializer", config.getValue().getDelegateDeserializer());
    config.getProperties(null).forEach((k, v) -> flat.put("properties." + k, v));
    return stringify(flat);
  }

  private static Map<String, String> stringify(Map<String, ?> props) {
    Map<String, String> result = new TreeMap<>();
    props.forEach((k, v) -> result.put(k, renderValue(v)));
    return result;
  }

  private static String renderValue(Object value) {
    if (value == null) {
      return "<null>";
    }
    if (value instanceof String s) {
      return s.replace("\\", "\\\\").replace("\n", "\\n");
    }
    if (value instanceof Class<?> c) {
      return c.getName() + " (Class)";
    }
    if (value instanceof Collection<?> c) {
      List<String> items = new ArrayList<>();
      c.forEach(item -> items.add(renderValue(item)));
      return items + " (List)";
    }
    return value + " (" + value.getClass().getSimpleName() + ")";
  }

  private static Map<String, String> delta(Map<String, String> base, Map<String, String> other) {
    Map<String, String> result = new TreeMap<>();
    other.forEach(
        (k, v) -> {
          if (!Objects.equals(base.get(k), v)) {
            result.put(k, v);
          }
        });
    base.keySet().stream()
        .filter(k -> !other.containsKey(k))
        .forEach(k -> result.put(k, "<removed>"));
    return result;
  }

  private static String render(
      String scenario, Map<String, Object> env, Map<String, Map<String, String>> sections) {
    StringBuilder sb = new StringBuilder();
    sb.append("# Golden Kafka/schema-registry client properties for scenario: ")
        .append(scenario)
        .append('\n');
    sb.append("# Generated by KafkaPropertyGoldenTest. Do not edit by hand; regenerate with\n");
    sb.append("# ").append(UPDATE_ENV).append("=true and review the diff.\n");
    sb.append("# Environment overrides on top of application.yaml:\n");
    if (env.isEmpty()) {
      sb.append("#   (none)\n");
    }
    new TreeMap<>(env)
        .forEach((k, v) -> sb.append("#   ").append(k).append('=').append(v).append('\n'));
    sections.forEach(
        (section, props) -> {
          sb.append('\n').append('[').append(section).append("]\n");
          props.forEach((k, v) -> sb.append(k).append('=').append(v).append('\n'));
        });
    return sb.toString();
  }

  /** Parses a rendered golden into {@code "section|key" -> value}. */
  private static Map<String, String> parse(String golden) {
    Map<String, String> result = new TreeMap<>();
    String section = "";
    for (String line : golden.split("\n", -1)) {
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      if (line.startsWith("[") && line.endsWith("]")) {
        section = line.substring(1, line.length() - 1);
        // Keep empty sections visible so a section disappearing is a diff too.
        result.put(section + "|", "<section>");
        continue;
      }
      int eq = line.indexOf('=');
      if (eq < 0) {
        result.put(section + "|" + line, "<malformed line>");
      } else {
        result.put(section + "|" + line.substring(0, eq), line.substring(eq + 1));
      }
    }
    return result;
  }

  static List<String> diff(Map<String, String> expected, Map<String, String> actual) {
    List<String> out = new ArrayList<>();
    TreeSet<String> keys = new TreeSet<>(expected.keySet());
    keys.addAll(actual.keySet());
    for (String key : keys) {
      int bar = key.indexOf('|');
      String label = "[" + key.substring(0, bar) + "] " + key.substring(bar + 1);
      boolean inExpected = expected.containsKey(key);
      boolean inActual = actual.containsKey(key);
      if (inExpected && !inActual) {
        out.add("- " + label + "=" + expected.get(key) + "  (missing property)");
      } else if (!inExpected && inActual) {
        out.add("+ " + label + "=" + actual.get(key) + "  (unexpected property)");
      } else if (!Objects.equals(expected.get(key), actual.get(key))) {
        out.add(
            "~ "
                + label
                + ": expected <"
                + expected.get(key)
                + "> but was <"
                + actual.get(key)
                + ">");
      }
    }
    return out;
  }

  private static String readResource(String resource) throws IOException {
    try (InputStream in =
        KafkaPropertyGoldenTest.class.getClassLoader().getResourceAsStream(resource)) {
      return in == null ? null : new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private static Map<String, Object> envOf(String... keyValues) {
    Map<String, Object> env = new LinkedHashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      env.put(keyValues[i], keyValues[i + 1]);
    }
    return env;
  }
}

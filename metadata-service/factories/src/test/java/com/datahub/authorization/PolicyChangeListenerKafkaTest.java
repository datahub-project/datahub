package com.datahub.authorization;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.group.GroupService;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.gms.factory.auth.PolicyChangeListener;
import com.linkedin.gms.factory.auth.PolicyChangeListenerFactory;
import com.linkedin.policy.DataHubPolicyInfo;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

// In this package to read the authorizer's package-private snapshot.
/**
 * Two processes with a policy cache, one real Kafka broker. A policy change committed by one
 * process reaches the other process's cache through the policy change topic.
 */
public class PolicyChangeListenerKafkaTest {

  private static final String TOPIC = "PolicyChange_v1";
  private static final PolicyChangeListener.Settings SETTINGS =
      new PolicyChangeListener.Settings(
          Duration.ofMillis(200),
          Duration.ofMillis(500),
          Duration.ofMinutes(5),
          Duration.ofSeconds(30));

  private ConfluentKafkaContainer kafka;
  private KafkaProducer<byte[], byte[]> producer;

  /** The shared database: both authorizers load the same rows. */
  private final List<PolicyFetcher.Policy> database = new CopyOnWriteArrayList<>();

  private final PolicyLoader loader = () -> PolicyLoader.Result.of(new ArrayList<>(database));
  private final List<PolicyChangeListener> listeners = new ArrayList<>();

  @BeforeClass
  public void startKafka() throws Exception {
    kafka =
        new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:8.2.2"))
            .withStartupAttempts(5);
    kafka.start();
    try (Admin admin =
        Admin.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      admin
          .createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1)))
          .all()
          .get(30, TimeUnit.SECONDS);
    }
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producer = new KafkaProducer<>(props);
  }

  @AfterClass(alwaysRun = true)
  public void stopKafka() {
    listeners.forEach(PolicyChangeListener::close);
    if (producer != null) {
      producer.close();
    }
    if (kafka != null) {
      kafka.stop();
    }
  }

  private DataHubAuthorizer startProcess() {
    DataHubAuthorizer authorizer =
        new DataHubAuthorizer(
            TestOperationContexts.systemContextNoSearchAuthorization(),
            mock(EntityClient.class),
            mock(GroupService.class),
            10,
            0, // no refresh timer: only the listener can bring changes in
            DataHubAuthorizer.AuthorizationMode.DEFAULT,
            1000,
            loader);
    authorizer.initialLoad(1, 0);
    PolicyChangeListener listener =
        new PolicyChangeListener(
            new KafkaConsumer<>(
                PolicyChangeListenerFactory.listenerProperties(
                    Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))),
            TOPIC,
            authorizer::requestLoad,
            null,
            SETTINGS,
            Clock.systemUTC());
    listeners.add(listener);
    listener.start();
    return authorizer;
  }

  private static PolicyFetcher.Policy policy(String id) {
    return new PolicyFetcher.Policy(
        UrnUtils.getUrn("urn:li:dataHubPolicy:" + id),
        new DataHubPolicyInfo()
            .setDisplayName(id)
            .setType("METADATA")
            .setState("ACTIVE")
            .setPrivileges(new StringArray(List.of("EDIT_ENTITY"))));
  }

  /** What the writing process does: commit, reload its own cache, then send the change. */
  private void commitPolicyChange(DataHubAuthorizer writer, String id) throws Exception {
    database.add(policy(id));
    writer.onPolicyChange();
    producer
        .send(new ProducerRecord<>(TOPIC, id.getBytes(), new byte[] {1}))
        .get(10, TimeUnit.SECONDS);
  }

  private static boolean hasPolicy(DataHubAuthorizer authorizer, String id) {
    return authorizer.currentSnapshot().getOrDefault(DataHubAuthorizer.ALL, List.of()).stream()
        .anyMatch(info -> id.equals(info.getDisplayName()));
  }

  private static void await(BooleanSupplier condition, String message) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(50);
    }
    throw new AssertionError(message);
  }

  @Test
  public void testAChangeOnOneProcessReachesTheOther() throws Exception {
    DataHubAuthorizer writer = startProcess();
    DataHubAuthorizer reader = startProcess();
    // Not needed for correctness: a listener that is still starting gets the change from its start
    // load. The wait makes the reader get it through a poll, which is the path under test.
    Thread.sleep(2_000);

    commitPolicyChange(writer, "broadcast-1");
    assertTrue(hasPolicy(writer, "broadcast-1"), "the writer has the change when it returns");

    await(() -> hasPolicy(reader, "broadcast-1"), "the other process never got the change");

    // A process that starts after the change still has it, from its start load.
    DataHubAuthorizer late = startProcess();
    assertTrue(hasPolicy(late, "broadcast-1"));
    assertEquals(
        late.currentSnapshot().getOrDefault(DataHubAuthorizer.ALL, List.of()).size(),
        reader.currentSnapshot().getOrDefault(DataHubAuthorizer.ALL, List.of()).size());
  }
}

package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end guard for the symptom reported in issue #19005: a structured-streaming read from Kafka
 * emitted a DataJob with <b>zero</b> Kafka input edges, because the shaded agent's {@code
 * TopicPartitionProxy} rejected every {@code TopicPartition} handed to it by Spark's
 * spark-sql-kafka-0-10 connector and {@code PlanUtils.safeApply} swallowed the failure at INFO.
 *
 * <p>Unlike {@link ShadedReflectionClassNameTest}, which asserts on the proxy in isolation, this
 * drives the whole path a user exercises — real broker, real micro-batch, real {@code
 * KafkaMicroBatchStreamStrategy} selection — and asserts a Kafka input dataset actually reaches the
 * emitter. It runs against the shaded {@code shadowJar_2_13}, the artifact users deploy.
 *
 * <p>Requires Docker; skipped automatically when unavailable.
 */
@Testcontainers(disabledWithoutDocker = true)
public class KafkaStreamingLineageTest {

  private static final String TOPIC = "lineage_topic";

  /** The topic must land as a kafka-platform dataset URN; the env segment is left open. */
  private static final Pattern KAFKA_INPUT_URN =
      Pattern.compile(
          "urn:li:dataset:\\(urn:li:dataPlatform:kafka," + Pattern.quote(TOPIC) + ",[A-Z]+\\)");

  /** Accepts both the plain-URN list and the edge-object form of the aspect. */
  private static final Pattern INPUT_DATASETS =
      Pattern.compile("\"(inputDatasets|inputDatasetEdges)\"\\s*:\\s*\\[(.*?)\\]", Pattern.DOTALL);

  @Container
  private static final ConfluentKafkaContainer KAFKA =
      new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:8.2.2"));

  @Test
  public void kafkaStreamingReadEmitsKafkaInputDataset(@TempDir Path tmp) throws Exception {
    produce("alpha", "beta", "gamma");

    Path out = tmp.resolve("out");
    Path checkpoint = tmp.resolve("checkpoint");
    Path mcps = tmp.resolve("mcps.json");

    SparkSession spark =
        SparkSession.builder()
            .appName("kafka-streaming-lineage")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            // Bind to loopback so stray LAN hosts can't reach Spark's internal ports.
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.extraListeners", "datahub.spark.DatahubSparkListener")
            .config("spark.datahub.emitter", "file")
            .config("spark.datahub.file.filename", mcps.toString())
            .getOrCreate();
    try {
      Dataset<Row> stream =
          spark
              .readStream()
              .format("kafka")
              .option("kafka.bootstrap.servers", KAFKA.getBootstrapServers())
              .option("subscribe", TOPIC)
              .option("startingOffsets", "earliest")
              .load();

      StreamingQuery query =
          stream
              .selectExpr("CAST(value AS STRING) AS value")
              .writeStream()
              .format("parquet")
              .option("path", out.toString())
              .option("checkpointLocation", checkpoint.toString())
              // Drain what is already in the topic, then stop — keeps the test bounded.
              .trigger(Trigger.AvailableNow())
              .start();
      query.awaitTermination(Duration.ofMinutes(2).toMillis());
      query.stop();
    } finally {
      spark.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    }

    String emitted =
        Files.exists(mcps) ? new String(Files.readAllBytes(mcps), StandardCharsets.UTF_8) : "";

    assertTrue(
        emitted.contains("dataJobInputOutput"),
        "the listener emitted no DataJob lineage for the streaming query:\n" + emitted);

    // Assert the topic is specifically an INPUT edge of the DataJob, not merely present somewhere
    // in
    // the payload: a bare substring check on the whole document would also be satisfied by an
    // output
    // edge or a standalone dataset aspect, while the bug being guarded is precisely that the input
    // list came back empty.
    List<String> inputEdges = inputDatasetArrays(emitted);
    assertFalse(
        inputEdges.isEmpty(),
        "no dataJobInputOutput aspect carried an input-dataset list:\n" + emitted);
    assertTrue(
        inputEdges.stream().anyMatch(edges -> KAFKA_INPUT_URN.matcher(edges).find()),
        "the Kafka topic is not an input edge of the DataJob (issue #19005):"
            + " KafkaMicroBatchStreamStrategy silently dropped its inputs. Expected a URN matching "
            + KAFKA_INPUT_URN.pattern()
            + " but the emitted input lists were "
            + inputEdges
            + "\n"
            + emitted);
  }

  /**
   * Pulls the contents of every input-dataset list out of the emitted {@code dataJobInputOutput}
   * aspects. The aspect payload is a JSON string nested inside the MCP envelope, so it is unescaped
   * before matching; both the plain-URN and edge-object shapes of the aspect are accepted.
   */
  private static List<String> inputDatasetArrays(String emitted) {
    List<String> lists = new ArrayList<>();
    for (String chunk : emitted.split("\"aspectName\"")) {
      if (!chunk.contains("dataJobInputOutput")) {
        continue;
      }
      Matcher matcher = INPUT_DATASETS.matcher(chunk.replace("\\\"", "\""));
      while (matcher.find()) {
        lists.add(matcher.group(2));
      }
    }
    return lists;
  }

  private void produce(String... values) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", KAFKA.getBootstrapServers());
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());
    try (Producer<String, String> producer = new KafkaProducer<>(props)) {
      for (String value : values) {
        producer.send(new ProducerRecord<>(TOPIC, value, value)).get();
      }
    }
  }
}

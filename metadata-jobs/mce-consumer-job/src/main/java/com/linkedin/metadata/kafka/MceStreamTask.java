package com.linkedin.metadata.kafka;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.dao.internal.BaseRemoteWriterDAO;
import com.linkedin.metadata.dao.internal.RestliRemoteWriterDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.restli.client.Client;
import com.linkedin.util.Configuration;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;

import javax.annotation.Nonnull;
import java.util.Properties;


@Slf4j
public class MceStreamTask {
  private static final Properties CFG = Configuration.loadProperties("task.properties");

  private static BaseRemoteWriterDAO _remoteWriterDAO;

  public static void main(final String[] args) {
    log.info("Creating MCE consumer task");
    final Client restClient = Boolean.valueOf(CFG.getProperty("isRestLiClientUseD2")) ?
            DefaultRestliClientFactory.getRestLiD2Client(CFG.getProperty("restLiClientD2ZkHost"), CFG.getProperty("restLiClientD2ZkPath")) :
        DefaultRestliClientFactory.getRestLiClient(CFG.getProperty("restLiServerHost"), Integer.parseInt(CFG.getProperty("restLiServerPort")));
    _remoteWriterDAO = new RestliRemoteWriterDAO(restClient);
    log.info("RemoteWriterDAO built successfully");

    // Configure the Streams application.
    final Properties streamsConfiguration = getStreamsConfiguration();

    // Define the processing topology of the Streams application.
    final StreamsBuilder builder = new StreamsBuilder();
    createProcessingTopology(builder);
    final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

    // Clean local state prior to starting the processing topology.
    streams.cleanUp();

    // Now run the processing topology via `start()` to begin processing its input data.
    streams.start();

    // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application.
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  /**
   * Configure the Streams application.
   *
   * @return Properties getStreamsConfiguration
   */
  static Properties getStreamsConfiguration() {
    final Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "mce-consuming-job");
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "mce-consuming-job-client");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CFG.getProperty("kafkaBootstrapServers"));
    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName());
    streamsConfiguration.put("schema.registry.url", CFG.getProperty("kafkaSchemaRegistry"));
    // Continue handling events after exception
    streamsConfiguration.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndContinueExceptionHandler.class);
    // Records will be flushed every 10 seconds.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
        Integer.valueOf(CFG.getProperty("kafkaStreamsCommitIntervalInSeconds")) * 1000);
    // Disable record caches.
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    return streamsConfiguration;
  }

  /**
   * Define the processing topology for job.
   *
   * @param builder StreamsBuilder to use
   */
  static void createProcessingTopology(final StreamsBuilder builder) {
    // Construct a `KStream` from the input topic.
    // The default key and value serdes will be used.
    final KStream<String, GenericData.Record> messages = builder.stream(CFG.getProperty("kafkaTopic"));
    messages.foreach((k, v) -> processSingleMCE(v));
  }

  /**
   * Process MCE and write in the underlying DB.
   *
   * @param record single MCE message
   */
  static void processSingleMCE(final GenericData.Record record) {
    log.debug("Got MCE");

    try {
      final com.linkedin.mxe.MetadataChangeEvent event = EventUtils.avroToPegasusMCE(record);

      if (event.hasProposedSnapshot()) {
        processProposedSnapshot(event.getProposedSnapshot());
      }
    } catch (Throwable throwable) {
      log.error("MCE Processor Error", throwable);
      log.error("Message: {}", record);
    }
  }

  static void processProposedSnapshot(@Nonnull Snapshot snapshotUnion) {
    final RecordTemplate snapshot = RecordUtils.getSelectedRecordTemplateFromUnion(snapshotUnion);
    final Urn urn = ModelUtils.getUrnFromSnapshotUnion(snapshotUnion);
    _remoteWriterDAO.create(urn, snapshot);
  }
}
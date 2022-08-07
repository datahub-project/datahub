package datahub.client.kafka;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.linkedin.mxe.MetadataChangeProposal;

import datahub.client.Callback;
import datahub.client.Emitter;
import datahub.client.MetadataWriteResponse;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.event.UpsertAspectRequest;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaEmitter implements Emitter {

  public static final String DEFAULT_MCP_KAFKA_TOPIC = "MetadataChangeProposal_v1";

  private final KafkaEmitterConfig config;
  private final KafkaProducer<Object, Object> producer;
  private final Properties kafkaConfigProperties;
  private AvroSerializer _avroSerializer;
  private static final int ADMIN_CLIENT_TIMEOUT_MS = 5000;

  /**
   * The default constructor
   * 
   * @param config
   * @throws IOException
   */
  public KafkaEmitter(KafkaEmitterConfig config) throws IOException {
    this.config = config;
    kafkaConfigProperties = new Properties();
    kafkaConfigProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.config.getBootstrap());
    kafkaConfigProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.StringSerializer.class);
    kafkaConfigProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        io.confluent.kafka.serializers.KafkaAvroSerializer.class);
    kafkaConfigProperties.put("schema.registry.url", this.config.getSchemaRegistryUrl());
    kafkaConfigProperties.putAll(config.getSchemaRegistryConfig());
    kafkaConfigProperties.putAll(config.getProducerConfig());
    producer = new KafkaProducer<Object, Object>(kafkaConfigProperties);
    _avroSerializer = new AvroSerializer();
  }

  @Override
  public void close() throws IOException {
    producer.close();

  }

  @Override
  public Future<MetadataWriteResponse> emit(@SuppressWarnings("rawtypes") MetadataChangeProposalWrapper mcpw,
      Callback datahubCallback) throws IOException {
    return emit(this.config.getEventFormatter().convert(mcpw), datahubCallback);
  }

  @Override
  public Future<MetadataWriteResponse> emit(MetadataChangeProposal mcp, Callback datahubCallback) throws IOException {
    GenericRecord genricRecord = _avroSerializer.serialize(mcp);
    ProducerRecord<Object, Object> record = new ProducerRecord<>(KafkaEmitter.DEFAULT_MCP_KAFKA_TOPIC,
        mcp.getEntityUrn().toString(), genricRecord);
    org.apache.kafka.clients.producer.Callback callback = new org.apache.kafka.clients.producer.Callback() {

      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {
        MetadataWriteResponse response = mapResponse(metadata, exception);
        datahubCallback.onCompletion(response);
      }
    };
    log.debug("Emit: topic: {} \n record: {}", KafkaEmitter.DEFAULT_MCP_KAFKA_TOPIC, record);
    Future<RecordMetadata> future = this.producer.send(record, callback);
    return mapFuture(future);
  }

  private Future<MetadataWriteResponse> mapFuture(Future<RecordMetadata> future) {
    return new Future<MetadataWriteResponse>() {

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
      }

      @Override
      public MetadataWriteResponse get() throws InterruptedException, ExecutionException {
        RecordMetadata recordMetadata = future.get();
        return mapResponse(recordMetadata, null);
      }

      @Override
      public MetadataWriteResponse get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
        RecordMetadata recordMetadata = future.get(timeout, unit);
        return mapResponse(recordMetadata, null);
      }

      @Override
      public boolean isCancelled() {
        return future.isCancelled();
      }

      @Override
      public boolean isDone() {
        return future.isDone();
      }
    };

  }

  @Override
  public boolean testConnection() throws IOException, ExecutionException, InterruptedException {
    try (AdminClient client = AdminClient.create(this.kafkaConfigProperties)) {
      log.info("Available topics:"
          + client.listTopics(new ListTopicsOptions().timeoutMs(ADMIN_CLIENT_TIMEOUT_MS)).listings().get());
    } catch (ExecutionException ex) {
      log.error("Kafka is not available, timed out after {} ms", ADMIN_CLIENT_TIMEOUT_MS);
      return false;
    }
    return true;
  }

  @Override
  public Future<MetadataWriteResponse> emit(List<UpsertAspectRequest> request, Callback callback) throws IOException {
    throw new UnsupportedOperationException("UpsertAspectRequest cannot be sent over Kafka");
  }

  private static MetadataWriteResponse mapResponse(RecordMetadata metadata, Exception exception) {

    MetadataWriteResponse.MetadataWriteResponseBuilder builder = MetadataWriteResponse.builder();

    if (exception == null) {
      builder.success(true);
      builder.underlyingResponse(metadata);
      builder.responseContent(metadata.toString());
    } else {
      builder.success(false);
      builder.underlyingResponse(exception);
      builder.responseContent(exception.toString());
    }
    return builder.build();
  }

  public Properties getKafkaConfgiProperties() {
    return kafkaConfigProperties;
  }

}

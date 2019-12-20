package com.linkedin.metadata.kafka;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.builders.search.*;
import com.linkedin.metadata.neo4j.Neo4jDriverFactory;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.elasticsearch.ElasticsearchConnector;
import com.linkedin.metadata.utils.elasticsearch.ElasticsearchConnectorFactory;
import com.linkedin.metadata.utils.elasticsearch.MCEElasticEvent;
import com.linkedin.mxe.MetadataAuditEvent;
import com.linkedin.mxe.Topics;
import com.linkedin.util.Configuration;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.linkedin.metadata.builders.graph.BaseGraphBuilder;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.builders.graph.RegisteredGraphBuilders;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.metadata.dao.internal.Neo4jGraphWriterDAO;
import com.linkedin.metadata.dao.utils.RecordUtils;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.net.URLEncoder;
import java.util.*;

@Slf4j
public class MaeStreamTask {
    private static final String DOC_TYPE = "doc";

    private static final String DEFAULT_KAFKA_TOPIC_NAME = Topics.METADATA_AUDIT_EVENT;
    private static final String DEFAULT_KAFKA_BOOTSTRAP_SERVER = "localhost:9092";
    private static final String DEFAULT_KAFKA_SCHEMAREGISTRY_URL = "http://localhost:8081";

    private static final String DEFAULT_ELASTICSEARCH_HOST = "localhost";
    private static final String DEFAULT_ELASTICSEARCH_PORT = "9200";

    private static final String DEFAULT_NEO4J_URI = "bolt://localhost";
    private static final String DEFAULT_NEO4J_USERNAME = "neo4j";
    private static final String DEFAULT_NEO4J_PASSWORD = "datahub";

    private static ElasticsearchConnector _elasticSearchConnector;
    private static SnapshotProcessor _snapshotProcessor;

    private static BaseGraphWriterDAO _graphWriterDAO;

    public static void main(final String[] args) {
        initializateES();
        initializateNeo4j();

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

    static void initializateNeo4j() {
        _graphWriterDAO = new Neo4jGraphWriterDAO(Neo4jDriverFactory.createInstance(
                Configuration.getEnvironmentVariable("NEO4J_URI", DEFAULT_NEO4J_URI),
                Configuration.getEnvironmentVariable("NEO4J_USERNAME", DEFAULT_NEO4J_USERNAME),
                Configuration.getEnvironmentVariable("NEO4J_PASSWORD", DEFAULT_NEO4J_PASSWORD)));
        log.info("Neo4jDriver built successfully");
    }

    static void initializateES() {
        // Initialize ElasticSearch connector and Snapshot processor
        _elasticSearchConnector = ElasticsearchConnectorFactory.createInstance(
            Configuration.getEnvironmentVariable("ELASTICSEARCH_HOST", DEFAULT_ELASTICSEARCH_HOST),
            Integer.valueOf(Configuration.getEnvironmentVariable("ELASTICSEARCH_PORT", DEFAULT_ELASTICSEARCH_PORT))
        );
        _snapshotProcessor = new SnapshotProcessor(RegisteredIndexBuilders.REGISTERED_INDEX_BUILDERS);
        log.info("ElasticSearchConnector built successfully");
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
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "mae-consumer-job");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "mae-consumer-job-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                Configuration.getEnvironmentVariable("KAFKA_BOOTSTRAP_SERVER", DEFAULT_KAFKA_BOOTSTRAP_SERVER));
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName());
        streamsConfiguration.put("schema.registry.url",
                Configuration.getEnvironmentVariable("KAFKA_SCHEMAREGISTRY_URL", DEFAULT_KAFKA_SCHEMAREGISTRY_URL));
        // Records will be flushed every 10 seconds.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Integer.valueOf(10000));
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
        final KStream<String, GenericData.Record> messages = builder.stream(Configuration
                .getEnvironmentVariable("KAFKA_TOPIC_NAME", DEFAULT_KAFKA_TOPIC_NAME));
        messages.foreach((k, v) -> processSingleMAE(v));
    }

    /**
     * Process MAE and update Elasticsearch & Neo4j
     *
     * @param record single MAE message
     */
    static void processSingleMAE(final GenericData.Record record) {
        log.debug("Got MAE");

        try {
            final MetadataAuditEvent event = EventUtils.avroToPegasusMAE(record);
            if (event.hasNewSnapshot()) {
                final Snapshot snapshot = event.getNewSnapshot();

                log.info(snapshot.toString());

                updateElasticsearch(snapshot);
                updateNeo4j(RecordUtils.getSelectedRecordTemplateFromUnion(snapshot));
            }
        } catch (Exception e) {
            log.error("Error deserializing message: {}", e.toString());
            log.error("Message: {}", record.toString());
        }
    }

    /**
     * Process snapshot and update Neo4j
     *
     * @param snapshot Snapshot
     */
    static void updateNeo4j(final RecordTemplate snapshot) {
        try {
            final BaseGraphBuilder graphBuilder = RegisteredGraphBuilders.getGraphBuilder(snapshot.getClass()).get();
            final GraphBuilder.GraphUpdates updates = graphBuilder.build(snapshot);

            if (!updates.getEntities().isEmpty()) {
                _graphWriterDAO.addEntities(updates.getEntities());
            }

            for (GraphBuilder.RelationshipUpdates update : updates.getRelationshipUpdates()) {
                _graphWriterDAO.addRelationships(update.getRelationships(), update.getPreUpdateOperation());
            }
        } catch (Exception ex) {
            log.error(ex.toString());
        }
    }

    /**
     * Process snapshot and update Elasticsearch
     *
     * @param snapshot Snapshot
     */
    static void updateElasticsearch(final Snapshot snapshot) {
        List<RecordTemplate> docs = new ArrayList<>();
        try {
            docs = _snapshotProcessor.getDocumentsToUpdate(snapshot);
        } catch (Exception e) {
            log.error("Error in getting documents from snapshot: {}", e.toString());
        }

        for (RecordTemplate doc : docs) {
            MCEElasticEvent elasticEvent = new MCEElasticEvent(doc);
            BaseIndexBuilder indexBuilderForDoc = null;
            for (BaseIndexBuilder indexBuilder : RegisteredIndexBuilders.REGISTERED_INDEX_BUILDERS) {
                Class docType = indexBuilder.getDocumentType();
                if (docType.isInstance(doc)) {
                    indexBuilderForDoc = indexBuilder;
                    break;
                }
            }
            if (indexBuilderForDoc == null) {
                continue;
            }
            elasticEvent.setIndex(indexBuilderForDoc.getDocumentType().getSimpleName().toLowerCase());
            elasticEvent.setType(DOC_TYPE);
            try {
                String urn = indexBuilderForDoc.getDocumentType().getMethod("getUrn").invoke(doc).toString();
                elasticEvent.setId(URLEncoder.encode(urn.toLowerCase(), "UTF-8"));
            } catch (UnsupportedEncodingException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                log.error("Failed to encode the urn with error ", e.toString());
                continue;
            }
            elasticEvent.setActionType(ChangeType.UPDATE);
            _elasticSearchConnector.feedElasticEvent(elasticEvent);
        }
    }
}
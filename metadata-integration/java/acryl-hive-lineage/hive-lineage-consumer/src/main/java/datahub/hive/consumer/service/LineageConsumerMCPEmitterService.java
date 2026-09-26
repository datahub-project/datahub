package datahub.hive.consumer.service;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.DataTemplate;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.FabricType;
import datahub.client.kafka.KafkaEmitter;
import datahub.hive.consumer.util.MCPEmitterUtil;
import datahub.hive.consumer.util.TimeUtils;
import datahub.hive.consumer.config.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Service for consuming Hive lineage messages from Kafka, transforming them into MCPs and emitting them to DataHub.
 */
@Service
@Slf4j
public class LineageConsumerMCPEmitterService {

    private final MCPTransformerService datasetMCPTransformerService;

    private final MCPTransformerService queryMCPTransformerService;

    private final KafkaEmitter kafkaEmitter;

    @Value("${spring.kafka.consumer.retry.max-attempts:3}")
    private int maxRetryAttempts;

    public LineageConsumerMCPEmitterService(@Qualifier("datasetMCPTransformerServiceImpl") MCPTransformerService datasetMCPTransformerService, @Qualifier("queryMCPTransformerServiceImpl") MCPTransformerService queryMCPTransformerService, KafkaEmitter kafkaEmitter) {
        this.datasetMCPTransformerService = datasetMCPTransformerService;
        this.queryMCPTransformerService = queryMCPTransformerService;
        this.kafkaEmitter = kafkaEmitter;
    }

    /**
     * Consumes messages from the Hive lineage topic and processes them.
     *
     * @param record The Kafka consumer record
     * @throws Exception If there's an error during message processing
     */
    @KafkaListener(
        topics = "${spring.kafka.consumer.hive.lineage.topic}", 
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeLineageMessage(ConsumerRecord<String, String> record) throws Exception {
        long startTime = System.currentTimeMillis();
        String key = record.key();
        String value = record.value();
        
        log.info("Received lineage message with key: {}", key);
        
        try {
            JsonObject lineageJson = JsonParser.parseString(value).getAsJsonObject();         
            processLineageMessage(lineageJson);
            long duration = TimeUtils.calculateDuration(startTime);
            log.info("Processed lineage message with key: {}. Time taken: {} ms", key, duration);
        } catch (Exception e) {
            log.error("Error processing lineage message with key: {} after {} retries", key, maxRetryAttempts, e);
            throw e;
        }
    }

    /**
     * Processes lineage message by transforming it into MCPs.
     *
     * @param lineageJson The JSON object representing the lineage message
     * @throws IOException If there's an error during MCP emission
     * @throws URISyntaxException If there's an error with URIs in the lineage data
     */
    private void processLineageMessage(JsonObject lineageJson) throws IOException, URISyntaxException {
        if (lineageJson.has(Constants.OUTPUTS_KEY) && !lineageJson.getAsJsonArray(Constants.OUTPUTS_KEY).isEmpty()) {
            // Transform and emit MCPs for the dataset entity
            List<DataTemplate<DataMap>> datasetAspects = datasetMCPTransformerService.transformToMCP(lineageJson);
            emit(lineageJson, datasetAspects, kafkaEmitter);

            // Transform and emit MCPs for the query entity
            List<DataTemplate<DataMap>> queryAspects = queryMCPTransformerService.transformToMCP(lineageJson);
            emit(lineageJson, queryAspects, kafkaEmitter);
        } else {
            log.info("Skipping lineage message as it does not have any outputs");
        }
    }

    /**
     * Emits MCPs for the given aspects.
     *
     * @param lineageJson The JSON object representing the lineage message
     * @param aspects The list of aspects to emit
     * @param kafkaEmitter The Kafka emitter to use for emitting MCPs
     * @throws IOException If there's an error during MCP emission
     * @throws URISyntaxException If there's an error with URIs in the lineage data
     */
    private void emit(JsonObject lineageJson, List<DataTemplate<DataMap>> aspects, KafkaEmitter kafkaEmitter) throws IOException, URISyntaxException {
        String entityUrn;
        String entityType;

        for (DataTemplate<DataMap> aspect : aspects) {
            // Determine entity type and URN based on the aspect type
            if (aspect.getClass().getName().contains(Constants.QUERY_KEY)) {
                entityType = Constants.QUERY_KEY;
                String queryId = lineageJson.get(Constants.HASH_KEY).getAsString();
                entityUrn = Constants.QUERY_URN_PREFIX + queryId;
            } else {
                entityType = Constants.DATASET_KEY;
                entityUrn = getDatasetUrn(lineageJson).toString();
            }
            MCPEmitterUtil.emitMCP(aspect, entityUrn, entityType, kafkaEmitter);
            log.info("Emitted MCP for entity: {}", entityUrn);
        }
    }

    /**
     * Gets the dataset URN for the given dataset JSON object.
     *
     * @param datasetJsonObject The JSON object representing the dataset
     * @return The dataset URN
     */
    private DatasetUrn getDatasetUrn(JsonObject datasetJsonObject) {
        String platformInstance = datasetJsonObject.get(Constants.PLATFORM_INSTANCE_KEY).getAsString();
        String datasetName = platformInstance + "." + datasetJsonObject.get(Constants.OUTPUTS_KEY).getAsJsonArray().get(0).getAsString();
        String environment = datasetJsonObject.get(Constants.ENVIRONMENT_KEY).getAsString();

        return new DatasetUrn(
                new DataPlatformUrn(Constants.PLATFORM_NAME),
                datasetName,
                FabricType.valueOf(environment)
        );
    }
}

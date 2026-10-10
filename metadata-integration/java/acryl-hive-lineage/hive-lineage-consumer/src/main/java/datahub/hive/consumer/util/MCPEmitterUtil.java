package datahub.hive.consumer.util;

import com.linkedin.data.template.DataTemplate;
import com.linkedin.data.DataMap;
import datahub.client.kafka.KafkaEmitter;
import datahub.client.Callback;
import datahub.client.MetadataWriteResponse;
import datahub.event.MetadataChangeProposalWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringEscapeUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Utility class for emitting Metadata Change Proposals (MCPs) to DataHub.
 */
@Slf4j
public class MCPEmitterUtil {

    private MCPEmitterUtil() {
        // Private constructor to prevent instantiation
    }

    /**
     * Emits a Metadata Change Proposal (MCP) to DataHub via Kafka.
     *
     * @param aspect The aspect to emit
     * @param entityUrn The URN of entity
     * @param entityType The type of entity
     * @param kafkaEmitter The KafkaEmitter to use for sending messages
     * @throws IOException If an I/O error occurs
     * @throws URISyntaxException If a URI syntax error occurs
     */
    public static void emitMCP(DataTemplate<DataMap> aspect, String entityUrn, String entityType,
                                KafkaEmitter kafkaEmitter) throws IOException, URISyntaxException {

        // Create the MCP wrapper
        MetadataChangeProposalWrapper<?> mcpw = MetadataChangeProposalWrapper.builder()
                .entityType(entityType)
                .entityUrn(entityUrn)
                .upsert()
                .aspect(aspect)
                .build();

        try {
            long startTime = System.currentTimeMillis();
            Callback callback = new Callback() {
                @Override
                public void onCompletion(MetadataWriteResponse response) {
                    if (response.isSuccess()) {
                        long duration = TimeUtils.calculateDuration(startTime);
                        log.info("Successfully emitted metadata change event for aspect {} for {}. Time taken: {} ms", 
                                StringEscapeUtils.escapeJava(mcpw.getAspectName()), 
                                StringEscapeUtils.escapeJava(mcpw.getEntityUrn()),
                                duration);
                    } else {
                        log.error("Failed to emit metadata change event for {}, aspect: {} due to {}", 
                                entityUrn, StringEscapeUtils.escapeJava(mcpw.getAspectName()), response.getResponseContent());
                    }
                }

                @Override
                public void onFailure(Throwable throwable) {
                    log.error("Failed to emit metadata change event for {}, aspect: {} due to {}", 
                            entityUrn, mcpw.getAspectName(), throwable.getMessage(), throwable);
                }
            };
            
            log.info("Emitting MCP for entity: {}, aspect: {}", 
                    StringEscapeUtils.escapeJava(mcpw.getEntityUrn()),
                    StringEscapeUtils.escapeJava(mcpw.getAspectName()));
            
            Future<MetadataWriteResponse> future = kafkaEmitter.emit(mcpw, callback);
            MetadataWriteResponse response = future.get();
            
            if (!response.isSuccess()) {
                log.error("Failed to emit MCP: {}", response.getResponseContent());
                throw new IOException("Failed to emit MCP: " + response.getResponseContent());
            } else {
                log.info("Successfully emitted MCP for entity: {}, aspect: {}", 
                        StringEscapeUtils.escapeJava(mcpw.getEntityUrn()),
                        StringEscapeUtils.escapeJava(mcpw.getAspectName()));
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to emit metadata change event for {}, aspect: {} due to {}", 
                    entityUrn, mcpw.getAspectName(), e.getMessage());
            Thread.currentThread().interrupt();
            throw new IOException("Error emitting MCP", e);
        }
    }
}

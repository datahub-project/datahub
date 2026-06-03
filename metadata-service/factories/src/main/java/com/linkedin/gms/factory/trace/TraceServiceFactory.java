package com.linkedin.gms.factory.trace;

import com.linkedin.metadata.config.messaging.KafkaOrPgQueueMessagingTransportCondition;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.systemmetadata.TraceService;
import com.linkedin.metadata.trace.McpFailedTracePort;
import com.linkedin.metadata.trace.McpPendingTracePort;
import com.linkedin.metadata.trace.TraceServiceImpl;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(KafkaOrPgQueueMessagingTransportCondition.class)
public class TraceServiceFactory {

  @Bean
  public TraceService traceService(
      @Qualifier("entityRegistry") EntityRegistry entityRegistry,
      @Qualifier("entityService") EntityService<?> entityService,
      @Qualifier("systemMetadataService") SystemMetadataService systemMetadataService,
      @Qualifier("mcpTraceReader") McpPendingTracePort mcpTraceReader,
      @Qualifier("mcpFailedTraceReader") McpFailedTracePort mcpFailedTraceReader) {
    return TraceServiceImpl.builder()
        .entityRegistry(entityRegistry)
        .entityService(entityService)
        .systemMetadataService(systemMetadataService)
        .mcpTraceReader(mcpTraceReader)
        .mcpFailedTraceReader(mcpFailedTraceReader)
        .build();
  }
}

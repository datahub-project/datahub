package com.linkedin.gms.factory.trace;

import com.linkedin.metadata.config.messaging.KafkaOrPgQueueMessagingTransportCondition;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.StructuredPropertyMappingPutTiming;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.systemmetadata.TraceService;
import com.linkedin.metadata.trace.McpFailedTracePort;
import com.linkedin.metadata.trace.McpPendingTracePort;
import com.linkedin.metadata.trace.TraceServiceImpl;
import javax.annotation.Nullable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
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
      @Qualifier("mcpFailedTraceReader") McpFailedTracePort mcpFailedTraceReader,
      @Value("${trace.searchCommitGraceMillis:#{null}}") @Nullable Long searchCommitGraceMillis,
      @Value("${elasticsearch.index.refreshIntervals.systemMetadataSeconds:3}")
          long systemMetadataRefreshSeconds) {
    long graceMillis =
        searchCommitGraceMillis != null
            ? searchCommitGraceMillis
            : StructuredPropertyMappingPutTiming.searchCommitGraceMillis(
                systemMetadataRefreshSeconds * 1000L);
    return TraceServiceImpl.builder()
        .entityRegistry(entityRegistry)
        .entityService(entityService)
        .systemMetadataService(systemMetadataService)
        .mcpTraceReader(mcpTraceReader)
        .mcpFailedTraceReader(mcpFailedTraceReader)
        .searchCommitGraceMillis(graceMillis)
        .build();
  }
}

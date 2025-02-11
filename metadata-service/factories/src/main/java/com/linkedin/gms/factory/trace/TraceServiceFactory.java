package com.linkedin.gms.factory.trace;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.systemmetadata.TraceService;
import com.linkedin.metadata.trace.MCLTraceReader;
import com.linkedin.metadata.trace.MCPFailedTraceReader;
import com.linkedin.metadata.trace.MCPTraceReader;
import com.linkedin.metadata.trace.TraceServiceImpl;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TraceServiceFactory {

  @Bean
  public TraceService traceService(
      @Qualifier("entityRegistry") EntityRegistry entityRegistry,
      @Qualifier("entityService") EntityService<?> entityService,
      @Qualifier("systemMetadataService") SystemMetadataService systemMetadataService,
      @Qualifier("mcpTraceReader") MCPTraceReader mcpTraceReader,
      @Qualifier("mcpFailedTraceReader") MCPFailedTraceReader mcpFailedTraceReader,
      @Qualifier("mclVersionedTraceReader") MCLTraceReader mclVersionedTraceReader,
      @Qualifier("mclTimeseriesTraceReader") MCLTraceReader mclTimeseriesTraceReader) {
    return TraceServiceImpl.builder()
        .entityRegistry(entityRegistry)
        .entityService(entityService)
        .systemMetadataService(systemMetadataService)
        .mcpTraceReader(mcpTraceReader)
        .mcpFailedTraceReader(mcpFailedTraceReader)
        .mclVersionedTraceReader(mclVersionedTraceReader)
        .mclTimeseriesTraceReader(mclTimeseriesTraceReader)
        .build();
  }
}

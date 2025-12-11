/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.system.bootstrapmcps.BootstrapMCP;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BootstrapMCPConfig {

  @Nonnull
  @Value("${systemUpdate.bootstrap.mcpConfig}")
  private String bootstrapMCPConfig;

  @Bean(name = "bootstrapMCPNonBlocking")
  public BootstrapMCP bootstrapMCPNonBlocking(
      final OperationContext opContext, EntityService<?> entityService) throws IOException {
    return new BootstrapMCP(opContext, bootstrapMCPConfig, entityService, false);
  }

  @Bean(name = "bootstrapMCPBlocking")
  public BootstrapMCP bootstrapMCPBlocking(
      final OperationContext opContext, EntityService<?> entityService) throws IOException {
    return new BootstrapMCP(opContext, bootstrapMCPConfig, entityService, true);
  }
}

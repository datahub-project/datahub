/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.removeunknownaspects.RemoveUnknownAspects;
import com.linkedin.metadata.entity.EntityService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RemoveUnknownAspectsConfig {
  @Bean(name = "removeUnknownAspects")
  public RemoveUnknownAspects removeUnknownAspects(EntityService<?> entityService) {
    return new RemoveUnknownAspects(entityService);
  }
}

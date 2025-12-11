/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.entity.versioning;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.versioning.EntityVersioningService;
import com.linkedin.metadata.entity.versioning.EntityVersioningServiceImpl;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class EntityVersioningServiceFactory {

  @Bean(name = "entityVersioningService")
  @Nonnull
  protected EntityVersioningService createInstance(EntityService<?> entityService) {

    return new EntityVersioningServiceImpl(entityService);
  }
}

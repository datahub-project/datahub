/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.common;

import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.SiblingGraphService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@Configuration
@Import({EntityServiceFactory.class})
public class SiblingGraphServiceFactory {

  @Autowired
  @Qualifier("entityService")
  private EntityService<?> _entityService;

  @Bean(name = "siblingGraphService")
  @Primary
  @Nonnull
  protected SiblingGraphService getInstance(final GraphService graphService) {
    return new SiblingGraphService(_entityService, graphService);
  }
}

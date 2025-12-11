/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.timeline;

import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.TimelineServiceImpl;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class TimelineServiceFactory {

  @Bean(name = "timelineService")
  @DependsOn({"entityAspectDao", "entityService", "entityRegistry"})
  @Nonnull
  protected TimelineService timelineService(
      @Qualifier("entityAspectDao") AspectDao aspectDao, EntityRegistry entityRegistry) {
    return new TimelineServiceImpl(aspectDao, entityRegistry);
  }
}

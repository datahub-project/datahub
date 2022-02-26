package com.linkedin.gms.factory.timeline;

import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.ebean.EbeanTimelineService;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.PropertySource;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class TimelineServiceFactory {

  @Bean(name = "timelineService")
  @DependsOn({"ebeanServer", "entityService"})
  @Nonnull
  protected TimelineService timelineService(EbeanAspectDao ebeanAspectDao) {
    return new EbeanTimelineService(ebeanAspectDao);
  }
}

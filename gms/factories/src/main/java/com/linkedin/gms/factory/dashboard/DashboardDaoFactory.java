package com.linkedin.gms.factory.dashboard;

import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.metadata.aspect.DashboardAspect;
import com.linkedin.metadata.dao.EbeanLocalDAO;
import com.linkedin.metadata.dao.producer.KafkaMetadataEventProducer;
import com.linkedin.metadata.snapshot.DashboardSnapshot;
import io.ebean.config.ServerConfig;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;


@Configuration
public class DashboardDaoFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "dashboardDAO")
  @DependsOn({"gmsEbeanServiceConfig", "kafkaEventProducer"})
  @Nonnull
  protected EbeanLocalDAO createInstance() {
    KafkaMetadataEventProducer<DashboardSnapshot, DashboardAspect, DashboardUrn> producer =
        new KafkaMetadataEventProducer(DashboardSnapshot.class, DashboardAspect.class,
            applicationContext.getBean(Producer.class));
    return new EbeanLocalDAO<>(DashboardAspect.class, producer, applicationContext.getBean(ServerConfig.class),
        DashboardUrn.class);
  }
}

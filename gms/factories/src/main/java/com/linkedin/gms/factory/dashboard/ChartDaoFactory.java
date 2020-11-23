package com.linkedin.gms.factory.dashboard;

import com.linkedin.common.urn.ChartUrn;
import com.linkedin.metadata.aspect.ChartAspect;
import com.linkedin.metadata.dao.EbeanLocalDAO;
import com.linkedin.metadata.dao.producer.KafkaMetadataEventProducer;
import com.linkedin.metadata.snapshot.ChartSnapshot;
import io.ebean.config.ServerConfig;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;


@Configuration
public class ChartDaoFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "chartDAO")
  @DependsOn({"gmsEbeanServiceConfig", "kafkaEventProducer"})
  @Nonnull
  protected EbeanLocalDAO createInstance() {
    KafkaMetadataEventProducer<ChartSnapshot, ChartAspect, ChartUrn> producer =
        new KafkaMetadataEventProducer(ChartSnapshot.class, ChartAspect.class,
            applicationContext.getBean(Producer.class));
    return new EbeanLocalDAO<>(ChartAspect.class, producer, applicationContext.getBean(ServerConfig.class),
        ChartUrn.class);
  }
}

package com.linkedin.gms.factory.datajob;

import javax.annotation.Nonnull;

import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.metadata.aspect.DataJobAspect;
import com.linkedin.metadata.dao.EbeanLocalDAO;
import com.linkedin.metadata.dao.producer.KafkaMetadataEventProducer;
import com.linkedin.metadata.snapshot.DataJobSnapshot;

import io.ebean.config.ServerConfig;

@Configuration
public class DataJobDAOFactory {
    @Autowired
    private ApplicationContext applicationContext;

    @Bean(name = "dataJobDAO")
    @DependsOn({"gmsEbeanServiceConfig", "kafkaEventProducer"})
    @Nonnull
    protected EbeanLocalDAO<DataJobAspect, DataJobUrn> createInstance() {
        KafkaMetadataEventProducer<DataJobSnapshot, DataJobAspect, DataJobUrn> producer =
            new KafkaMetadataEventProducer<>(DataJobSnapshot.class, DataJobAspect.class,
                applicationContext.getBean(Producer.class));
        return new EbeanLocalDAO<>(DataJobAspect.class, producer, applicationContext.getBean(ServerConfig.class), DataJobUrn.class);
    }
}

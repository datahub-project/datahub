package com.linkedin.gms.factory.datajob;

import javax.annotation.Nonnull;

import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.metadata.aspect.DataFlowAspect;
import com.linkedin.metadata.dao.EbeanLocalDAO;
import com.linkedin.metadata.dao.producer.KafkaMetadataEventProducer;
import com.linkedin.metadata.snapshot.DataFlowSnapshot;

import io.ebean.config.ServerConfig;

@Configuration
public class DataFlowDAOFactory {
    @Autowired
    private ApplicationContext applicationContext;

    @Bean(name = "dataFlowDAO")
    @DependsOn({"gmsEbeanServiceConfig", "kafkaEventProducer"})
    @Nonnull
    protected EbeanLocalDAO<DataFlowAspect, DataFlowUrn> createInstance() {
        KafkaMetadataEventProducer<DataFlowSnapshot, DataFlowAspect, DataFlowUrn> producer =
            new KafkaMetadataEventProducer<>(DataFlowSnapshot.class, DataFlowAspect.class,
                applicationContext.getBean(Producer.class));
        return new EbeanLocalDAO<>(DataFlowAspect.class, producer, applicationContext.getBean(ServerConfig.class), DataFlowUrn.class);
    }
}

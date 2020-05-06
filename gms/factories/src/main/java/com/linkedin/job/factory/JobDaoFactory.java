package com.linkedin.job.factory;

import com.linkedin.common.urn.JobUrn;
import com.linkedin.metadata.aspect.JobAspect;
import com.linkedin.metadata.dao.EbeanLocalDAO;
import com.linkedin.metadata.dao.producer.KafkaMetadataEventProducer;
import com.linkedin.metadata.dao.producer.KafkaProducerCallback;
import com.linkedin.metadata.snapshot.JobSnapshot;
import io.ebean.config.ServerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
@ComponentScan(basePackages = "com.linkedin")
public class JobDaoFactory {
    @Autowired
    ApplicationContext applicationContext;

    @Bean(name = "jobDao")
    @DependsOn({"gmsEbeanServiceConfig", "kafkaEventProducer"})
    protected EbeanLocalDAO<JobAspect, JobUrn> createInstance() {
        KafkaMetadataEventProducer<JobSnapshot, JobAspect, JobUrn> producer =
                new KafkaMetadataEventProducer(JobSnapshot.class,
                        JobAspect.class,
                        applicationContext.getBean(Producer.class),
                        new KafkaProducerCallback());

        return new EbeanLocalDAO<>(JobAspect.class, producer, applicationContext.getBean(ServerConfig.class));
    }
}

package com.linkedin.job.factory;

import com.linkedin.metadata.configs.JobSearchConfig;
import com.linkedin.metadata.dao.search.ESSearchDAO;
import com.linkedin.metadata.search.JobDocument;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;

@Configuration
public class JobSearchDaoFactory {
    @Autowired
    ApplicationContext applicationContext;

    @Bean(name = "jobSearchDao")
    @DependsOn({"elasticSearchRestHighLevelClient"})
    @Nonnull
    protected ESSearchDAO createInstance() {
        return new ESSearchDAO(applicationContext.getBean(RestHighLevelClient.class), JobDocument.class,
                new JobSearchConfig());
    }
}

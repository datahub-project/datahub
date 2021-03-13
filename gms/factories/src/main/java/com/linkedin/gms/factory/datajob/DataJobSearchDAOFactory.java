package com.linkedin.gms.factory.datajob;

import com.linkedin.metadata.configs.DataJobSearchConfig;
import com.linkedin.metadata.dao.search.ESSearchDAO;
import com.linkedin.metadata.search.DataJobDocument;

import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;

@Configuration
public class DataJobSearchDAOFactory {

    @Autowired
    ApplicationContext applicationContext;

    @Bean(name = "dataJobSearchDAO")
    @DependsOn({"elasticSearchRestHighLevelClient"})
    @Nonnull
    protected ESSearchDAO createInstance() {
        return new ESSearchDAO(applicationContext.getBean(RestHighLevelClient.class), DataJobDocument.class,
            new DataJobSearchConfig());
    }
}

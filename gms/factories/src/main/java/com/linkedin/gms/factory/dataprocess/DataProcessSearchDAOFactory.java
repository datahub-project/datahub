package com.linkedin.gms.factory.dataprocess;

import com.linkedin.metadata.configs.DataProcessSearchConfig;
import com.linkedin.metadata.dao.search.ESSearchDAO;
import com.linkedin.metadata.search.DataProcessDocument;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;

@Configuration
public class DataProcessSearchDAOFactory {

    @Autowired
    ApplicationContext applicationContext;

    @Bean(name = "dataProcessSearchDAO")
    @DependsOn({"elasticSearchRestHighLevelClient"})
    @Nonnull
    protected ESSearchDAO createInstance() {
        return new ESSearchDAO(applicationContext.getBean(RestHighLevelClient.class), DataProcessDocument.class,
                new DataProcessSearchConfig());
    }
}

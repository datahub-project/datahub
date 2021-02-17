package com.linkedin.gms.factory.ml;

import com.linkedin.metadata.configs.MLModelSearchConfig;
import com.linkedin.metadata.dao.search.ESSearchDAO;
import com.linkedin.metadata.search.MLModelDocument;

import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;

@Configuration
public class MLModelSearchDAOFactory {

    @Autowired
    ApplicationContext applicationContext;

    @Bean(name = "mlModelSearchDAO")
    @DependsOn({"elasticSearchRestHighLevelClient"})
    @Nonnull
    protected ESSearchDAO createInstance() {
        return new ESSearchDAO(applicationContext.getBean(RestHighLevelClient.class), MLModelDocument.class,
            new MLModelSearchConfig());
    }
}

package com.linkedin.gms.factory.datajob;

import com.linkedin.metadata.configs.BrowseConfigFactory;
import com.linkedin.metadata.dao.browse.ESBrowseDAO;
import com.linkedin.metadata.search.DataFlowDocument;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;


@Configuration
public class DataFlowBrowseDAOFactory {
    @Autowired
    ApplicationContext applicationContext;

    @Nonnull
    @Bean(name = "dataFlowBrowseDao")
    @DependsOn({"elasticSearchRestHighLevelClient"})
    protected ESBrowseDAO createInstance() {
        return new ESBrowseDAO(applicationContext.getBean(RestHighLevelClient.class),
                BrowseConfigFactory.getBrowseConfig(DataFlowDocument.class));
    }
}

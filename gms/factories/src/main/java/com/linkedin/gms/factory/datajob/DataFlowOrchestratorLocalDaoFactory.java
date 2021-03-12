package com.linkedin.gms.factory.datajob;

import com.linkedin.common.urn.DataFlowOrchestratorUrn;
import com.linkedin.metadata.aspect.DataFlowOrchestratorAspect;
import com.linkedin.metadata.dao.ImmutableLocalDAO;
import com.linkedin.metadata.resources.datajob.utils.DataFlowOrchestratorUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class DataFlowOrchestratorLocalDaoFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "dataFlowOrchestratorLocalDAO")
  protected ImmutableLocalDAO createInstance() {
    return new ImmutableLocalDAO<>(DataFlowOrchestratorAspect.class, DataFlowOrchestratorUtil.getDataFlowOrchestratorInfoMap(),
        DataFlowOrchestratorUrn.class);
  }
}

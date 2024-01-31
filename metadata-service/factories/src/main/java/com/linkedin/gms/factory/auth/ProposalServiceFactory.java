package com.linkedin.gms.factory.auth;

import com.datahub.authentication.proposal.ProposalService;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class ProposalServiceFactory {
  @Autowired
  @Qualifier("entityService")
  private EntityService _entityService;

  @Autowired
  @Qualifier("graphClient")
  private GraphClient _graphClient;

  @Bean(name = "proposalService")
  @Scope("singleton")
  @Nonnull
  protected ProposalService getInstance(final @Qualifier("entityClient") EntityClient entityClient)
      throws Exception {
    return new ProposalService(this._entityService, entityClient, this._graphClient);
  }
}

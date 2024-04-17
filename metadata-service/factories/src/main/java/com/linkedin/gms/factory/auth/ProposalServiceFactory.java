package com.linkedin.gms.factory.auth;

import com.datahub.authentication.proposal.ProposalService;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ProposalServiceFactory {
  @Autowired
  @Qualifier("entityService")
  private EntityService<?> _entityService;

  @Autowired
  @Qualifier("graphClient")
  private GraphClient _graphClient;

  @Bean(name = "proposalService")
  @Nonnull
  protected ProposalService getInstance(final @Qualifier("entityClient") EntityClient entityClient)
      throws Exception {
    return new ProposalService(this._entityService, entityClient, this._graphClient);
  }
}

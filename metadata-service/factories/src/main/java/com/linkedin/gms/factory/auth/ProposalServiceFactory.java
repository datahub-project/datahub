

package com.linkedin.gms.factory.auth;

import com.datahub.authentication.proposal.ProposalService;
import com.linkedin.entity.client.JavaEntityClient;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.entity.EntityService;
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
  @Qualifier("javaEntityClient")
  private JavaEntityClient _javaEntityClient;

  @Bean(name = "proposalService")
  @Scope("singleton")
  @Nonnull
  protected ProposalService getInstance() throws Exception {
    return new ProposalService(this._entityService, this._javaEntityClient);
  }
}
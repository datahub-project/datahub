package com.linkedin.gms.factory.test;

import com.datahub.authentication.Authentication;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.entity.JavaEntityClientFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.DomainService;
import com.linkedin.metadata.service.OwnerService;
import com.linkedin.metadata.service.TagService;
import com.linkedin.metadata.service.GlossaryTermService;
import com.linkedin.metadata.test.action.Action;
import com.linkedin.metadata.test.action.ActionApplier;
import com.linkedin.metadata.test.action.cleanup.DeprecationAction;
import com.linkedin.metadata.test.action.domain.SetDomainAction;
import com.linkedin.metadata.test.action.domain.UnsetDomainAction;
import com.linkedin.metadata.test.action.owner.AddOwnersAction;
import com.linkedin.metadata.test.action.owner.RemoveOwnersAction;
import com.linkedin.metadata.test.action.tag.AddTagsAction;
import com.linkedin.metadata.test.action.tag.RemoveTagsAction;
import com.linkedin.metadata.test.action.term.AddGlossaryTermsAction;
import com.linkedin.metadata.test.action.term.RemoveGlossaryTermsAction;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;

@Configuration
@Import({JavaEntityClientFactory.class, SystemAuthenticationFactory.class})
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class TestActionApplierFactory {

  @Autowired
  @Qualifier("javaEntityClient")
  private EntityClient entityClient;

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication systemAuthentication;

  @Autowired
  @Qualifier("entityService")
  private EntityService entityService;

  @Bean(name = "testActionApplier")
  @Nonnull
  protected ActionApplier getInstance() {
    List<Action> appliers = new ArrayList<>();
    TagService tagService = new TagService(entityClient, systemAuthentication);
    GlossaryTermService termsService = new GlossaryTermService(entityClient, systemAuthentication);
    OwnerService ownerService = new OwnerService(entityClient, systemAuthentication);
    DomainService domainService = new DomainService(entityClient, systemAuthentication);
    appliers.add(new AddTagsAction(tagService));
    appliers.add(new RemoveTagsAction(tagService));
    appliers.add(new AddGlossaryTermsAction(termsService));
    appliers.add(new RemoveGlossaryTermsAction(termsService));
    appliers.add(new AddOwnersAction(ownerService));
    appliers.add(new RemoveOwnersAction(ownerService));
    appliers.add(new SetDomainAction(domainService));
    appliers.add(new UnsetDomainAction(domainService));
    appliers.add(new DeprecationAction(entityService));
    return new ActionApplier(appliers);
  }
}

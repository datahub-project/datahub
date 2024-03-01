package com.linkedin.gms.factory.test;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.*;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.test.action.Action;
import com.linkedin.metadata.test.action.ActionApplier;
import com.linkedin.metadata.test.action.cleanup.DeprecationAction;
import com.linkedin.metadata.test.action.cleanup.UnDeprecationAction;
import com.linkedin.metadata.test.action.domain.SetDomainAction;
import com.linkedin.metadata.test.action.domain.UnsetDomainAction;
import com.linkedin.metadata.test.action.form.AssignFormAction;
import com.linkedin.metadata.test.action.form.SetFormPromptIncompleteAction;
import com.linkedin.metadata.test.action.form.UnassignFormAction;
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
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class TestActionApplierFactory {
  @Autowired
  @Qualifier("entityService")
  private EntityService entityService;

  @Bean(name = "testActionApplier")
  @Nonnull
  protected ActionApplier getInstance(final SystemEntityClient systemEntityClient) {
    List<Action> appliers = new ArrayList<>();
    TagService tagService =
        new TagService(systemEntityClient, systemEntityClient.getSystemAuthentication());
    GlossaryTermService termsService =
        new GlossaryTermService(systemEntityClient, systemEntityClient.getSystemAuthentication());
    OwnerService ownerService =
        new OwnerService(systemEntityClient, systemEntityClient.getSystemAuthentication());
    DomainService domainService =
        new DomainService(systemEntityClient, systemEntityClient.getSystemAuthentication());
    FormService formService =
        new FormService(systemEntityClient, systemEntityClient.getSystemAuthentication());
    appliers.add(new AddTagsAction(tagService));
    appliers.add(new RemoveTagsAction(tagService));
    appliers.add(new AddGlossaryTermsAction(termsService));
    appliers.add(new RemoveGlossaryTermsAction(termsService));
    appliers.add(new AddOwnersAction(ownerService));
    appliers.add(new RemoveOwnersAction(ownerService));
    appliers.add(new SetDomainAction(domainService));
    appliers.add(new UnsetDomainAction(domainService));
    appliers.add(new DeprecationAction(entityService));
    appliers.add(new UnDeprecationAction(entityService));
    appliers.add(new AssignFormAction(formService));
    appliers.add(new UnassignFormAction(formService));
    appliers.add(new SetFormPromptIncompleteAction(formService));
    return new ActionApplier(appliers);
  }
}

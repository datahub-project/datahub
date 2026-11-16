package com.linkedin.gms.factory.form;

import static com.linkedin.metadata.Constants.FORM_INFO_ASPECT_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.form.FormInfo;
import com.linkedin.metadata.service.FormService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Set;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testng.annotations.Test;

/**
 * Loads {@link FormServiceFactory} through Spring and checks that the {@code formService} singleton
 * reads forms through the system entity client, even when GMS also has a plain {@link EntityClient}
 * bean in the context.
 */
public class FormServiceFactoryTest {

  private static final Urn FORM_URN = UrnUtils.getUrn("urn:li:form:test-form");

  @Test
  public void testFormServiceReadsThroughTheSystemEntityClient() throws Exception {
    SystemEntityClient systemEntityClient = mock(SystemEntityClient.class);
    EntityClient userEntityClient = mock(EntityClient.class);
    OperationContext opContext = TestOperationContexts.systemContextNoValidate();
    FormInfo formInfo = new FormInfo().setName("Test form");
    when(systemEntityClient.getV2(
            any(OperationContext.class),
            eq(FORM_URN.getEntityType()),
            eq(FORM_URN),
            eq(Set.of(FORM_INFO_ASPECT_NAME))))
        .thenReturn(formInfoResponse(formInfo));

    new ApplicationContextRunner()
        .withUserConfiguration(FormServiceFactory.class)
        .withBean("systemEntityClient", SystemEntityClient.class, () -> systemEntityClient)
        .withBean("entityClient", EntityClient.class, () -> userEntityClient)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              FormService formService = context.getBean("formService", FormService.class);
              assertThat(context.getBean(FormService.class)).isSameAs(formService);

              assertThat(formService.getFormInfo(opContext, FORM_URN)).isEqualTo(formInfo);
              verifyNoInteractions(userEntityClient);
            });
  }

  private static EntityResponse formInfoResponse(FormInfo formInfo) {
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(FORM_INFO_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(formInfo.data())));
    return new EntityResponse()
        .setEntityName(FORM_URN.getEntityType())
        .setUrn(FORM_URN)
        .setAspects(aspects);
  }
}

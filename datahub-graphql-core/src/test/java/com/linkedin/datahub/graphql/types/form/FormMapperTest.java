package com.linkedin.datahub.graphql.types.form;

import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Form;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.form.FormNotificationSettings;
import com.linkedin.metadata.Constants;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class FormMapperTest {

  @Test
  public void testGetSuccess() throws Exception {
    com.linkedin.form.FormSettings formSettings =
        new com.linkedin.form.FormSettings()
            .setNotificationSettings(
                new FormNotificationSettings().setNotifyAssigneesOnPublish(true));
    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.FORM_SETTINGS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(formSettings.data())));
    EntityResponse response = new EntityResponse().setAspects(new EnvelopedAspectMap(aspects));
    response.setUrn(Urn.createFromString("urn:li:form:test"));

    Form form = FormMapper.map(null, response);

    assertTrue(form.getFormSettings().getNotificationSettings().getNotifyAssigneesOnPublish());
  }
}

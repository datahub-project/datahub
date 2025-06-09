package com.linkedin.datahub.graphql.utils;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.generated.FormNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.FormSettingsInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.FormUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class FormUtilsTest {

  @Test
  public void testUpdateFormSettings_newFormSettings() {
    OperationContext mockContext = Mockito.mock(OperationContext.class);
    EntityResponse mockResponse = null;
    FormSettingsInput formSettingsInput = new FormSettingsInput();
    FormNotificationSettingsInput notificationSettingsInput = new FormNotificationSettingsInput();
    notificationSettingsInput.setNotifyAssigneesOnPublish(true);
    formSettingsInput.setNotificationSettings(notificationSettingsInput);

    com.linkedin.form.FormSettings result =
        FormUtils.updateFormSettings(mockContext, mockResponse, formSettingsInput);

    assertNotNull(result);
    assertTrue(result.hasNotificationSettings());
    assertTrue(result.getNotificationSettings().isNotifyAssigneesOnPublish());
  }

  @Test
  public void testUpdateFormSettings_existingFormSettings() {
    OperationContext mockContext = Mockito.mock(OperationContext.class);
    EntityResponse mockResponse = Mockito.mock(EntityResponse.class);

    com.linkedin.form.FormSettings existingFormSettings = new com.linkedin.form.FormSettings();

    com.linkedin.form.FormNotificationSettings existingNotificationSettings =
        new com.linkedin.form.FormNotificationSettings();
    existingNotificationSettings.setNotifyAssigneesOnPublish(false);
    existingFormSettings.setNotificationSettings(existingNotificationSettings);

    FormSettingsInput formSettingsInput = new FormSettingsInput();
    FormNotificationSettingsInput notificationSettingsInput = new FormNotificationSettingsInput();
    notificationSettingsInput.setNotifyAssigneesOnPublish(true);
    formSettingsInput.setNotificationSettings(notificationSettingsInput);

    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setName(STATUS_ASPECT_NAME).setValue(new Aspect(existingFormSettings.data()));
    EnvelopedAspectMap envelopedAspectMap =
        new EnvelopedAspectMap(
            Collections.singletonMap(Constants.FORM_SETTINGS_ASPECT_NAME, envelopedAspect));
    when(mockResponse.getAspects()).thenReturn(envelopedAspectMap);

    com.linkedin.form.FormSettings result =
        FormUtils.updateFormSettings(mockContext, mockResponse, formSettingsInput);

    assertNotNull(result);
    assertTrue(result.hasNotificationSettings());
    assertTrue(result.getNotificationSettings().isNotifyAssigneesOnPublish());
  }

  @Test
  public void testUpdateFormSettings_emptyNotificationSettingsInput() {
    OperationContext mockContext = Mockito.mock(OperationContext.class);
    FormSettingsInput formSettingsInput = new FormSettingsInput();

    com.linkedin.form.FormSettings result =
        FormUtils.updateFormSettings(mockContext, null, formSettingsInput);

    assertNotNull(result);
    assertTrue(result.hasNotificationSettings());
    assertFalse(result.getNotificationSettings().isNotifyAssigneesOnPublish());
  }
}

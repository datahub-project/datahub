package com.linkedin.datahub.graphql.resolvers.settings.applications;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateApplicationsSettingsInput;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.ApplicationsSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpdateApplicationsSettingsResolverTest {

  private static final UpdateApplicationsSettingsInput TEST_INPUT_ENABLED =
      new UpdateApplicationsSettingsInput(true);
  private static final UpdateApplicationsSettingsInput TEST_INPUT_DISABLED =
      new UpdateApplicationsSettingsInput(false);

  @Test
  public void testGetSuccessNoExistingSettings() throws Exception {
    SettingsService mockService = initSettingsService(null);
    UpdateApplicationsSettingsResolver resolver =
        new UpdateApplicationsSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_ENABLED);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1))
        .updateGlobalSettings(
            any(),
            Mockito.eq(
                new GlobalSettingsInfo()
                    .setApplications(new ApplicationsSettings().setEnabled(true))));
  }

  @Test
  public void testGetSuccessExistingSettings() throws Exception {
    SettingsService mockService = initSettingsService(new ApplicationsSettings().setEnabled(false));
    UpdateApplicationsSettingsResolver resolver =
        new UpdateApplicationsSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_ENABLED);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1))
        .updateGlobalSettings(
            any(),
            Mockito.eq(
                new GlobalSettingsInfo()
                    .setApplications(new ApplicationsSettings().setEnabled(true))));
  }

  @Test
  public void testGetSuccessDisableApplications() throws Exception {
    SettingsService mockService = initSettingsService(new ApplicationsSettings().setEnabled(true));
    UpdateApplicationsSettingsResolver resolver =
        new UpdateApplicationsSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_DISABLED);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1))
        .updateGlobalSettings(
            any(),
            Mockito.eq(
                new GlobalSettingsInfo()
                    .setApplications(new ApplicationsSettings().setEnabled(false))));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    SettingsService mockService = initSettingsService(new ApplicationsSettings().setEnabled(false));
    UpdateApplicationsSettingsResolver resolver =
        new UpdateApplicationsSettingsResolver(mockService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_ENABLED);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private static SettingsService initSettingsService(
      ApplicationsSettings existingApplicationsSettings) {
    SettingsService mockService = Mockito.mock(SettingsService.class);

    Mockito.when(mockService.getGlobalSettings(any()))
        .thenReturn(
            new GlobalSettingsInfo()
                .setApplications(existingApplicationsSettings, SetMode.IGNORE_NULL));

    return mockService;
  }
}

package com.linkedin.datahub.graphql.resolvers.settings;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateOrganizationDisplayPreferencesInput;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.GlobalVisualSettings;
import com.linkedin.settings.global.HelpLink;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpdateOrganizationDisplayPreferencesResolverTest {
  private static final UpdateOrganizationDisplayPreferencesInput TEST_INPUT =
      new UpdateOrganizationDisplayPreferencesInput("myCompany", "https://www.google.com");

  @Test
  public void testGetSuccessNoExistingSettings() throws Exception {
    SettingsService mockService = initSettingsService(null);
    UpdateOrganizationDisplayPreferencesResolver resolver =
        new UpdateOrganizationDisplayPreferencesResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1))
        .updateGlobalSettings(
            any(OperationContext.class),
            Mockito.eq(
                new GlobalSettingsInfo()
                    .setVisual(
                        new GlobalVisualSettings()
                            .setCustomOrgName("myCompany")
                            .setCustomLogoUrl("https://www.google.com"))));
  }

  @Test
  public void testGetSuccessWithSettings() throws Exception {
    SettingsService mockService =
        initSettingsService(
            new GlobalVisualSettings()
                .setCustomOrgName("notMyCompany")
                .setCustomLogoUrl("https://www.goooooogle.com")
                .setHelpLink(
                    new HelpLink()
                        .setIsEnabled(true)
                        .setLabel("testing")
                        .setLink("https://www.google.com")));
    UpdateOrganizationDisplayPreferencesResolver resolver =
        new UpdateOrganizationDisplayPreferencesResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1))
        .updateGlobalSettings(
            any(OperationContext.class),
            Mockito.eq(
                new GlobalSettingsInfo()
                    .setVisual(
                        new GlobalVisualSettings()
                            .setCustomLogoUrl("https://www.google.com")
                            .setCustomOrgName("myCompany")
                            .setHelpLink(
                                new HelpLink()
                                    .setIsEnabled(true)
                                    .setLabel("testing")
                                    .setLink("https://www.google.com")))));
  }

  @Test
  public void testGetException() throws Exception {
    SettingsService mockService = Mockito.mock(SettingsService.class);
    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .getGlobalSettings(any(OperationContext.class));

    UpdateOrganizationDisplayPreferencesResolver resolver =
        new UpdateOrganizationDisplayPreferencesResolver(mockService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    SettingsService mockService = initSettingsService(null);
    UpdateOrganizationDisplayPreferencesResolver resolver =
        new UpdateOrganizationDisplayPreferencesResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private static SettingsService initSettingsService(GlobalVisualSettings existingVisualSettings) {
    SettingsService mockService = Mockito.mock(SettingsService.class);

    Mockito.when(mockService.getGlobalSettings(any(OperationContext.class)))
        .thenReturn(
            new GlobalSettingsInfo().setVisual(existingVisualSettings, SetMode.IGNORE_NULL));

    return mockService;
  }
}

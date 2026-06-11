package com.linkedin.datahub.graphql.resolvers.settings.user;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateCorpUserLocaleSettingsInput;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserLocaleSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpdateCorpUserLocaleSettingsResolverTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final UpdateCorpUserLocaleSettingsInput TEST_INPUT_EN =
      new UpdateCorpUserLocaleSettingsInput("en");
  private static final UpdateCorpUserLocaleSettingsInput TEST_INPUT_NULL =
      new UpdateCorpUserLocaleSettingsInput(null);

  @Test
  public void testSetLanguageNoExistingSettings() throws Exception {
    SettingsService mockService = initSettingsService(TEST_USER_URN, null);
    UpdateCorpUserLocaleSettingsResolver resolver =
        new UpdateCorpUserLocaleSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_EN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1))
        .updateCorpUserSettings(
            any(),
            Mockito.eq(TEST_USER_URN),
            Mockito.eq(
                new CorpUserSettings()
                    .setAppearance(
                        new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false))
                    .setLocale(new CorpUserLocaleSettings().setLanguage("en"))));
  }

  @Test
  public void testSetLanguageExistingSettings() throws Exception {
    SettingsService mockService =
        initSettingsService(
            TEST_USER_URN,
            new CorpUserSettings()
                .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true))
                .setLocale(new CorpUserLocaleSettings().setLanguage("de")));
    UpdateCorpUserLocaleSettingsResolver resolver =
        new UpdateCorpUserLocaleSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_EN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1))
        .updateCorpUserSettings(
            any(),
            Mockito.eq(TEST_USER_URN),
            Mockito.eq(
                new CorpUserSettings()
                    .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true))
                    .setLocale(new CorpUserLocaleSettings().setLanguage("en"))));
  }

  @Test
  public void testClearLanguage() throws Exception {
    SettingsService mockService =
        initSettingsService(
            TEST_USER_URN,
            new CorpUserSettings()
                .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true))
                .setLocale(new CorpUserLocaleSettings().setLanguage("en")));
    UpdateCorpUserLocaleSettingsResolver resolver =
        new UpdateCorpUserLocaleSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_NULL);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1))
        .updateCorpUserSettings(
            any(),
            Mockito.eq(TEST_USER_URN),
            Mockito.eq(
                new CorpUserSettings()
                    .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true))
                    .setLocale(new CorpUserLocaleSettings())));
  }

  @Test
  public void testGetCorpUserSettingsException() throws Exception {
    SettingsService mockService = Mockito.mock(SettingsService.class);
    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .getCorpUserSettings(any(), Mockito.eq(TEST_USER_URN));

    UpdateCorpUserLocaleSettingsResolver resolver =
        new UpdateCorpUserLocaleSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_EN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testUpdateCorpUserSettingsException() throws Exception {
    SettingsService mockService = initSettingsService(TEST_USER_URN, null);
    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .updateCorpUserSettings(any(), Mockito.eq(TEST_USER_URN), Mockito.any());

    UpdateCorpUserLocaleSettingsResolver resolver =
        new UpdateCorpUserLocaleSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_EN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private static SettingsService initSettingsService(Urn user, CorpUserSettings existingSettings) {
    SettingsService mockService = Mockito.mock(SettingsService.class);
    Mockito.when(mockService.getCorpUserSettings(any(), Mockito.eq(user)))
        .thenReturn(existingSettings);
    return mockService;
  }
}

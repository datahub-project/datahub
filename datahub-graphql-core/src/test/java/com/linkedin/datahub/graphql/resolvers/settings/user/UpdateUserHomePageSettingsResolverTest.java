package com.linkedin.datahub.graphql.resolvers.settings.user;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateUserHomePageSettingsInput;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserHomePageSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetchingEnvironment;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpdateUserHomePageSettingsResolverTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final UpdateUserHomePageSettingsInput TEST_INPUT =
      new UpdateUserHomePageSettingsInput();
  private static final UpdateUserHomePageSettingsInput TEST_INPUT_WITH_TEMPLATE =
      new UpdateUserHomePageSettingsInput();
  private static final UpdateUserHomePageSettingsInput TEST_INPUT_NO_DISMISSED =
      new UpdateUserHomePageSettingsInput();

  static {
    TEST_INPUT.setNewDismissedAnnouncements(Arrays.asList("urn:li:post:a1", "urn:li:post:a2"));
    TEST_INPUT_WITH_TEMPLATE.setPageTemplate("urn:li:pageTemplate:homepage");
    TEST_INPUT_WITH_TEMPLATE.setNewDismissedAnnouncements(
        Collections.singletonList("urn:li:post:a3"));
  }

  @Test
  public void testUpdateSettingsWithNoExistingSettings() throws Exception {
    SettingsService mockService = initSettingsService(TEST_USER_URN, null);
    UpdateUserHomePageSettingsResolver resolver =
        new UpdateUserHomePageSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    CorpUserSettings expectedSettings =
        new CorpUserSettings()
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false))
            .setHomePage(
                new CorpUserHomePageSettings()
                    .setDismissedAnnouncements(
                        new UrnArray(
                            Arrays.asList(
                                Urn.createFromString("urn:li:post:a1"),
                                Urn.createFromString("urn:li:post:a2")))));

    Mockito.verify(mockService, Mockito.times(1))
        .updateCorpUserSettings(any(), Mockito.eq(TEST_USER_URN), Mockito.eq(expectedSettings));
  }

  @Test
  public void testUpdateSettingsWithExistingSettingsAndAnnouncements() throws Exception {
    CorpUserHomePageSettings existingHomePage =
        new CorpUserHomePageSettings()
            .setDismissedAnnouncements(
                new UrnArray(Collections.singletonList(Urn.createFromString("urn:li:post:a1"))));
    CorpUserSettings existingSettings =
        new CorpUserSettings()
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true))
            .setHomePage(existingHomePage);

    SettingsService mockService = initSettingsService(TEST_USER_URN, existingSettings);
    UpdateUserHomePageSettingsResolver resolver =
        new UpdateUserHomePageSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    CorpUserSettings expectedSettings =
        new CorpUserSettings()
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true))
            .setHomePage(
                new CorpUserHomePageSettings()
                    .setDismissedAnnouncements(
                        new UrnArray(
                            Arrays.asList(
                                Urn.createFromString("urn:li:post:a1"),
                                Urn.createFromString("urn:li:post:a2")))));

    Mockito.verify(mockService, Mockito.times(1))
        .updateCorpUserSettings(any(), Mockito.eq(TEST_USER_URN), Mockito.eq(expectedSettings));
  }

  @Test
  public void testUpdateSettingsWithPageTemplate() throws Exception {
    SettingsService mockService = initSettingsService(TEST_USER_URN, null);
    UpdateUserHomePageSettingsResolver resolver =
        new UpdateUserHomePageSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_WITH_TEMPLATE);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    CorpUserSettings expectedSettings =
        new CorpUserSettings()
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false))
            .setHomePage(
                new CorpUserHomePageSettings()
                    .setPageTemplate(UrnUtils.getUrn("urn:li:pageTemplate:homepage"))
                    .setDismissedAnnouncements(
                        new UrnArray(
                            Collections.singletonList(Urn.createFromString("urn:li:post:a3")))));

    Mockito.verify(mockService, Mockito.times(1))
        .updateCorpUserSettings(any(), Mockito.eq(TEST_USER_URN), Mockito.eq(expectedSettings));
  }

  @Test
  public void testUpdateSettingsWithNoDismissedAnnouncements() throws Exception {
    SettingsService mockService = initSettingsService(TEST_USER_URN, null);
    UpdateUserHomePageSettingsResolver resolver =
        new UpdateUserHomePageSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_NO_DISMISSED);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    CorpUserSettings expectedSettings =
        new CorpUserSettings()
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false))
            .setHomePage(new CorpUserHomePageSettings());

    Mockito.verify(mockService, Mockito.times(1))
        .updateCorpUserSettings(any(), Mockito.eq(TEST_USER_URN), Mockito.eq(expectedSettings));
  }

  @Test
  public void testGetCorpUserSettingsException() throws Exception {
    SettingsService mockService = Mockito.mock(SettingsService.class);
    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .getCorpUserSettings(any(), Mockito.eq(TEST_USER_URN));

    UpdateUserHomePageSettingsResolver resolver =
        new UpdateUserHomePageSettingsResolver(mockService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testUpdateCorpUserSettingsException() throws Exception {
    SettingsService mockService = initSettingsService(TEST_USER_URN, null);
    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .updateCorpUserSettings(
            any(), Mockito.eq(TEST_USER_URN), Mockito.any(CorpUserSettings.class));

    UpdateUserHomePageSettingsResolver resolver =
        new UpdateUserHomePageSettingsResolver(mockService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
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

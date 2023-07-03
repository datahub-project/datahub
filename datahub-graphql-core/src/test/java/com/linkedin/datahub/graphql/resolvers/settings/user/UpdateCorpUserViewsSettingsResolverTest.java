package com.linkedin.datahub.graphql.resolvers.settings.user;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateCorpUserViewsSettingsInput;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.identity.CorpUserViewsSettings;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;


public class UpdateCorpUserViewsSettingsResolverTest {

  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:dataHubView:test-id");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final UpdateCorpUserViewsSettingsInput TEST_VIEWS_INPUT = new UpdateCorpUserViewsSettingsInput(
      TEST_URN.toString()
  );
  private static final UpdateCorpUserViewsSettingsInput TEST_VIEWS_INPUT_NULL = new UpdateCorpUserViewsSettingsInput(
      null
  );

  @Test
  public void testGetSuccessViewSettingsNoExistingSettings() throws Exception {
    SettingsService mockService = initSettingsService(
        TEST_USER_URN,
        new CorpUserSettings()
          .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true))
    );
    UpdateCorpUserViewsSettingsResolver resolver = new UpdateCorpUserViewsSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_VIEWS_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1)).updateCorpUserSettings(
        Mockito.eq(TEST_USER_URN),
        Mockito.eq(new CorpUserSettings()
          .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true))
          .setViews(new CorpUserViewsSettings().setDefaultView(TEST_URN))),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetSuccessViewSettingsExistingSettings() throws Exception {
    SettingsService mockService = initSettingsService(
        TEST_USER_URN,
        new CorpUserSettings()
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true))
            .setViews(new CorpUserViewsSettings().setDefaultView(UrnUtils.getUrn(
                "urn:li:dataHubView:otherView"
            )))
    );
    UpdateCorpUserViewsSettingsResolver resolver = new UpdateCorpUserViewsSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_VIEWS_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1)).updateCorpUserSettings(
        Mockito.eq(TEST_USER_URN),
        Mockito.eq(new CorpUserSettings()
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true))
            .setViews(new CorpUserViewsSettings().setDefaultView(TEST_URN))),
        Mockito.any(Authentication.class));
  }


  @Test
  public void testGetSuccessViewSettingsRemoveDefaultView() throws Exception {
    SettingsService mockService = initSettingsService(
        TEST_USER_URN,
        new CorpUserSettings()
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true))
            .setViews(new CorpUserViewsSettings().setDefaultView(UrnUtils.getUrn(
                "urn:li:dataHubView:otherView"
            )))
    );
    UpdateCorpUserViewsSettingsResolver resolver = new UpdateCorpUserViewsSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_VIEWS_INPUT_NULL);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1)).updateCorpUserSettings(
        Mockito.eq(TEST_USER_URN),
        Mockito.eq(new CorpUserSettings()
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true))
            .setViews(new CorpUserViewsSettings().setDefaultView(null, SetMode.IGNORE_NULL))),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetCorpUserSettingsException() throws Exception {
    SettingsService mockService = Mockito.mock(SettingsService.class);
    Mockito.doThrow(RuntimeException.class).when(mockService).getCorpUserSettings(
        Mockito.eq(TEST_USER_URN),
        Mockito.any(Authentication.class));

    UpdateCorpUserViewsSettingsResolver resolver = new UpdateCorpUserViewsSettingsResolver(mockService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_VIEWS_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }


  @Test
  public void testUpdateCorpUserSettingsException() throws Exception {
    SettingsService mockService = initSettingsService(
        TEST_USER_URN,
        null
    );
    Mockito.doThrow(RuntimeException.class).when(mockService).updateCorpUserSettings(
        Mockito.eq(TEST_USER_URN),
        Mockito.any(CorpUserSettings.class),
        Mockito.any(Authentication.class));

    UpdateCorpUserViewsSettingsResolver resolver = new UpdateCorpUserViewsSettingsResolver(mockService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_VIEWS_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private static SettingsService initSettingsService(
      Urn user,
      CorpUserSettings existingSettings
  ) {
    SettingsService mockService = Mockito.mock(SettingsService.class);

    Mockito.when(mockService.getCorpUserSettings(
        Mockito.eq(user),
        Mockito.any(Authentication.class)))
        .thenReturn(existingSettings);

    return mockService;
  }
}
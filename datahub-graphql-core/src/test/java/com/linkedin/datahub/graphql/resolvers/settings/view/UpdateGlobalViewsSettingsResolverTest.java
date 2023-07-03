package com.linkedin.datahub.graphql.resolvers.settings.view;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateGlobalViewsSettingsInput;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.GlobalViewsSettings;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;


public class UpdateGlobalViewsSettingsResolverTest {

  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:dataHubView:test-id");
  private static final UpdateGlobalViewsSettingsInput TEST_INPUT = new UpdateGlobalViewsSettingsInput(
      TEST_URN.toString()
  );

  @Test
  public void testGetSuccessNoExistingSettings() throws Exception {
    SettingsService mockService = initSettingsService(
        null
    );
    UpdateGlobalViewsSettingsResolver resolver = new UpdateGlobalViewsSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1)).updateGlobalSettings(
        Mockito.eq(new GlobalSettingsInfo().setViews(new GlobalViewsSettings().setDefaultView(TEST_URN))),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetSuccessNoDefaultView() throws Exception {
    SettingsService mockService = initSettingsService(
        new GlobalViewsSettings()
    );
    UpdateGlobalViewsSettingsResolver resolver = new UpdateGlobalViewsSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1)).updateGlobalSettings(
        Mockito.eq(new GlobalSettingsInfo().setViews(new GlobalViewsSettings().setDefaultView(TEST_URN))),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetSuccessExistingDefaultView() throws Exception {
    SettingsService mockService = initSettingsService(
        new GlobalViewsSettings().setDefaultView(UrnUtils.getUrn(
            "urn:li:dataHubView:otherView"
        ))
    );
    UpdateGlobalViewsSettingsResolver resolver = new UpdateGlobalViewsSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1)).updateGlobalSettings(
        Mockito.eq(new GlobalSettingsInfo().setViews(new GlobalViewsSettings().setDefaultView(TEST_URN))),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetGlobalViewsSettingsException() throws Exception {
    SettingsService mockService = Mockito.mock(SettingsService.class);
    Mockito.doThrow(RuntimeException.class).when(mockService).getGlobalSettings(
        Mockito.any(Authentication.class));

    UpdateGlobalViewsSettingsResolver resolver = new UpdateGlobalViewsSettingsResolver(mockService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }


  @Test
  public void testUpdateGlobalViewsSettingsException() throws Exception {
    SettingsService mockService = initSettingsService(
        new GlobalViewsSettings()
    );
    Mockito.doThrow(RuntimeException.class).when(mockService).updateGlobalSettings(
        Mockito.any(GlobalSettingsInfo.class),
        Mockito.any(Authentication.class));

    UpdateGlobalViewsSettingsResolver resolver = new UpdateGlobalViewsSettingsResolver(mockService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetGlobalViewsSettingsNoSettingsException() throws Exception {
    SettingsService mockService = initSettingsService(
        null // Should never be null.
    );
    Mockito.doThrow(RuntimeException.class).when(mockService).getGlobalSettings(
        Mockito.any(Authentication.class));

    UpdateGlobalViewsSettingsResolver resolver = new UpdateGlobalViewsSettingsResolver(mockService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    SettingsService mockService = initSettingsService(
        new GlobalViewsSettings()
    );
    UpdateGlobalViewsSettingsResolver resolver = new UpdateGlobalViewsSettingsResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private static SettingsService initSettingsService(
      GlobalViewsSettings existingViewSettings
  ) {
    SettingsService mockService = Mockito.mock(SettingsService.class);

    Mockito.when(mockService.getGlobalSettings(
        Mockito.any(Authentication.class)))
      .thenReturn(new GlobalSettingsInfo().setViews(existingViewSettings, SetMode.IGNORE_NULL));

    return mockService;
  }
}
package com.linkedin.datahub.graphql.resolvers.settings.view;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.GlobalViewsSettings;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GlobalViewsSettingsResolverTest {

  private static final Urn TEST_URN = UrnUtils.getUrn("urn:li:dataHubView:test-id");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  @Test
  public void testGetSuccessNullSettings() throws Exception {
    SettingsService mockService = initSettingsService(null);
    GlobalViewsSettingsResolver resolver = new GlobalViewsSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.GlobalViewsSettings result = resolver.get(mockEnv).get();

    Assert.assertNotNull(result); // Empty settings
    Assert.assertNull(result.getDefaultView());
  }

  @Test
  public void testGetSuccessEmptySettings() throws Exception {
    SettingsService mockService = initSettingsService(new GlobalViewsSettings());
    GlobalViewsSettingsResolver resolver = new GlobalViewsSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.GlobalViewsSettings result = resolver.get(mockEnv).get();

    Assert.assertNull(result.getDefaultView());
  }

  @Test
  public void testGetSuccessExistingSettings() throws Exception {
    SettingsService mockService =
        initSettingsService(new GlobalViewsSettings().setDefaultView(TEST_URN));
    GlobalViewsSettingsResolver resolver = new GlobalViewsSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.GlobalViewsSettings result = resolver.get(mockEnv).get();

    Assert.assertEquals(result.getDefaultView(), TEST_URN.toString());
  }

  @Test
  public void testGetException() throws Exception {
    SettingsService mockService = Mockito.mock(SettingsService.class);
    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .getGlobalSettings(Mockito.any(Authentication.class));

    GlobalViewsSettingsResolver resolver = new GlobalViewsSettingsResolver(mockService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    SettingsService mockService = initSettingsService(new GlobalViewsSettings());
    UpdateGlobalViewsSettingsResolver resolver = new UpdateGlobalViewsSettingsResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext(TEST_USER_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private static SettingsService initSettingsService(GlobalViewsSettings existingViewSettings) {
    SettingsService mockService = Mockito.mock(SettingsService.class);

    Mockito.when(mockService.getGlobalSettings(Mockito.any(Authentication.class)))
        .thenReturn(new GlobalSettingsInfo().setViews(existingViewSettings, SetMode.IGNORE_NULL));

    return mockService;
  }
}

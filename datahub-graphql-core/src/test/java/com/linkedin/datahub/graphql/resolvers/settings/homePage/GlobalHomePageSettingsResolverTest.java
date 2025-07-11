package com.linkedin.datahub.graphql.resolvers.settings.homePage;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubPageTemplate;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.GlobalHomePageSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GlobalHomePageSettingsResolverTest {

  private static final Urn TEST_TEMPLATE_URN =
      UrnUtils.getUrn("urn:li:dataHubPageTemplate:test-template");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  @Test
  public void testGetSuccessNullSettings() throws Exception {
    SettingsService mockService = initSettingsService(null);
    GlobalHomePageSettingsResolver resolver = new GlobalHomePageSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.GlobalHomePageSettings result =
        resolver.get(mockEnv).get();

    Assert.assertNotNull(result); // Empty settings
    Assert.assertNull(result.getDefaultTemplate());
  }

  @Test
  public void testGetSuccessEmptySettings() throws Exception {
    SettingsService mockService = initSettingsService(new GlobalHomePageSettings());
    GlobalHomePageSettingsResolver resolver = new GlobalHomePageSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.GlobalHomePageSettings result =
        resolver.get(mockEnv).get();

    Assert.assertNull(result.getDefaultTemplate());
  }

  @Test
  public void testGetSuccessExistingSettings() throws Exception {
    SettingsService mockService =
        initSettingsService(new GlobalHomePageSettings().setDefaultTemplate(TEST_TEMPLATE_URN));
    GlobalHomePageSettingsResolver resolver = new GlobalHomePageSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.GlobalHomePageSettings result =
        resolver.get(mockEnv).get();

    Assert.assertNotNull(result.getDefaultTemplate());
    DataHubPageTemplate template = result.getDefaultTemplate();
    Assert.assertEquals(template.getUrn(), TEST_TEMPLATE_URN.toString());
    Assert.assertEquals(template.getType(), EntityType.DATAHUB_PAGE_TEMPLATE);
  }

  @Test
  public void testGetSuccessSettingsWithoutDefaultTemplate() throws Exception {
    SettingsService mockService = initSettingsService(new GlobalHomePageSettings());
    GlobalHomePageSettingsResolver resolver = new GlobalHomePageSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    com.linkedin.datahub.graphql.generated.GlobalHomePageSettings result =
        resolver.get(mockEnv).get();

    Assert.assertNull(result.getDefaultTemplate());
  }

  @Test
  public void testGetException() throws Exception {
    SettingsService mockService = Mockito.mock(SettingsService.class);
    Mockito.doThrow(RuntimeException.class).when(mockService).getGlobalSettings(any());

    GlobalHomePageSettingsResolver resolver = new GlobalHomePageSettingsResolver(mockService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private static SettingsService initSettingsService(
      GlobalHomePageSettings existingHomePageSettings) {
    SettingsService mockService = Mockito.mock(SettingsService.class);

    Mockito.when(mockService.getGlobalSettings(any()))
        .thenReturn(
            new GlobalSettingsInfo().setHomePage(existingHomePageSettings, SetMode.IGNORE_NULL));

    return mockService;
  }
}

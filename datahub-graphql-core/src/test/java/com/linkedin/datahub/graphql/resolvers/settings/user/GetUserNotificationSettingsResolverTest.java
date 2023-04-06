package com.linkedin.datahub.graphql.resolvers.settings.user;

import com.datahub.authentication.Authentication;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.resolvers.settings.NotificationSettingsMatcher;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetchingEnvironment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.settings.NotificationSettingsTestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class GetUserNotificationSettingsResolverTest {
  private SettingsService _settingsService;
  private GetUserNotificationSettingsResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() throws Exception {
    _settingsService = mock(SettingsService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    final QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);

    _resolver = new GetUserNotificationSettingsResolver(_settingsService);
  }

  @Test
  public void testExceptionThrown() {
    when(_settingsService.getCorpUserSettings(eq(USER_URN), eq(_authentication))).thenThrow(new RuntimeException());

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testGetUserNotificationSettingPasses() throws Exception {
    when(_settingsService.getCorpUserSettings(eq(USER_URN), eq(_authentication))).thenReturn(CORP_USER_SETTINGS);

    final NotificationSettings result = _resolver.get(_dataFetchingEnvironment).join();
    final NotificationSettingsMatcher matcher = new NotificationSettingsMatcher(getMappedUserNotificationSettings());
    assertTrue(matcher.matches(result));
  }
}

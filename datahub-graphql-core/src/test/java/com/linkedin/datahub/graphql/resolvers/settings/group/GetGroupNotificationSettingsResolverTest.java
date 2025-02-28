package com.linkedin.datahub.graphql.resolvers.settings.group;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.settings.NotificationSettingsTestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GetGroupNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.resolvers.settings.NotificationSettingsMatcher;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GetGroupNotificationSettingsResolverTest {
  private SettingsService _settingsService;
  private GetGroupNotificationSettingsResolver _resolver;
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

    final GetGroupNotificationSettingsInput input = new GetGroupNotificationSettingsInput();
    input.setGroupUrn(GROUP_URN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    _resolver = new GetGroupNotificationSettingsResolver(_settingsService);
  }

  @Test
  public void testExceptionThrown() {
    when(_settingsService.getCorpGroupSettings(any(OperationContext.class), eq(GROUP_URN)))
        .thenThrow(new RuntimeException());

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testGetGroupNotificationSettingPasses() throws Exception {
    when(_settingsService.getCorpGroupSettings(any(OperationContext.class), eq(GROUP_URN)))
        .thenReturn(CORP_GROUP_SETTINGS);

    final NotificationSettings result = _resolver.get(_dataFetchingEnvironment).join();
    final NotificationSettingsMatcher matcher =
        new NotificationSettingsMatcher(getMappedGroupNotificationSettings());
    assertTrue(matcher.matches(result));
  }
}

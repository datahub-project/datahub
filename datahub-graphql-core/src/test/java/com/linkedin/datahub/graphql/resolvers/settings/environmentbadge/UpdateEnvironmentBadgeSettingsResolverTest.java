package com.linkedin.datahub.graphql.resolvers.settings.environmentbadge;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.ApplicationsSettings;
import com.linkedin.settings.global.EnvironmentBadgeSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.testng.annotations.Test;

public class UpdateEnvironmentBadgeSettingsResolverTest {

  @Test
  public void testGetSuccessAuthorized() throws Exception {
    SettingsService mockService = mock(SettingsService.class);
    when(mockService.getGlobalSettings(any())).thenReturn(new GlobalSettingsInfo());
    UpdateEnvironmentBadgeSettingsResolver resolver =
        new UpdateEnvironmentBadgeSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getArgument("input")).thenReturn(ImmutableMap.of("enabled", true));
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
    verify(mockService, times(1))
        .updateGlobalSettings(
            any(),
            eq(
                new GlobalSettingsInfo()
                    .setEnvironmentBadge(new EnvironmentBadgeSettings().setEnabled(true))));
  }

  @Test
  public void testGetSuccessPreservesExistingSettings() throws Exception {
    SettingsService mockService = mock(SettingsService.class);
    when(mockService.getGlobalSettings(any()))
        .thenReturn(
            new GlobalSettingsInfo()
                .setApplications(new ApplicationsSettings().setEnabled(true))
                .setEnvironmentBadge(new EnvironmentBadgeSettings().setEnabled(false)));
    UpdateEnvironmentBadgeSettingsResolver resolver =
        new UpdateEnvironmentBadgeSettingsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getArgument("input")).thenReturn(ImmutableMap.of("enabled", true));
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
    verify(mockService, times(1))
        .updateGlobalSettings(
            any(),
            eq(
                new GlobalSettingsInfo()
                    .setApplications(new ApplicationsSettings().setEnabled(true))
                    .setEnvironmentBadge(new EnvironmentBadgeSettings().setEnabled(true))));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    SettingsService mockService = mock(SettingsService.class);
    UpdateEnvironmentBadgeSettingsResolver resolver =
        new UpdateEnvironmentBadgeSettingsResolver(mockService);

    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getArgument("input")).thenReturn(ImmutableMap.of("enabled", true));
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verify(mockService, never()).updateGlobalSettings(any(), any());
  }
}

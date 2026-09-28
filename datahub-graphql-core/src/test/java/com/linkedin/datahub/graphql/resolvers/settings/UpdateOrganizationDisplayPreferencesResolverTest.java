package com.linkedin.datahub.graphql.resolvers.settings;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.ApplicationsSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.GlobalVisualSettings;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import org.testng.annotations.Test;

public class UpdateOrganizationDisplayPreferencesResolverTest {

  @Test
  public void testGetSuccessAuthorized() throws Exception {
    SettingsService mockService = mock(SettingsService.class);
    when(mockService.getGlobalSettings(any())).thenReturn(new GlobalSettingsInfo());
    UpdateOrganizationDisplayPreferencesResolver resolver =
        new UpdateOrganizationDisplayPreferencesResolver(mockService);

    DataFetchingEnvironment mockEnv =
        mockEnv(getMockAllowContext(), ImmutableMap.of("showEnvironmentBadge", true));

    assertTrue(resolver.get(mockEnv).get());
    verify(mockService, times(1))
        .updateGlobalSettings(
            any(),
            eq(
                new GlobalSettingsInfo()
                    .setVisual(new GlobalVisualSettings().setShowEnvironmentBadge(true))));
  }

  @Test
  public void testGetSuccessPreservesExistingSettings() throws Exception {
    SettingsService mockService = mock(SettingsService.class);
    when(mockService.getGlobalSettings(any()))
        .thenReturn(
            new GlobalSettingsInfo()
                .setApplications(new ApplicationsSettings().setEnabled(true))
                .setVisual(new GlobalVisualSettings().setShowEnvironmentBadge(false)));
    UpdateOrganizationDisplayPreferencesResolver resolver =
        new UpdateOrganizationDisplayPreferencesResolver(mockService);

    DataFetchingEnvironment mockEnv =
        mockEnv(getMockAllowContext(), ImmutableMap.of("showEnvironmentBadge", true));

    assertTrue(resolver.get(mockEnv).get());
    verify(mockService, times(1))
        .updateGlobalSettings(
            any(),
            eq(
                new GlobalSettingsInfo()
                    .setApplications(new ApplicationsSettings().setEnabled(true))
                    .setVisual(new GlobalVisualSettings().setShowEnvironmentBadge(true))));
  }

  @Test
  public void testGetOmittedFieldLeavesValueUnchanged() throws Exception {
    SettingsService mockService = mock(SettingsService.class);
    when(mockService.getGlobalSettings(any()))
        .thenReturn(
            new GlobalSettingsInfo()
                .setVisual(new GlobalVisualSettings().setShowEnvironmentBadge(true)));
    UpdateOrganizationDisplayPreferencesResolver resolver =
        new UpdateOrganizationDisplayPreferencesResolver(mockService);

    DataFetchingEnvironment mockEnv = mockEnv(getMockAllowContext(), Collections.emptyMap());

    assertTrue(resolver.get(mockEnv).get());
    verify(mockService, times(1))
        .updateGlobalSettings(
            any(),
            eq(
                new GlobalSettingsInfo()
                    .setVisual(new GlobalVisualSettings().setShowEnvironmentBadge(true))));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    SettingsService mockService = mock(SettingsService.class);
    UpdateOrganizationDisplayPreferencesResolver resolver =
        new UpdateOrganizationDisplayPreferencesResolver(mockService);

    DataFetchingEnvironment mockEnv =
        mockEnv(getMockDenyContext(), ImmutableMap.of("showEnvironmentBadge", true));

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verify(mockService, never()).updateGlobalSettings(any(), any());
  }

  private static DataFetchingEnvironment mockEnv(QueryContext context, Object input) {
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(context);
    return mockEnv;
  }
}

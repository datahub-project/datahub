package com.linkedin.datahub.graphql.resolvers.settings;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GlobalSettings;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.GlobalVisualSettings;
import graphql.schema.DataFetchingEnvironment;
import org.testng.annotations.Test;

public class GlobalSettingsResolverTest {

  @Test
  public void testMapsVisualSettingsForUnprivilegedUser() throws Exception {
    // Every user needs the display preferences to render the UI, so reads are not
    // privilege-gated.
    SettingsService mockService = mock(SettingsService.class);
    when(mockService.getGlobalSettings(any()))
        .thenReturn(
            new GlobalSettingsInfo()
                .setVisual(new GlobalVisualSettings().setShowEnvironmentBadge(true)));

    GlobalSettings result = new GlobalSettingsResolver(mockService).get(mockEnv()).get();

    assertTrue(result.getVisualSettings().getShowEnvironmentBadge());
  }

  @Test
  public void testNoVisualSettings() throws Exception {
    SettingsService mockService = mock(SettingsService.class);
    when(mockService.getGlobalSettings(any())).thenReturn(new GlobalSettingsInfo());

    GlobalSettings result = new GlobalSettingsResolver(mockService).get(mockEnv()).get();

    assertNull(result.getVisualSettings());
  }

  private static DataFetchingEnvironment mockEnv() {
    QueryContext context = getMockDenyContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(context);
    return mockEnv;
  }
}

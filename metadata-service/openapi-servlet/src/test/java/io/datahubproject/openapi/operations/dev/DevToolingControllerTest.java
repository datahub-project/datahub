package io.datahubproject.openapi.operations.dev;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DevToolingControllerTest {

  private MockMvc mockMvc;

  @BeforeMethod
  public void setup() {
    FeatureFlags featureFlags = new FeatureFlags();
    ConfigurationProvider configProvider = mock(ConfigurationProvider.class);
    when(configProvider.getFeatureFlags()).thenReturn(featureFlags);
    mockMvc = MockMvcBuilders.standaloneSetup(new DevToolingController(configProvider)).build();
  }

  @Test
  public void testGetAllFeatureFlags_returnsAllFlags() throws Exception {
    mockMvc
        .perform(get("/openapi/operations/dev/featureFlags"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.showBrowseV2").exists())
        .andExpect(jsonPath("$.entityVersioning").exists())
        .andExpect(jsonPath("$.readOnlyModeEnabled").exists());
  }

  @Test
  public void testGetSpecificFlag_returnsValue() throws Exception {
    mockMvc
        .perform(get("/openapi/operations/dev/featureFlags/showBrowseV2"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.showBrowseV2").value(false));
  }

  @Test
  public void testGetUnknownFlag_returns404WithError() throws Exception {
    mockMvc
        .perform(get("/openapi/operations/dev/featureFlags/doesNotExist"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.error").exists());
  }
}

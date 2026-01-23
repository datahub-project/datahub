package io.datahubproject.openapi.operations.maintenance;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.DataMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.MaintenanceSeverity;
import com.linkedin.settings.global.MaintenanceWindowSettings;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.openapi.operations.maintenance.models.EnableMaintenanceRequest;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest(classes = MaintenanceControllerTest.TestConfig.class)
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class MaintenanceControllerTest extends AbstractTestNGSpringContextTests {

  private static final String BASE_PATH = "/openapi/operations/maintenance";

  @Autowired private MaintenanceController maintenanceController;
  @Autowired private MockMvc mockMvc;

  @Autowired
  @Qualifier("systemEntityClient")
  private EntityClient entityClient;

  @Autowired private AuthorizerChain authorizerChain;
  @Autowired private ObjectMapper objectMapper;

  @BeforeMethod
  public void setupMocks() {
    // Reset mocks
    reset(entityClient);

    // Setup authentication
    Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "urn:li:corpuser:admin"));
    AuthenticationContext.setAuthentication(authentication);

    // Setup authorization - default to ALLOW
    when(authorizerChain.authorize(any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
  }

  @Test
  public void testControllerInit() {
    assertNotNull(maintenanceController);
  }

  // ==================== Status Tests ====================

  @Test
  public void testGetStatusWhenDisabled() throws Exception {
    GlobalSettingsInfo settings = new GlobalSettingsInfo();
    MaintenanceWindowSettings maintenanceSettings = new MaintenanceWindowSettings();
    maintenanceSettings.setEnabled(false);
    settings.setMaintenanceWindow(maintenanceSettings);

    mockGetGlobalSettings(settings);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/status").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.enabled").value(false))
        .andExpect(jsonPath("$.message").doesNotExist());
  }

  @Test
  public void testGetStatusWhenEnabled() throws Exception {
    GlobalSettingsInfo settings = new GlobalSettingsInfo();
    MaintenanceWindowSettings maintenanceSettings = new MaintenanceWindowSettings();
    maintenanceSettings.setEnabled(true);
    maintenanceSettings.setMessage("Scheduled maintenance in progress");
    maintenanceSettings.setSeverity(MaintenanceSeverity.WARNING);
    maintenanceSettings.setLinkUrl("https://status.example.com");
    maintenanceSettings.setLinkText("Status page");
    maintenanceSettings.setEnabledAt(1234567890L);
    maintenanceSettings.setEnabledBy("urn:li:corpuser:admin");
    settings.setMaintenanceWindow(maintenanceSettings);

    mockGetGlobalSettings(settings);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/status").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.enabled").value(true))
        .andExpect(jsonPath("$.message").value("Scheduled maintenance in progress"))
        .andExpect(jsonPath("$.severity").value("WARNING"))
        .andExpect(jsonPath("$.linkUrl").value("https://status.example.com"))
        .andExpect(jsonPath("$.linkText").value("Status page"))
        .andExpect(jsonPath("$.enabledAt").value(1234567890L))
        .andExpect(jsonPath("$.enabledBy").value("urn:li:corpuser:admin"));
  }

  @Test
  public void testGetStatusNoMaintenanceSettings() throws Exception {
    GlobalSettingsInfo settings = new GlobalSettingsInfo();
    // No maintenance window settings set

    mockGetGlobalSettings(settings);

    mockMvc
        .perform(
            MockMvcRequestBuilders.get(BASE_PATH + "/status").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.enabled").value(false));
  }

  // ==================== Enable Tests ====================

  @Test
  public void testEnableMaintenanceSuccess() throws Exception {
    GlobalSettingsInfo settings = new GlobalSettingsInfo();
    mockGetGlobalSettings(settings);

    EnableMaintenanceRequest request = new EnableMaintenanceRequest();
    request.setMessage("Scheduled maintenance");
    request.setSeverity(EnableMaintenanceRequest.MaintenanceSeverityDto.WARNING);
    request.setLinkUrl("https://status.example.com");

    mockMvc
        .perform(
            MockMvcRequestBuilders.post(BASE_PATH + "/enable")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.enabled").value(true))
        .andExpect(jsonPath("$.message").value("Scheduled maintenance"))
        .andExpect(jsonPath("$.severity").value("WARNING"))
        .andExpect(jsonPath("$.linkUrl").value("https://status.example.com"));

    verify(entityClient).ingestProposal(any(), any(), eq(false));
  }

  @Test
  public void testEnableMaintenanceAllSeverities() throws Exception {
    for (EnableMaintenanceRequest.MaintenanceSeverityDto severity :
        EnableMaintenanceRequest.MaintenanceSeverityDto.values()) {
      reset(entityClient);
      GlobalSettingsInfo settings = new GlobalSettingsInfo();
      mockGetGlobalSettings(settings);

      EnableMaintenanceRequest request = new EnableMaintenanceRequest();
      request.setMessage("Test message for " + severity);
      request.setSeverity(severity);

      mockMvc
          .perform(
              MockMvcRequestBuilders.post(BASE_PATH + "/enable")
                  .contentType(MediaType.APPLICATION_JSON)
                  .content(objectMapper.writeValueAsString(request))
                  .accept(MediaType.APPLICATION_JSON))
          .andExpect(status().isOk())
          .andExpect(jsonPath("$.severity").value(severity.name()));
    }
  }

  // ==================== Disable Tests ====================

  @Test
  public void testDisableMaintenanceSuccess() throws Exception {
    GlobalSettingsInfo settings = new GlobalSettingsInfo();
    MaintenanceWindowSettings maintenanceSettings = new MaintenanceWindowSettings();
    maintenanceSettings.setEnabled(true);
    maintenanceSettings.setMessage("Previous message");
    settings.setMaintenanceWindow(maintenanceSettings);

    mockGetGlobalSettings(settings);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post(BASE_PATH + "/disable").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.message").value("Maintenance mode disabled"));

    verify(entityClient).ingestProposal(any(), any(), eq(false));
  }

  // ==================== Helper Methods ====================

  private void mockGetGlobalSettings(GlobalSettingsInfo settings) throws Exception {
    DataMap dataMap = settings.data();
    Aspect aspect = new Aspect(dataMap);
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(aspect);

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(GLOBAL_SETTINGS_INFO_ASPECT_NAME, envelopedAspect);

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setEntityName(GLOBAL_SETTINGS_ENTITY_NAME);
    entityResponse.setUrn(GLOBAL_SETTINGS_URN);
    entityResponse.setAspects(aspectMap);

    when(entityClient.getV2(
            any(OperationContext.class),
            eq(GLOBAL_SETTINGS_ENTITY_NAME),
            eq(GLOBAL_SETTINGS_URN),
            eq(Set.of(GLOBAL_SETTINGS_INFO_ASPECT_NAME))))
        .thenReturn(entityResponse);
  }

  // ==================== Test Configuration ====================

  @SpringBootConfiguration
  @Import({TestMaintenanceConfig.class, TracingInterceptor.class})
  @ComponentScan(basePackages = {"io.datahubproject.openapi.operations.maintenance"})
  static class TestConfig {}

  @TestConfiguration
  public static class TestMaintenanceConfig {

    @Bean(name = "systemEntityClient")
    @Primary
    public EntityClient systemEntityClient() {
      return mock(EntityClient.class);
    }

    @Bean
    public ObjectMapper objectMapper() {
      return new ObjectMapper();
    }

    @Bean
    @Primary
    public SystemTelemetryContext systemTelemetryContext() {
      return mock(SystemTelemetryContext.class);
    }

    @Bean(name = "systemOperationContext")
    public OperationContext systemOperationContext(
        ObjectMapper objectMapper, SystemTelemetryContext systemTelemetryContext) {
      return TestOperationContexts.systemContextTraceNoSearchAuthorization(
          () -> ObjectMapperContext.builder().objectMapper(objectMapper).build(),
          () -> systemTelemetryContext);
    }

    @Bean
    @Primary
    public AuthorizerChain authorizerChain() {
      AuthorizerChain authorizerChain = mock(AuthorizerChain.class);
      Authentication authentication = mock(Authentication.class);
      when(authentication.getActor())
          .thenReturn(new Actor(ActorType.USER, "urn:li:corpuser:admin"));
      when(authorizerChain.authorize(any()))
          .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
      AuthenticationContext.setAuthentication(authentication);
      return authorizerChain;
    }
  }
}

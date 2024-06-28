package com.datahub.auth.authentication;

import static com.linkedin.metadata.Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_SETTINGS_URN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.OidcSettings;
import com.linkedin.settings.global.SsoSettings;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.servlet.DispatcherServlet;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@SpringBootTest(classes = {DispatcherServlet.class})
@ComponentScan(basePackages = {"com.datahub.auth.authentication"})
@Import({AuthServiceTestConfiguration.class})
public class AuthServiceControllerTest extends AbstractTestNGSpringContextTests {
  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Autowired private AuthServiceController authServiceController;
  @Autowired private EntityService mockEntityService;

  private final String PREFERRED_JWS_ALGORITHM = "preferredJwsAlgorithm";

  @Test
  public void initTest() {
    assertNotNull(authServiceController);
    assertNotNull(mockEntityService);
  }

  @Test
  public void oldPreferredJwsAlgorithmIsNotReturned() throws IOException {
    OidcSettings mockOidcSettings =
        new OidcSettings()
            .setEnabled(true)
            .setClientId("1")
            .setClientSecret("2")
            .setDiscoveryUri("http://localhost")
            .setPreferredJwsAlgorithm("test");
    SsoSettings mockSsoSettings =
        new SsoSettings().setBaseUrl("http://localhost").setOidcSettings(mockOidcSettings);
    GlobalSettingsInfo mockGlobalSettingsInfo = new GlobalSettingsInfo().setSso(mockSsoSettings);

    when(mockEntityService.getLatestAspect(
            any(OperationContext.class),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(mockGlobalSettingsInfo);

    ResponseEntity<String> httpResponse = authServiceController.getSsoSettings(null).join();
    assertEquals(httpResponse.getStatusCode(), HttpStatus.OK);

    JsonNode jsonNode = new ObjectMapper().readTree(httpResponse.getBody());
    assertFalse(jsonNode.has(PREFERRED_JWS_ALGORITHM));
  }

  @Test
  public void newPreferredJwsAlgorithmIsReturned() throws IOException {
    OidcSettings mockOidcSettings =
        new OidcSettings()
            .setEnabled(true)
            .setClientId("1")
            .setClientSecret("2")
            .setDiscoveryUri("http://localhost")
            .setPreferredJwsAlgorithm("jws1")
            .setPreferredJwsAlgorithm2("jws2");
    SsoSettings mockSsoSettings =
        new SsoSettings().setBaseUrl("http://localhost").setOidcSettings(mockOidcSettings);
    GlobalSettingsInfo mockGlobalSettingsInfo = new GlobalSettingsInfo().setSso(mockSsoSettings);

    when(mockEntityService.getLatestAspect(
            any(OperationContext.class),
            eq(GLOBAL_SETTINGS_URN),
            eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME)))
        .thenReturn(mockGlobalSettingsInfo);

    ResponseEntity<String> httpResponse = authServiceController.getSsoSettings(null).join();
    assertEquals(httpResponse.getStatusCode(), HttpStatus.OK);

    JsonNode jsonNode = new ObjectMapper().readTree(httpResponse.getBody());
    assertTrue(jsonNode.has(PREFERRED_JWS_ALGORITHM));
    assertEquals(jsonNode.get(PREFERRED_JWS_ALGORITHM).asText(), "jws2");
  }
}

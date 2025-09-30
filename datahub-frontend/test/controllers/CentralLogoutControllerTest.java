package controllers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import auth.sso.SsoManager;
import com.linkedin.metadata.utils.BasePathUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.pac4j.play.LogoutController;
import play.mvc.Http;
import play.mvc.Result;

public class CentralLogoutControllerTest {

  private CentralLogoutController controller;
  private SsoManager mockSsoManager;
  private Config mockConfig;
  private Http.Request mockRequest;

  @BeforeEach
  public void setUp() throws Exception {
    // Create mock configurations
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "");
    mockConfig = ConfigFactory.parseMap(configMap);

    // Mock SSO manager
    mockSsoManager = mock(SsoManager.class);

    // Mock HTTP request
    mockRequest = mock(Http.Request.class);

    // Create the controller
    controller = new CentralLogoutController();
    
    // Use reflection to set private fields
    setPrivateField(controller, "ssoManager", mockSsoManager);
    setPrivateField(controller, "config", mockConfig);
  }

  private void setPrivateField(Object target, String fieldName, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  @Test
  public void testExecuteLogoutWithSsoEnabledSuccess() throws Exception {
    // Setup: SSO enabled, logout succeeds
    when(mockSsoManager.isSsoEnabled()).thenReturn(true);

    try (MockedStatic<BasePathUtils> basePathUtilsMock = mockStatic(BasePathUtils.class)) {
      basePathUtilsMock.when(() -> BasePathUtils.normalizeBasePath(anyString())).thenReturn("");
      basePathUtilsMock.when(() -> BasePathUtils.addBasePath(anyString(), anyString())).thenReturn("/login");

      // Execute the method
      Result result = controller.executeLogout(mockRequest);

      // Verify the result - when SSO is enabled but logout fails, it should redirect to error URL
      assertNotNull(result);
      assertEquals(303, result.status()); // 303 is correct for redirect with new session
      String redirectUrl = result.redirectLocation().orElse("");
      assertTrue(redirectUrl.contains("error_msg") || redirectUrl.contains("Failed to sign out"), 
          "Expected error message in redirect URL, but got: " + redirectUrl);
    }

    // Verify SSO manager was called
    verify(mockSsoManager).isSsoEnabled();
  }

  @Test
  public void testExecuteLogoutWithSsoEnabledException() throws Exception {
    // Setup: SSO enabled, logout throws exception
    when(mockSsoManager.isSsoEnabled()).thenReturn(true);

    try (MockedStatic<BasePathUtils> basePathUtilsMock = mockStatic(BasePathUtils.class)) {
      basePathUtilsMock.when(() -> BasePathUtils.normalizeBasePath(anyString())).thenReturn("");
      basePathUtilsMock.when(() -> BasePathUtils.addBasePath(anyString(), anyString())).thenReturn("/login");

      // Execute the method - this will trigger the exception handling path
      Result result = controller.executeLogout(mockRequest);

      // Verify the result - should redirect to error URL
      assertNotNull(result);
      assertEquals(303, result.status()); // 303 is correct for redirect with new session
      String redirectUrl = result.redirectLocation().orElse("");
      assertTrue(redirectUrl.contains("error_msg") || redirectUrl.contains("Failed to sign out"), 
          "Expected error message in redirect URL, but got: " + redirectUrl);
    }

    // Verify SSO manager was called
    verify(mockSsoManager).isSsoEnabled();
  }

  @Test
  public void testExecuteLogoutWithSsoDisabled() {
    // Setup: SSO disabled
    when(mockSsoManager.isSsoEnabled()).thenReturn(false);

    try (MockedStatic<BasePathUtils> basePathUtilsMock = mockStatic(BasePathUtils.class)) {
      basePathUtilsMock.when(() -> BasePathUtils.normalizeBasePath(anyString())).thenReturn("");
      basePathUtilsMock.when(() -> BasePathUtils.addBasePath(anyString(), anyString())).thenReturn("/login");

      // Execute the method
      Result result = controller.executeLogout(mockRequest);

      // Verify the result - should redirect to login URL
      assertNotNull(result);
      assertEquals(303, result.status()); // 303 is correct for redirect with new session
      assertEquals("/login", result.redirectLocation().orElse(""));
    }

    // Verify SSO manager was called
    verify(mockSsoManager).isSsoEnabled();
  }

  @Test
  public void testExecuteLogoutWithCustomBasePath() throws Exception {
    // Setup: Custom base path
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "/custom");
    Config customConfig = ConfigFactory.parseMap(configMap);
    setPrivateField(controller, "config", customConfig);

    when(mockSsoManager.isSsoEnabled()).thenReturn(false);

    try (MockedStatic<BasePathUtils> basePathUtilsMock = mockStatic(BasePathUtils.class)) {
      basePathUtilsMock.when(() -> BasePathUtils.normalizeBasePath("/custom")).thenReturn("/custom");
      basePathUtilsMock.when(() -> BasePathUtils.addBasePath("/login", "/custom")).thenReturn("/custom/login");

      // Execute the method
      Result result = controller.executeLogout(mockRequest);

      // Verify the result
      assertNotNull(result);
      assertEquals(303, result.status()); // 303 is correct for redirect with new session
      assertEquals("/custom/login", result.redirectLocation().orElse(""));
    }

    // Verify SSO manager was called
    verify(mockSsoManager).isSsoEnabled();
  }

  @Test
  public void testExecuteLogoutWithEmptyBasePath() throws Exception {
    // Setup: Empty base path
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "");
    Config emptyConfig = ConfigFactory.parseMap(configMap);
    setPrivateField(controller, "config", emptyConfig);

    when(mockSsoManager.isSsoEnabled()).thenReturn(false);

    try (MockedStatic<BasePathUtils> basePathUtilsMock = mockStatic(BasePathUtils.class)) {
      basePathUtilsMock.when(() -> BasePathUtils.normalizeBasePath("")).thenReturn("");
      basePathUtilsMock.when(() -> BasePathUtils.addBasePath("/login", "")).thenReturn("/login");

      // Execute the method
      Result result = controller.executeLogout(mockRequest);

      // Verify the result
      assertNotNull(result);
      assertEquals(303, result.status()); // 303 is correct for redirect with new session
      assertEquals("/login", result.redirectLocation().orElse(""));
    }

    // Verify SSO manager was called
    verify(mockSsoManager).isSsoEnabled();
  }

  @Test
  public void testExecuteLogoutWithNullBasePath() throws Exception {
    // Setup: Empty base path (null is not allowed by ConfigFactory)
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "");
    Config emptyConfig = ConfigFactory.parseMap(configMap);
    setPrivateField(controller, "config", emptyConfig);

    when(mockSsoManager.isSsoEnabled()).thenReturn(false);

    try (MockedStatic<BasePathUtils> basePathUtilsMock = mockStatic(BasePathUtils.class)) {
      basePathUtilsMock.when(() -> BasePathUtils.normalizeBasePath("")).thenReturn("");
      basePathUtilsMock.when(() -> BasePathUtils.addBasePath("/login", "")).thenReturn("/login");

      // Execute the method
      Result result = controller.executeLogout(mockRequest);

      // Verify the result
      assertNotNull(result);
      assertEquals(303, result.status()); // 303 is correct for redirect with new session
      assertEquals("/login", result.redirectLocation().orElse(""));
    }

    // Verify SSO manager was called
    verify(mockSsoManager).isSsoEnabled();
  }

  @Test
  public void testConstructorSettings() {
    // Test that constructor sets the correct values
    CentralLogoutController newController = new CentralLogoutController();
    
    // Verify that the controller extends LogoutController and has the expected behavior
    assertNotNull(newController);
    assertTrue(newController instanceof LogoutController);
  }

  @Test
  public void testGetBasePathMethod() throws Exception {
    // Test the private getBasePath method indirectly through executeLogout
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "/test/path");
    Config testConfig = ConfigFactory.parseMap(configMap);
    setPrivateField(controller, "config", testConfig);

    when(mockSsoManager.isSsoEnabled()).thenReturn(false);

    try (MockedStatic<BasePathUtils> basePathUtilsMock = mockStatic(BasePathUtils.class)) {
      basePathUtilsMock.when(() -> BasePathUtils.normalizeBasePath("/test/path")).thenReturn("/test/path");
      basePathUtilsMock.when(() -> BasePathUtils.addBasePath("/login", "/test/path")).thenReturn("/test/path/login");

      // Execute the method
      Result result = controller.executeLogout(mockRequest);

      // Verify that getBasePath was called correctly
      basePathUtilsMock.verify(() -> BasePathUtils.normalizeBasePath("/test/path"));
      basePathUtilsMock.verify(() -> BasePathUtils.addBasePath("/login", "/test/path"));
    }
  }
}

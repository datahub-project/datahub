package io.datahubproject.openapi.v1.timeline;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.testng.Assert.assertNotNull;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.config.GlobalControllerExceptionHandler;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureWebMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.testng.annotations.Test;

@SpringBootTest(
    classes = TimelineControllerV1Test.TestConfig.class,
    properties = "authorization.restApiAuthorization=false")
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class TimelineControllerV1Test extends AbstractTestNGSpringContextTests {

  private static final Urn TEST_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:testPlatform,timelineTest,PROD)");

  @MockitoBean private TimelineService mockTimelineService;

  @Autowired private TimelineControllerV1 timelineController;

  @Autowired private MockMvc mockMvc;

  @Test
  public void initTest() {
    assertNotNull(timelineController);
  }

  @Test
  public void testGetTimelinePreferredPath() throws Exception {
    List<ChangeTransaction> timeline =
        Collections.singletonList(
            ChangeTransaction.builder().timestamp(1L).actor("datahub").build());
    when(mockTimelineService.getTimeline(
            any(OperationContext.class),
            eq(TEST_URN),
            anySet(),
            anyLong(),
            anyLong(),
            eq(null),
            eq(null),
            anyBoolean()))
        .thenReturn(timeline);

    try (MockedStatic<AuthUtil> authUtilMock = Mockito.mockStatic(AuthUtil.class)) {
      authUtilMock
          .when(() -> AuthUtil.canViewEntity(any(OperationContext.class), eq(TEST_URN)))
          .thenReturn(true);

      mockMvc
          .perform(
              MockMvcRequestBuilders.get("/openapi/v1/timeline/{urn}", TEST_URN)
                  .param("categories", "TECHNICAL_SCHEMA")
                  .accept(MediaType.APPLICATION_JSON))
          .andExpect(status().isOk())
          .andExpect(jsonPath("$[0].timestamp").value(1))
          .andExpect(jsonPath("$[0].actor").value("datahub"));
    }
  }

  @Test
  public void testGetTimelineLegacyPath() throws Exception {
    List<ChangeTransaction> timeline =
        Collections.singletonList(
            ChangeTransaction.builder().timestamp(2L).actor("legacy").build());
    when(mockTimelineService.getTimeline(
            any(OperationContext.class),
            eq(TEST_URN),
            anySet(),
            anyLong(),
            anyLong(),
            eq(null),
            eq(null),
            anyBoolean()))
        .thenReturn(timeline);

    try (MockedStatic<AuthUtil> authUtilMock = Mockito.mockStatic(AuthUtil.class)) {
      authUtilMock
          .when(() -> AuthUtil.canViewEntity(any(OperationContext.class), eq(TEST_URN)))
          .thenReturn(true);

      mockMvc
          .perform(
              MockMvcRequestBuilders.get("/openapi/timeline/v1/{urn}", TEST_URN)
                  .param("categories", "TECHNICAL_SCHEMA")
                  .accept(MediaType.APPLICATION_JSON))
          .andExpect(status().isOk())
          .andExpect(jsonPath("$[0].timestamp").value(2))
          .andExpect(jsonPath("$[0].actor").value("legacy"));
    }
  }

  @SpringBootConfiguration
  @Import({TimelineControllerV1TestConfig.class, GlobalControllerExceptionHandler.class})
  static class TestConfig {}

  @TestConfiguration
  public static class TimelineControllerV1TestConfig {

    @Bean
    public ObjectMapper objectMapper() {
      return new ObjectMapper();
    }

    @Bean(name = "systemOperationContext")
    public OperationContext systemOperationContext() {
      return TestOperationContexts.systemContextNoSearchAuthorization();
    }

    @Bean
    @Primary
    public AuthorizerChain authorizerChain() {
      AuthorizerChain authorizerChain = mock(AuthorizerChain.class);

      Authentication authentication = mock(Authentication.class);
      when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
      when(authorizerChain.authorize(any()))
          .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
      AuthenticationContext.setAuthentication(authentication);

      return authorizerChain;
    }

    @Bean
    public TimelineControllerV1 timelineControllerV1(
        @org.springframework.beans.factory.annotation.Qualifier("systemOperationContext")
            OperationContext systemOperationContext,
        TimelineService timelineService,
        AuthorizerChain authorizerChain) {
      return new TimelineControllerV1(
          systemOperationContext, timelineService, authorizerChain, false);
    }
  }
}

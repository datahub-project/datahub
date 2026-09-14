package io.datahubproject.openapi.operations.messaging;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
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
import com.linkedin.gms.factory.kafka.common.TopicConventionFactory;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.messaging.ConsumerGroupLagSnapshot;
import com.linkedin.metadata.messaging.ConsumerLagPort;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.mxe.TopicConvention;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import org.mockito.MockedStatic;
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
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest(
    classes = MessagingOperationsControllerTest.WebTestConfig.class,
    properties = "datahub.messaging.transport=kafka")
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class MessagingOperationsControllerTest extends AbstractTestNGSpringContextTests {

  @Autowired private MockMvc mockMvc;

  @Autowired private MessagingOperationsController controller;

  @Autowired private ConsumerLagPort consumerLagPort;

  @MockitoBean private MetadataQueueStore metadataQueueStore;

  @Autowired private TopicConvention topicConvention;

  @Autowired private AuthorizerChain authorizerChain;

  @BeforeMethod
  public void setupAuth() {
    Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
    AuthenticationContext.setAuthentication(authentication);
    when(authorizerChain.authorize(any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
    when(consumerLagPort.transport()).thenReturn("kafka");
    when(consumerLagPort.mcpLag(anyBoolean(), anyBoolean()))
        .thenReturn(
            ConsumerGroupLagSnapshot.builder()
                .consumerGroupId("mcp")
                .topics(Collections.emptyMap())
                .build());
    when(topicConvention.getMetadataChangeProposalTopicName())
        .thenReturn("MetadataChangeProposal_v1");
    when(topicConvention.getMetadataChangeLogVersionedTopicName())
        .thenReturn("MetadataChangeLog_Versioned_v1");
    when(topicConvention.getMetadataChangeLogTimeseriesTopicName())
        .thenReturn("MetadataChangeLog_Timeseries_v1");
  }

  @Test
  public void controllerBeanPresent() {
    assertNotNull(controller);
  }

  @Test
  public void getTransport_returnsKafkaTopics() throws Exception {
    mockMvc
        .perform(get("/openapi/operations/messaging/transport").accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.transport").value("kafka"))
        .andExpect(jsonPath("$.metadataChangeProposalTopic").value("MetadataChangeProposal_v1"));
  }

  @Test
  public void mcpLag_returnsEnvelope() throws Exception {
    mockMvc
        .perform(
            get("/openapi/operations/messaging/mcp/consumer/lag")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.transport").value("kafka"));
  }

  @Test
  public void mcpLag_viewSystemStatusAllowsGet() throws Exception {
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class)) {
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAPIOperationsAuthorized(
                      any(OperationContext.class), eq(PoliciesConfig.VIEW_SYSTEM_STATUS_PRIVILEGE)))
          .thenReturn(true);

      mockMvc
          .perform(
              get("/openapi/operations/messaging/mcp/consumer/lag")
                  .accept(MediaType.APPLICATION_JSON))
          .andExpect(status().isOk());
    }
  }

  @Test
  public void mcpLag_noPrivilegeReturnsForbidden() throws Exception {
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class)) {
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAPIOperationsAuthorized(
                      any(OperationContext.class), eq(PoliciesConfig.VIEW_SYSTEM_STATUS_PRIVILEGE)))
          .thenReturn(false);

      mockMvc
          .perform(
              get("/openapi/operations/messaging/mcp/consumer/lag")
                  .accept(MediaType.APPLICATION_JSON))
          .andExpect(status().isForbidden());
    }
  }

  @Test
  public void registerConsumer_viewSystemStatusAloneIsForbidden() throws Exception {
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class)) {
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAPIOperationsAuthorized(
                      any(OperationContext.class), eq(PoliciesConfig.VIEW_SYSTEM_STATUS_PRIVILEGE)))
          .thenReturn(true);
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAPIAuthorized(
                      any(OperationContext.class),
                      eq(PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)))
          .thenReturn(false);

      mockMvc
          .perform(
              put("/openapi/operations/messaging/consumers")
                  .contentType(MediaType.APPLICATION_JSON)
                  .content("{\"topicName\":\"MetadataChangeProposal_v1\",\"consumerGroup\":\"g\"}"))
          .andExpect(status().isForbidden());
    }
  }

  @SpringBootConfiguration
  @EnableWebMvc
  @Import({
    MessagingOperationsController.class,
    MessagingOperationsControllerTest.SharedBeans.class,
    MessagingOperationsControllerTest.WebMvcBeans.class
  })
  static class WebTestConfig {}

  @TestConfiguration
  static class WebMvcBeans {

    @Bean
    MockMvc mockMvc(org.springframework.web.context.WebApplicationContext context) {
      return org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup(context)
          .build();
    }
  }

  static class SharedBeans {

    @Bean(name = "systemOperationContext")
    OperationContext systemOperationContext() {
      ObjectMapper objectMapper = new ObjectMapper();
      SystemTelemetryContext systemTelemetryContext = mock(SystemTelemetryContext.class);
      return TestOperationContexts.systemContextTraceNoSearchAuthorization(
          () -> ObjectMapperContext.builder().objectMapper(objectMapper).build(),
          () -> systemTelemetryContext);
    }

    @Bean
    @Primary
    AuthorizerChain authorizerChain() {
      return mock(AuthorizerChain.class);
    }

    @Bean(name = TopicConventionFactory.TOPIC_CONVENTION_BEAN)
    TopicConvention topicConvention() {
      return mock(TopicConvention.class);
    }

    @Bean
    @Primary
    ConsumerLagPort consumerLagPort() {
      return mock(ConsumerLagPort.class);
    }
  }
}

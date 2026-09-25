package io.datahubproject.openapi.operations.messaging;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.messaging.MessagingTransport;
import com.linkedin.metadata.queue.ConsumerOffsetResetDetail;
import com.linkedin.metadata.queue.ConsumerOffsetResetReport;
import com.linkedin.metadata.queue.ConsumerOffsetResetSpec;
import com.linkedin.metadata.queue.MetadataQueueStore;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
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
    classes = PgQueueMessagingOperationsControllerTest.WebTestConfig.class,
    properties = "datahub.messaging.transport=pgqueue")
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class PgQueueMessagingOperationsControllerTest extends AbstractTestNGSpringContextTests {

  private static final String RESET_PATH = "/openapi/operations/messaging/consumer/offsets/reset";

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withUserConfiguration(
              PgQueueMessagingOperationsController.class,
              PgQueueMessagingOperationsControllerTest.SharedBeans.class);

  @Autowired private MockMvc mockMvc;

  @Autowired private PgQueueMessagingOperationsController controller;

  @MockitoBean private MetadataQueueStore metadataQueueStore;

  @Autowired private AuthorizerChain authorizerChain;

  @BeforeMethod
  public void setupAuth() {
    Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
    AuthenticationContext.setAuthentication(authentication);
    when(authorizerChain.authorize(any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
  }

  @Test
  public void pgQueueTransport_registersController() {
    contextRunner
        .withPropertyValues(MessagingTransport.PROPERTY + "=" + MessagingTransport.PGQUEUE)
        .run(
            context ->
                assertFalse(
                    context.getBeansOfType(PgQueueMessagingOperationsController.class).isEmpty()));
  }

  @Test
  public void kafkaTransport_doesNotRegisterController() {
    contextRunner
        .withPropertyValues(MessagingTransport.PROPERTY + "=" + MessagingTransport.KAFKA)
        .run(
            context ->
                assertTrue(
                    context.getBeansOfType(PgQueueMessagingOperationsController.class).isEmpty()));
  }

  @Test
  public void resetEndpoint_notMappedWhenKafkaTransport() throws Exception {
    new WebApplicationContextRunner()
        .withUserConfiguration(WebTestConfig.class)
        .withPropertyValues(MessagingTransport.PROPERTY + "=" + MessagingTransport.KAFKA)
        .run(
            context -> {
              assertTrue(
                  context.getBeansOfType(PgQueueMessagingOperationsController.class).isEmpty());
              MockMvc kafkaMockMvc = context.getBean(MockMvc.class);
              kafkaMockMvc
                  .perform(
                      post(RESET_PATH)
                          .contentType(MediaType.APPLICATION_JSON)
                          .content("{\"onlyStuckAhead\":true}"))
                  .andExpect(status().isNotFound());
            });
  }

  @Test
  public void controllerBeanPresentUnderPgQueueTransport() {
    assertNotNull(controller);
  }

  @Test
  public void resetConsumerOffsets_success() throws Exception {
    ConsumerOffsetResetReport report =
        ConsumerOffsetResetReport.builder()
            .partitionsUpdated(1)
            .resets(
                List.of(
                    ConsumerOffsetResetDetail.builder()
                        .consumerGroup("generic-mae-consumer-job-client")
                        .topicName("MetadataChangeLog_Versioned_v1")
                        .partitionId(0)
                        .previousOffset(1604L)
                        .newOffset(10L)
                        .maxSeq(10L)
                        .build()))
            .build();
    when(metadataQueueStore.resetConsumerOffsets(
            ArgumentMatchers.any(ConsumerOffsetResetSpec.class)))
        .thenReturn(report);

    mockMvc
        .perform(
            post(RESET_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"onlyStuckAhead\":true}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.transport").value("pgqueue"))
        .andExpect(jsonPath("$.partitionsUpdated").value(1))
        .andExpect(jsonPath("$.resets[0].previousOffset").value(1604))
        .andExpect(jsonPath("$.resets[0].newOffset").value(10));
  }

  @Test
  public void resetConsumerOffsets_forbiddenWhenUnauthorized() throws Exception {
    try (MockedStatic<AuthUtil> authUtilMock = Mockito.mockStatic(AuthUtil.class)) {
      authUtilMock
          .when(() -> AuthUtil.isAPIAuthorized(any(OperationContext.class), any()))
          .thenReturn(false);

      mockMvc
          .perform(
              post(RESET_PATH)
                  .contentType(MediaType.APPLICATION_JSON)
                  .content("{\"onlyStuckAhead\":true}"))
          .andExpect(status().isForbidden());
    }
  }

  @Test
  public void resetConsumerOffsets_notFoundForUnknownTopic() throws Exception {
    when(metadataQueueStore.resetConsumerOffsets(
            ArgumentMatchers.any(ConsumerOffsetResetSpec.class)))
        .thenThrow(new IllegalArgumentException("Topic not found: missing-topic"));

    mockMvc
        .perform(
            post(RESET_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"topicName\":\"missing-topic\"}"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.error").value("Topic not found: missing-topic"));
  }

  @SpringBootConfiguration
  @EnableWebMvc
  @Import({
    PgQueueMessagingOperationsController.class,
    PgQueueMessagingOperationsControllerTest.SharedBeans.class,
    PgQueueMessagingOperationsControllerTest.WebMvcBeans.class
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
      AuthorizerChain authorizerChain = mock(AuthorizerChain.class);
      Authentication authentication = mock(Authentication.class);
      when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
      when(authorizerChain.authorize(any()))
          .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
      AuthenticationContext.setAuthentication(authentication);
      return authorizerChain;
    }

    @Bean
    MetadataQueueStore metadataQueueStore() {
      return mock(MetadataQueueStore.class);
    }
  }
}

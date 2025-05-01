package com.datahub.auth.authentication;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.invite.InviteTokenService;
import com.datahub.authentication.token.StatelessTokenService;
import com.datahub.authentication.user.NativeUserService;
import com.datahub.telemetry.TrackingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.TraceContext;
import io.datahubproject.metadata.services.SecretService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.Tracer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;

@TestConfiguration
public class AuthServiceTestConfiguration {

  public static String SYSTEM_CLIENT_ID = "systemClientId";
  @MockBean StatelessTokenService statelessTokenService;

  @MockBean(name = "configurationProvider")
  ConfigurationProvider configProvider;

  @MockBean NativeUserService nativeUserService;

  @MockBean EntityService entityService;

  @MockBean SecretService secretService;

  @MockBean InviteTokenService inviteTokenService;

  @MockBean TrackingService trackingService;

  @MockBean Tracer mockTracer;

  @MockBean SpanContext mockSpanContext;

  @Bean(name = "systemOperationContext")
  public OperationContext systemOperationContext(ObjectMapper objectMapper) {
    TraceContext mockTraceContext = TraceContext.builder().tracer(mockTracer).build();
    return TestOperationContexts.systemContextTraceNoSearchAuthorization(
        () -> ObjectMapperContext.builder().objectMapper(objectMapper).build(),
        () -> mockTraceContext);
  }

  @Bean
  public Authentication systemAuthentication() {
    final Actor systemActor = new Actor(ActorType.USER, SYSTEM_CLIENT_ID);
    return new Authentication(
        systemActor, String.format("Basic %s:%s", SYSTEM_CLIENT_ID, "systemSecret"));
  }
  @Bean
  public ObjectMapper objectMapper() {
    return new ObjectMapper();
  }
}

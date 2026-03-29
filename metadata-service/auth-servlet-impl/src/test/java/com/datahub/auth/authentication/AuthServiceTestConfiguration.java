package com.datahub.auth.authentication;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.invite.InviteTokenService;
import com.datahub.authentication.session.UserSessionEligibilityChecker;
import com.datahub.authentication.token.StatelessTokenService;
import com.datahub.authentication.user.NativeUserService;
import com.datahub.telemetry.TrackingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.metadata.services.SecretService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
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

  @MockBean UserSessionEligibilityChecker userSessionEligibilityChecker;

  @MockBean EntityService entityService;

  @MockBean SecretService secretService;

  @MockBean InviteTokenService inviteTokenService;

  @MockBean TrackingService trackingService;

  @MockBean SpanContext mockSpanContext;

  @Bean
  public Tracer noopTestTracer() {
    return OpenTelemetry.noop().getTracer("auth-servlet-impl-test");
  }

  @Bean(name = "systemOperationContext")
  public OperationContext systemOperationContext(ObjectMapper objectMapper, Tracer noopTestTracer) {
    SystemTelemetryContext mockSystemTelemetryContext =
        SystemTelemetryContext.builder().tracer(noopTestTracer).build();
    return TestOperationContexts.systemContextTraceNoSearchAuthorization(
        () -> ObjectMapperContext.builder().objectMapper(objectMapper).build(),
        () -> mockSystemTelemetryContext);
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

  @Bean
  public MetricUtils metricUtils() {
    return MetricUtils.builder().registry(new SimpleMeterRegistry()).build();
  }
}

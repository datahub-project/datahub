package com.datahub.auth.authentication;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.invite.InviteTokenService;
import com.datahub.authentication.token.StatelessTokenService;
import com.datahub.authentication.user.NativeUserService;
import com.datahub.telemetry.TrackingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;

@TestConfiguration
public class AuthServiceTestConfiguration {
  @MockBean StatelessTokenService _statelessTokenService;

  @MockBean Authentication _systemAuthentication;

  @MockBean(name = "systemOperationContext")
  OperationContext systemOperationContext;

  @MockBean(name = "configurationProvider")
  ConfigurationProvider _configProvider;

  @MockBean NativeUserService _nativeUserService;

  @MockBean EntityService _entityService;

  @MockBean SecretService _secretService;

  @MockBean InviteTokenService _inviteTokenService;

  @MockBean TrackingService _trackingService;

  @Bean
  public ObjectMapper objectMapper() {
    return new ObjectMapper();
  }
}

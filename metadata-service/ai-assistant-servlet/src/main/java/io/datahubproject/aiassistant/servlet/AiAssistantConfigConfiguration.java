package io.datahubproject.aiassistant.servlet;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.AiAssistantConfigPlatformService;
import com.linkedin.metadata.service.AiAssistantConfigService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AiAssistantConfigConfiguration {

  @Bean
  public AiAssistantConfigPlatformService aiAssistantConfigPlatformService(
      SystemEntityClient systemEntityClient,
      SecretService secretService,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
    return new AiAssistantConfigPlatformService(
        systemEntityClient, secretService, systemOperationContext);
  }

  @Bean
  public AiAssistantConfigService aiAssistantConfigService(
      AiAssistantConfigPlatformService platformService) {
    return new AiAssistantConfigService(platformService);
  }
}

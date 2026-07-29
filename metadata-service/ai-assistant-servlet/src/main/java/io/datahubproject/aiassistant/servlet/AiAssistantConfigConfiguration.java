package io.datahubproject.aiassistant.servlet;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.AiAssistantConfigPersistenceService;
import com.linkedin.metadata.service.AiAssistantConfigService;
import com.linkedin.metadata.service.SettingsService;
import io.datahubproject.metadata.services.SecretService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AiAssistantConfigConfiguration {

  @Bean
  public AiAssistantConfigPersistenceService aiAssistantConfigPersistenceService(
      SystemEntityClient systemEntityClient,
      SecretService secretService,
      SettingsService settingsService) {
    return new AiAssistantConfigPersistenceService(
        systemEntityClient, secretService, settingsService);
  }

  @Bean
  public AiAssistantConfigService aiAssistantConfigService(
      AiAssistantConfigPersistenceService persistenceService) {
    return new AiAssistantConfigService(persistenceService);
  }
}

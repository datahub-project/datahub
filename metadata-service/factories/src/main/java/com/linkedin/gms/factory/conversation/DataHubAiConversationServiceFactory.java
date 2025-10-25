package com.linkedin.gms.factory.conversation;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.DataHubAiConversationService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class DataHubAiConversationServiceFactory {

  @Bean(name = "dataHubAiConversationService")
  @Scope("singleton")
  @Nonnull
  protected DataHubAiConversationService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient) {
    return new DataHubAiConversationService(systemEntityClient);
  }
}

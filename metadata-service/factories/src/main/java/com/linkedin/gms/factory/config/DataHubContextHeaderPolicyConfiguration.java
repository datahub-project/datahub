package com.linkedin.gms.factory.config;

import com.linkedin.metadata.config.DataHubConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.request.DataHubContextRulesHolder;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DataHubContextHeaderPolicyConfiguration {

  public DataHubContextHeaderPolicyConfiguration(
      ConfigurationProvider configurationProvider,
      @Nonnull @Qualifier("systemOperationContext") OperationContext systemOperationContext) {
    @Nonnull
    DataHubConfiguration datahub =
        Objects.requireNonNull(
            configurationProvider.getDatahub(), "datahub configuration must be set");
    DataHubContextRulesHolder.setPolicy(
        DataHubContextPolicyFactory.from(
            Objects.requireNonNull(
                datahub.getRequestContext(), "datahub.requestContext must be set"),
            systemOperationContext.getObjectMapper()));
  }
}

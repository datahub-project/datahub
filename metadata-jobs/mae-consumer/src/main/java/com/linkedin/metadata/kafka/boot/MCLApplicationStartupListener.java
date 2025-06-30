package com.linkedin.metadata.kafka.boot;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.boot.BootstrapManager;
import com.linkedin.metadata.boot.kafka.DataHubUpgradeKafkaListener;
import com.linkedin.metadata.kafka.config.MetadataChangeLogProcessorCondition;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.context.WebApplicationContext;

/** Responsible for coordinating starting steps that happen before the application starts up. */
@Slf4j
@Component
@Conditional(MetadataChangeLogProcessorCondition.class)
public class MCLApplicationStartupListener implements ApplicationListener<ContextRefreshedEvent> {

  private static final String ROOT_WEB_APPLICATION_CONTEXT_ID =
      String.format("%s:", WebApplicationContext.class.getName());

  private final DataHubUpgradeKafkaListener _dataHubUpgradeKafkaListener;
  private final ConfigurationProvider _configurationProvider;
  private final BootstrapManager _mclBootstrapManager;

  @Autowired
  @Qualifier("systemOperationContext")
  OperationContext systemOperationContext;

  public MCLApplicationStartupListener(
      @Qualifier("dataHubUpgradeKafkaListener")
          DataHubUpgradeKafkaListener dataHubUpgradeKafkaListener,
      ConfigurationProvider configurationProvider,
      @Qualifier("mclBootstrapManager") BootstrapManager bootstrapManager) {
    _dataHubUpgradeKafkaListener = dataHubUpgradeKafkaListener;
    _configurationProvider = configurationProvider;
    _mclBootstrapManager = bootstrapManager;
  }

  @Override
  public void onApplicationEvent(@Nonnull ContextRefreshedEvent event) {
    if (ROOT_WEB_APPLICATION_CONTEXT_ID.equals(event.getApplicationContext().getId())
        && _configurationProvider.getSystemUpdate().isWaitForSystemUpdate()) {
      _mclBootstrapManager.start(systemOperationContext);
    }
  }
}

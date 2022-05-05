package com.linkedin.gms.factory.change;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.config.events.EntityChangeEventSinkConfiguration;
import com.linkedin.metadata.event.change.EntityChangeEventSink;
import com.linkedin.metadata.event.change.EntityChangeEventSinkConfig;
import com.linkedin.metadata.event.change.EntityChangeEventSinkManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Slf4j
@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class EntityChangeEventSinkManagerFactory {

  @Autowired
  private ConfigurationProvider configurationProvider;

  @Bean(name = "changeEventSinkManager")
  @Singleton
  @Nonnull
  protected EntityChangeEventSinkManager getInstance() {

    final List<EntityChangeEventSink> configuredSinks = new ArrayList<>();
    final List<EntityChangeEventSinkConfiguration> sinkConfigurations =
        this.configurationProvider.getEventSinks() != null && this.configurationProvider.getEventSinks().getEntityChangeEvent() != null
            ? this.configurationProvider.getEventSinks().getEntityChangeEvent().getSinks()
            : Collections.emptyList();
    for (EntityChangeEventSinkConfiguration sink : sinkConfigurations) {

      boolean isSinkEnabled = sink.isEnabled();

      if (isSinkEnabled) {
        final String type = sink.getType();
        final Map<String, Object> configs = sink.getConfigs() != null ? sink.getConfigs() : Collections.emptyMap();

        log.debug(String.format("Found configs for change event sink of type %s: %s ", type, configs));

        // Instantiate the Change event Sink.
        Class<? extends EntityChangeEventSink> clazz = null;
        try {
          clazz = (Class<? extends EntityChangeEventSink>) Class.forName(type);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(
              String.format("Failed to find ChangeEventSink class with name %s on the classpath.", type));
        }

        // Else construct an instance of the class, each class should have an empty constructor.
        try {
          final EntityChangeEventSink changeEventSink = clazz.newInstance();
          changeEventSink.init(new EntityChangeEventSinkConfig(
              configs
          ));
          configuredSinks.add(changeEventSink);
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to instantiate ChangeEventSink with class name %s", clazz.getCanonicalName()), e);
        }
      } else {
        log.info(String.format("Skipping disabled change event sink sink with type %s", sink.getType()));
      }
    }
    log.info(String.format("Creating ChangeEventSink. sinks: %s", configuredSinks));
    return new EntityChangeEventSinkManager(configuredSinks);
  }
}
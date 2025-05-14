package config;

import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authorization.AuthorizationConfiguration;
import com.linkedin.metadata.config.VisualConfiguration;
import com.linkedin.metadata.config.cache.CacheConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.PropertySource;

/**
 * Minimal sharing between metadata-service and frontend Does not use the factories module to avoid
 * transitive dependencies.
 */
@EnableConfigurationProperties
@PropertySource(value = "classpath:/application.yaml", factory = YamlPropertySourceFactory.class)
@ConfigurationProperties
@Data
public class ConfigurationProvider {
  /** Kafka related configs. */
  private KafkaConfiguration kafka;

  /** Configuration for caching */
  private CacheConfiguration cache;

  /** Configuration for the view layer */
  private VisualConfiguration visualConfig;

  /** Configuration for authorization */
  private AuthorizationConfiguration authorization;

  /** Configuration for authentication */
  private AuthenticationConfiguration authentication;
}

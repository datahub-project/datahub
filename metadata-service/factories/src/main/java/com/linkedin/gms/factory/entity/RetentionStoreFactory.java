package com.linkedin.gms.factory.entity;

import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.entity.RetentionStore;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.io.FileNotFoundException;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;


@Configuration
@Import({EntityRegistryFactory.class})
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class RetentionStoreFactory {

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Value("${retentionStore.path}")
  String retentionConfigPath;

  @Bean(name = "retentionStore")
  @Nonnull
  protected RetentionStore createInstance() throws FileNotFoundException {
    if (StringUtils.isEmpty(retentionConfigPath)) {
      return new RetentionStore(entityRegistry);
    }
    return new RetentionStore(entityRegistry, retentionConfigPath);
  }
}

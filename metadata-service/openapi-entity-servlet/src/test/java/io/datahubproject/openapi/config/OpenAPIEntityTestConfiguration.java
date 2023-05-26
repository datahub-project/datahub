package io.datahubproject.openapi.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.*;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;



@TestConfiguration
public class OpenAPIEntityTestConfiguration {
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper(new YAMLFactory());
    }

    @MockBean
    public EntityService entityService;

    @Bean("entityRegistry")
    @Primary
    protected EntityRegistry entityRegistry() throws EntityRegistryException {
        return new MergedEntityRegistry(SnapshotEntityRegistry.getInstance()).apply(configEntityRegistry());
    }

    private ConfigEntityRegistry configEntityRegistry() throws EntityRegistryException {
        return new ConfigEntityRegistry(
                OpenAPIEntityTestConfiguration.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
    }
}

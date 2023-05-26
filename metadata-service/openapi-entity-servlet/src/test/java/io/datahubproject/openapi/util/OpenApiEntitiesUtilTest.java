package io.datahubproject.openapi.util;

import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.openapi.config.OpenAPIEntityTestConfiguration;
import io.datahubproject.openapi.dto.UpsertAspectRequest;
import io.datahubproject.openapi.generated.ContainerEntityRequestV2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertEquals;


@Import({OpenAPIEntityTestConfiguration.class})
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class OpenApiEntitiesUtilTest extends AbstractTestNGSpringContextTests {
    @Autowired
    private EntityRegistry entityRegistry;

    @Test
    public void testInitialization() {
        assertNotNull(entityRegistry);
    }

    @Test
    public void containerConversionTest() {
        ContainerEntityRequestV2 test = ContainerEntityRequestV2.builder()
                .urn("urn:li:container:123")
                .build();
        List<UpsertAspectRequest> expected = List.of(UpsertAspectRequest.builder()
                .entityType("container")
                .entityUrn("urn:li:container:123")
                .build());

        assertEquals(expected, OpenApiEntitiesUtil.convertEntityToUpsert(test, ContainerEntityRequestV2.class, entityRegistry));
    }
}

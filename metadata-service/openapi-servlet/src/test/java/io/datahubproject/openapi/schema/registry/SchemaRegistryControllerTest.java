package io.datahubproject.openapi.schema.registry;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;


@TestPropertySource(locations = "classpath:/application.yml")
public class SchemaRegistryControllerTest extends AbstractTestNGSpringContextTests {

}
package com.linkedin.gms.factory.kafka.schemaregistry;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

@TestPropertySource(locations = "classpath:/application.yml")
@SpringBootTest(classes = {InternalSchemaRegistryFactory.class})
public class InternalSchemaRegistryFactoryTest extends AbstractTestNGSpringContextTests {

}
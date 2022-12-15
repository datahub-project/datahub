package io.datahubproject.openapi.test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;


@TestConfiguration
//@ComponentScan({"com.linkedin.gms.factory"})
public class OpenAPISpringTestServerConfiguration {

  @Autowired
  private TestRestTemplate restTemplate;
}

package io.datahubproject.openapi.test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@SpringBootApplication
@EnableKafka
public class OpenAPISpringTestServer {

  public static void main(String[] args) {
    SpringApplication.run(OpenAPISpringTestServer.class, args);
  }
}

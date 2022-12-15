package io.datahubproject.openapi.test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;


@SpringBootApplication
@EnableKafka
public class OpenAPISpringTestServer {

  //TODO: Add OpenAPI servlet
  public static void main(String[] args) {
    SpringApplication.run(OpenAPISpringTestServer.class, args);
  }

}

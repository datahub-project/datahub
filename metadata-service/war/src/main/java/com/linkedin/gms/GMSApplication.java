package com.linkedin.gms;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import({CommonApplicationConfig.class, ServletConfig.class})
public class GMSApplication extends SpringBootServletInitializer {

  @Override
  protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
    return application.sources(GMSApplication.class);
  }

  public static void main(String[] args) {
    SpringApplication.run(GMSApplication.class, args);
  }
}

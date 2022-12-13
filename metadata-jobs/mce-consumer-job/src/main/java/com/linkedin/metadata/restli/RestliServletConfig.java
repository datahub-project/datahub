package com.linkedin.metadata.restli;

import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.server.RestliHandlerServlet;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.net.URI;

@Configuration
public class RestliServletConfig {
    @Value("${server.port}")
    private int configuredPort;

    @Bean("restliEntityClient")
    @Primary
    public RestliEntityClient restliEntityClient() {
        String selfUri = String.format("http://localhost:%s/gms/", configuredPort);
        final Client restClient = DefaultRestliClientFactory.getRestLiClient(URI.create(selfUri), null);
        return new RestliEntityClient(restClient);
    }

    @Bean
    public ServletRegistrationBean<RestliHandlerServlet> servletRegistrationBean(
            @Qualifier("restliHandlerServlet") RestliHandlerServlet servlet) {
        return new ServletRegistrationBean<>(servlet, "/gms/*");
    }

    @Bean
    public RestliHandlerServlet restliHandlerServlet() {
        return new RestliHandlerServlet();
    }
}

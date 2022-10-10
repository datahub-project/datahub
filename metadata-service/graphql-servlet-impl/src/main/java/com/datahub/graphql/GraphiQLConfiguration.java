package com.datahub.graphql;

import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

public class GraphiQLConfiguration {

    @Configuration
    @EnableWebMvc
    public static class GraphiQLWebMvcResourceConfiguration implements WebMvcConfigurer {

        @Override
        public void addResourceHandlers(ResourceHandlerRegistry registry) {
            registry.addResourceHandler("/graphiql/**")
                    .addResourceLocations(new ClassPathResource("/graphiql/"));
        }
    }

}
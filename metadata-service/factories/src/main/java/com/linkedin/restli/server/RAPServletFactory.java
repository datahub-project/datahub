package com.linkedin.restli.server;

import com.linkedin.metadata.filter.RestliLoggingFilter;
import com.linkedin.parseq.Engine;
import com.linkedin.parseq.EngineBuilder;
import com.linkedin.r2.filter.FilterChains;
import com.linkedin.r2.filter.transport.FilterChainDispatcher;
import com.linkedin.r2.transport.http.server.RAPServlet;
import com.linkedin.restli.docgen.DefaultDocumentationRequestHandler;
import com.linkedin.restli.server.spring.SpringInjectResourceFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;

@Slf4j
@Configuration
public class RAPServletFactory {
    @Value("#{systemEnvironment['RESTLI_SERVLET_THREADS']}")
    private Integer environmentThreads;

    @Bean(name = "restliSpringInjectResourceFactory")
    public SpringInjectResourceFactory springInjectResourceFactory() {
        return new SpringInjectResourceFactory();
    }

    @Bean("parseqEngineThreads")
    public int parseqEngineThreads() {
        return environmentThreads != null ? environmentThreads : (Runtime.getRuntime().availableProcessors() + 1);
    }
    @Bean
    public RAPServlet rapServlet(
            @Qualifier("restliSpringInjectResourceFactory") SpringInjectResourceFactory springInjectResourceFactory,
            @Qualifier("parseqEngineThreads") int threads) {
        log.info("Starting restli servlet with {} threads.", threads);
        Engine parseqEngine = new EngineBuilder()
                .setTaskExecutor(Executors.newFixedThreadPool(threads))
                .setTimerScheduler(Executors.newSingleThreadScheduledExecutor())
                .build();

        RestLiConfig config = new RestLiConfig();
        config.setDocumentationRequestHandler(new DefaultDocumentationRequestHandler());
        config.setResourcePackageNames("com.linkedin.metadata.resources");
        config.addFilter(new RestliLoggingFilter());

        RestLiServer restLiServer = new RestLiServer(config, springInjectResourceFactory, parseqEngine);
        return new RAPServlet(new FilterChainDispatcher(new DelegatingTransportDispatcher(restLiServer, restLiServer),
                FilterChains.empty()));
    }
}

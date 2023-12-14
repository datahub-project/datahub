package com.linkedin.restli.server;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.linkedin.data.codec.AbstractJacksonDataCodec;
import com.linkedin.metadata.filter.RestliLoggingFilter;
import com.linkedin.parseq.Engine;
import com.linkedin.parseq.EngineBuilder;
import com.linkedin.r2.filter.FilterChains;
import com.linkedin.r2.filter.transport.FilterChainDispatcher;
import com.linkedin.r2.transport.http.server.RAPServlet;
import com.linkedin.restli.docgen.DefaultDocumentationRequestHandler;
import com.linkedin.restli.server.spring.SpringInjectResourceFactory;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class RAPServletFactory {
  @Value("#{systemEnvironment['RESTLI_SERVLET_THREADS']}")
  private Integer environmentThreads;

  @Value("${" + INGESTION_MAX_SERIALIZED_STRING_LENGTH + ":16000000}")
  private int maxSerializedStringLength;

  @Bean(name = "restliSpringInjectResourceFactory")
  public SpringInjectResourceFactory springInjectResourceFactory() {
    return new SpringInjectResourceFactory();
  }

  @Bean("parseqEngineThreads")
  public int parseqEngineThreads() {
    return environmentThreads != null
        ? environmentThreads
        : (Runtime.getRuntime().availableProcessors() + 1);
  }

  @Bean
  public RAPServlet rapServlet(
      @Qualifier("restliSpringInjectResourceFactory")
          SpringInjectResourceFactory springInjectResourceFactory,
      @Qualifier("parseqEngineThreads") int threads) {
    log.info("Starting restli servlet with {} threads.", threads);
    Engine parseqEngine =
        new EngineBuilder()
            .setTaskExecutor(Executors.newFixedThreadPool(threads))
            .setTimerScheduler(Executors.newSingleThreadScheduledExecutor())
            .build();

    // !!!!!!! IMPORTANT !!!!!!!
    // This effectively sets the max aspect size to 16 MB. Used in deserialization of messages.
    // Without this the limit is
    // whatever Jackson is defaulting to (5 MB currently).
    AbstractJacksonDataCodec.JSON_FACTORY.setStreamReadConstraints(
        StreamReadConstraints.builder().maxStringLength(maxSerializedStringLength).build());
    // !!!!!!! IMPORTANT !!!!!!!

    RestLiConfig config = new RestLiConfig();
    config.setDocumentationRequestHandler(new DefaultDocumentationRequestHandler());
    config.setResourcePackageNames("com.linkedin.metadata.resources");
    config.addFilter(new RestliLoggingFilter());

    RestLiServer restLiServer = new RestLiServer(config, springInjectResourceFactory, parseqEngine);
    return new RAPServlet(
        new FilterChainDispatcher(
            new DelegatingTransportDispatcher(restLiServer, restLiServer), FilterChains.empty()));
  }
}

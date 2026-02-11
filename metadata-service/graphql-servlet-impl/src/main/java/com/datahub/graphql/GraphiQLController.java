package com.datahub.graphql;

import static java.nio.charset.StandardCharsets.*;

import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Slf4j
@Controller
@ConditionalOnProperty(
    name = "graphql.graphiql.enabled",
    havingValue = "true",
    matchIfMissing = true)
public class GraphiQLController {

  private final String graphiqlHtml;

  public GraphiQLController() {
    Resource graphiQLResource = new ClassPathResource("graphiql/index.html");
    try (Reader reader = new InputStreamReader(graphiQLResource.getInputStream(), UTF_8)) {
      this.graphiqlHtml = FileCopyUtils.copyToString(reader);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @GetMapping(value = "/api/graphiql", produces = MediaType.TEXT_HTML_VALUE + ";charset=UTF-8")
  @ResponseBody
  CompletableFuture<String> graphiQL(HttpServletResponse response) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // Add Content Security Policy headers to allow unpkg.com for external resources
          response.setHeader(
              "Content-Security-Policy",
              "default-src 'self'; "
                  + "script-src 'self' https://unpkg.com 'unsafe-inline'; "
                  + "style-src 'self' https://unpkg.com 'unsafe-inline'; "
                  + "img-src 'self' data:; "
                  + "connect-src 'self'");

          response.setHeader("X-Content-Type-Options", "nosniff");
          response.setHeader("X-Frame-Options", "DENY");

          return this.graphiqlHtml;
        },
        this.getClass().getSimpleName(),
        "graphiQL");
  }
}

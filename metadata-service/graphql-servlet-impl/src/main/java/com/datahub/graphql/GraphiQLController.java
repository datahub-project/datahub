package com.datahub.graphql;

import static java.nio.charset.StandardCharsets.*;

import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Slf4j
@Controller
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

  @GetMapping(value = "/api/graphiql", produces = MediaType.TEXT_HTML_VALUE)
  @ResponseBody
  CompletableFuture<String> graphiQL() {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> this.graphiqlHtml, this.getClass().getSimpleName(), "graphiQL");
  }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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

  @GetMapping(value = "/api/graphiql", produces = MediaType.TEXT_HTML_VALUE + ";charset=UTF-8")
  @ResponseBody
  CompletableFuture<String> graphiQL() {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> this.graphiqlHtml, this.getClass().getSimpleName(), "graphiQL");
  }
}

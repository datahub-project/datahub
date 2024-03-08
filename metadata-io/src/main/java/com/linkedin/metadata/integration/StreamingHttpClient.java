package com.linkedin.metadata.integration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/** A simple HTTP client that can query a server and process the response stream. */
public class StreamingHttpClient {

  private final HttpClient client =
      HttpClient.newBuilder()
          .version(HttpClient.Version.HTTP_1_1)
          .followRedirects(HttpClient.Redirect.NORMAL)
          .connectTimeout(Duration.ofSeconds(20))
          .executor(Executors.newCachedThreadPool())
          .build();

  private final StreamingJsonChunkProcessor _jsonChunkProcessor = new StreamingJsonChunkProcessor();

  public StreamingHttpClient() {}

  /**
   * Queries a server and processes the response stream.
   *
   * @param requestUri the URI to query
   * @param headers the headers to include in the request
   * @param headerChunkProcessor a consumer that processes the header chunk
   * @param rowChunkProcessor a consumer that processes the row chunk
   * @param errorChunkProcessor a consumer that processes the error chunk
   */
  public void queryAndProcessStream(
      String requestUri,
      Map<String, String> headers,
      Consumer<List<String>> headerChunkProcessor,
      Consumer<List<String>> rowChunkProcessor,
      Consumer<List<String>> errorChunkProcessor) {

    String requestBody = "{}";
    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder()
            .uri(URI.create(requestUri))
            .POST(HttpRequest.BodyPublishers.ofString(requestBody));

    for (Map.Entry<String, String> header : headers.entrySet()) {
      requestBuilder.header(header.getKey(), header.getValue());
    }

    client
        .sendAsync(requestBuilder.build(), HttpResponse.BodyHandlers.ofInputStream())
        .thenApply(
            response -> {
              if (response.statusCode() != 200) {
                errorChunkProcessor.accept(
                    List.of(
                        "Failed to query server: "
                            + response.statusCode()
                            + " "
                            + response.body()));
                if (response.statusCode() == 404) {
                  throw new ResourceNotFoundException("Server couldn't locate the resource");
                }
                throw new RuntimeException("Failed to query server: " + response.statusCode());
              }
              return response.body();
            })
        .thenAccept(
            inputStream -> {
              try {
                this._jsonChunkProcessor.processJsonStream(
                    inputStream, headerChunkProcessor, rowChunkProcessor, errorChunkProcessor);
              } catch (IOException e) {
                e.printStackTrace();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            })
        .join();
  }

  private void processStream(
      InputStream inputStream,
      StreamingJsonChunkProcessor jsonChunkProcessor,
      Consumer<String> applier)
      throws IOException {
    // Wrap the InputStream in an InputStreamReader and BufferedReader
    try (InputStreamReader streamReader =
            new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        BufferedReader reader = new BufferedReader(streamReader)) {

      String line;
      while ((line = reader.readLine()) != null) {
        // Each line is processed here
        applier.accept(line);
      }
    }
  }
}
